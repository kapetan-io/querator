package store

import (
	"bytes"
	"math"
	"strings"
	"unicode"

	"github.com/kapetan-io/querator/internal/placement"

	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport/reply"
)

var ErrEmptyQueueName = reply.NewInvalidOption("queue name is invalid; queue name cannot be empty")

const maxReferenceLength = 2_000
const maxQueueNameLength = 512

const MaxBackendNameLength = 512
const maxInt16 = 65536

type QueuesValidation struct{}

func (s QueuesValidation) validateGet(name string) error {
	if strings.TrimSpace(name) == "" {
		return ErrEmptyQueueName
	}

	if strings.Contains(name, "~") {
		return reply.NewInvalidOption("queue name is invalid; '%s' cannot contain '~' character", name)
	}

	return nil
}
func (s QueuesValidation) validateQueueName(info types.QueueInfo) error {
	if len(info.Name) > maxQueueNameLength {
		return reply.NewInvalidOption("queue name is invalid; cannot be greater than '%d' characters", maxQueueNameLength)
	}

	if strings.TrimSpace(info.Name) == "" {
		return ErrEmptyQueueName
	}

	if strings.ContainsFunc(info.Name, unicode.IsSpace) {
		return reply.NewInvalidOption("queue name is invalid; '%s' cannot contain whitespace", info.Name)
	}

	if strings.Contains(info.Name, "~") {
		return reply.NewInvalidOption("queue name is invalid; '%s' cannot contain '~' character", info.Name)
	}
	return nil
}

func (s QueuesValidation) validateQueueInfo(info types.QueueInfo) error {
	if err := s.validateQueueName(info); err != nil {
		return err
	}

	if len(info.DeadQueue) > maxQueueNameLength {
		return reply.NewInvalidOption("dead queue is invalid; cannot be greater than '%d' characters", maxQueueNameLength)
	}

	if strings.ContainsFunc(info.DeadQueue, unicode.IsSpace) {
		return reply.NewInvalidOption("dead queue is invalid; '%s' cannot contain whitespace", info.DeadQueue)
	}

	if strings.Contains(info.DeadQueue, "~") {
		return reply.NewInvalidOption("dead queue is invalid; '%s' cannot contain '~' character", info.DeadQueue)
	}

	if len(info.Reference) > maxReferenceLength {
		return reply.NewInvalidOption("reference field is invalid; cannot be greater than '%d' characters", maxReferenceLength)
	}

	if info.MaxAttempts > maxInt16 {
		return reply.NewInvalidOption("max attempts is invalid; cannot be greater than %d", maxInt16)
	}

	if info.MaxAttempts < 0 {
		return reply.NewInvalidOption("max attempts is invalid; cannot be negative number")
	}

	if info.RequestedPartitions < 1 {
		return reply.NewInvalidOption("partitions is invalid; cannot be less than 1")
	}

	if info.RequestedPartitions > placement.MaxPartitions {
		return reply.NewInvalidOption("partitions is invalid; cannot be greater than %d", placement.MaxPartitions)
	}

	return s.validateAvailablePartitions(info.Name, info.PartitionInfo)
}

func (s QueuesValidation) validateAvailablePartitions(queueName string, partitions []types.PartitionInfo) error {
	var available int
	for _, p := range partitions {
		if !p.Draining {
			available++
		}
	}
	if available > placement.MaxPartitions {
		return reply.NewInvalidOption("partitions is invalid; queue '%s' lists %d available partitions, "+
			"cannot be greater than %d", queueName, available, placement.MaxPartitions)
	}

	return nil
}

type partitionChange struct {
	Stored   types.QueueInfo
	Proposed types.PartitionList
}

func (c partitionChange) Stale() bool {
	if c.Stored.Revision != c.Proposed.Revision {
		return true
	}
	return c.Proposed.CompareCreationID && c.Stored.CreationID != c.Proposed.CreationID
}

func (s QueuesValidation) validatePartitionChange(change partitionChange) error {
	stored := make(map[int]types.PartitionInfo, len(change.Stored.PartitionInfo))
	for _, p := range change.Stored.PartitionInfo {
		stored[p.PartitionNum] = p
	}

	name := change.Proposed.QueueName
	if change.Proposed.NextPartition < change.Stored.NextPartition {
		return reply.NewInvalidOption("partitions is invalid; queue '%s' next partition number %d is below %d, "+
			"a partition number cannot be reused", name, change.Proposed.NextPartition, change.Stored.NextPartition)
	}

	seen := make(map[int]struct{}, len(change.Proposed.PartitionInfo))
	for _, p := range change.Proposed.PartitionInfo {
		if _, ok := seen[p.PartitionNum]; ok {
			return reply.NewInvalidOption("partitions is invalid; queue '%s' lists partition %d twice, "+
				"a partition number cannot be reused", name, p.PartitionNum)
		}
		seen[p.PartitionNum] = struct{}{}

		if p.PartitionNum >= change.Proposed.NextPartition {
			return reply.NewInvalidOption("partitions is invalid; queue '%s' partition %d is not below the next "+
				"partition number %d, a partition number cannot be reused", name, p.PartitionNum,
				change.Proposed.NextPartition)
		}

		before, existed := stored[p.PartitionNum]
		if !existed {
			if p.PartitionNum < change.Stored.NextPartition {
				return reply.NewInvalidOption("partitions is invalid; queue '%s' partition number %d was used "+
					"before, a partition number cannot be reused", name, p.PartitionNum)
			}
			continue
		}
		if before.Draining && !p.Draining {
			return reply.NewInvalidOption("partitions is invalid; queue '%s' partition %d is draining "+
				"and cannot become available again", name, p.PartitionNum)
		}
		if before.StorageName != p.StorageName {
			return reply.NewInvalidOption("partitions is invalid; queue '%s' partition %d is on storage backend "+
				"'%s' and cannot change to '%s'", name, p.PartitionNum, before.StorageName, p.StorageName)
		}
	}

	return s.validateAvailablePartitions(name, change.Proposed.PartitionInfo)
}

type timeoutChange struct {
	Stored   types.QueueInfo
	Proposed types.QueueInfo
}

func (s QueuesValidation) validateTimeoutChange(change timeoutChange) error {
	if change.Proposed.LeaseTimeout < 0 && change.Proposed.LeaseTimeout != change.Stored.LeaseTimeout {
		return reply.NewInvalidOption("lease timeout is invalid; cannot be negative")
	}

	if change.Proposed.ExpireTimeout < 0 && change.Proposed.ExpireTimeout != change.Stored.ExpireTimeout {
		return reply.NewInvalidOption("expire timeout is invalid; cannot be negative")
	}

	return nil
}

func ValidateQueueName(name string) error {
	return (QueuesValidation{}).validateQueueName(types.QueueInfo{Name: name})
}

func ValidateQueueAdd(info types.QueueInfo) error {
	return (QueuesValidation{}).validateAdd(info)
}

func (s QueuesValidation) validateAdd(info types.QueueInfo) error {
	if err := s.validateQueueInfo(info); err != nil {
		return err
	}

	if err := s.validateTimeoutChange(timeoutChange{Proposed: info}); err != nil {
		return err
	}

	if info.LeaseTimeout.Microseconds() == 0 {
		return reply.NewInvalidOption("lease timeout is invalid; cannot be empty")
	}

	if info.ExpireTimeout.Microseconds() == 0 {
		return reply.NewInvalidOption("expire timeout is invalid; cannot be empty")
	}

	if info.LeaseTimeout > info.ExpireTimeout {
		return reply.NewInvalidOption("lease timeout is too long; %s cannot be greater than the "+
			"expire timeout %s", info.LeaseTimeout.String(), info.ExpireTimeout.String())
	}

	return nil
}

func (s QueuesValidation) validateUpdate(info types.QueueInfo) error {
	if err := s.validateQueueInfo(info); err != nil {
		return err
	}

	if info.LeaseTimeout > info.ExpireTimeout {
		return reply.NewInvalidOption("lease timeout is too long; %s cannot be greater than the "+
			"expire timeout %s", info.LeaseTimeout.String(), info.ExpireTimeout.String())
	}

	return nil
}

func (s QueuesValidation) validateList(opts types.ListOptions) error {

	if opts.Limit < 0 {
		return reply.NewInvalidOption("limit is invalid; limit cannot be negative")
	}

	if opts.Limit > maxInt16 {
		return reply.NewInvalidOption("limit is invalid; cannot be greater than %d", maxInt16)
	}

	if bytes.Contains(opts.Pivot, []byte("~")) {
		return reply.NewInvalidOption("pivot is invalid; '%s' cannot contain '~' character", opts.Pivot)
	}

	return nil
}

func (s QueuesValidation) validateDelete(name string) error {
	if len(name) > maxQueueNameLength {
		return reply.NewInvalidOption("queue name is invalid; cannot be greater than '%d' characters", maxQueueNameLength)
	}

	if strings.TrimSpace(name) == "" {
		return ErrEmptyQueueName
	}

	if strings.ContainsFunc(name, unicode.IsSpace) {
		return reply.NewInvalidOption("queue name is invalid; '%s' cannot contain whitespace", name)
	}

	if strings.Contains(name, "~") {
		return reply.NewInvalidOption("queue name is invalid; '%s' cannot contain '~' character", name)
	}
	return nil
}

func (s QueuesValidation) validateBackend(backend types.Backend) error {
	if strings.TrimSpace(backend.Name) == "" {
		return reply.NewInvalidOption("backend is invalid; backend name cannot be empty")
	}

	if len(backend.Name) > MaxBackendNameLength {
		return reply.NewInvalidOption("backend is invalid; cannot be greater than '%d' characters", MaxBackendNameLength)
	}

	if math.IsNaN(backend.Affinity) || math.IsInf(backend.Affinity, 0) {
		return reply.NewInvalidOption("affinity is invalid; backend '%s' affinity must be a finite number, got %v",
			backend.Name, backend.Affinity)
	}

	if backend.Affinity < 0 {
		return reply.NewInvalidOption("affinity is invalid; backend '%s' affinity cannot be negative, got %v",
			backend.Name, backend.Affinity)
	}

	return nil
}
