package store

import (
	"bytes"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport"
	"strings"
)

var ErrEmptyQueueName = transport.NewInvalidOption("queue name is invalid; queue name cannot be empty")

const maxReferenceLength = 2_000
const maxQueueNameLength = 512
const maxInt16 = 65536 // 1<<16

type QueuesValidation struct{}

func (s QueuesValidation) validateGet(name string) error {
	if strings.TrimSpace(name) == "" {
		return ErrEmptyQueueName
	}

	if strings.Contains(name, "~") {
		return transport.NewInvalidOption("queue name is invalid; '%s' cannot contain '~' character", name)
	}

	return nil
}
func (s QueuesValidation) validateQueueName(info types.QueueInfo) error {
	if len(info.Name) > maxQueueNameLength {
		return transport.NewInvalidOption("queue name is invalid; cannot be greater than '%d' characters", maxQueueNameLength)
	}

	if strings.TrimSpace(info.Name) == "" {
		return ErrEmptyQueueName
	}

	if strings.Contains(info.Name, "~") {
		return transport.NewInvalidOption("queue name is invalid; '%s' cannot contain '~' character", info.Name)
	}
	return nil
}

func (s QueuesValidation) validateQueueInfo(info types.QueueInfo) error {
	if err := s.validateQueueName(info); err != nil {
		return err
	}

	if len(info.DeadQueue) > maxQueueNameLength {
		return transport.NewInvalidOption("dead queue is invalid; cannot be greater than '%d' characters", maxQueueNameLength)
	}

	if strings.Contains(info.DeadQueue, "~") {
		return transport.NewInvalidOption("dead queue is invalid; '%s' cannot contain '~' character", info.DeadQueue)
	}

	if len(info.Reference) > maxReferenceLength {
		return transport.NewInvalidOption("reference field is invalid; cannot be greater than '%d' characters", maxReferenceLength)
	}

	if info.MaxAttempts > maxInt16 {
		return transport.NewInvalidOption("max attempts is invalid; cannot be greater than %d", maxInt16)
	}

	if info.MaxAttempts < 0 {
		return transport.NewInvalidOption("max attempts is invalid; cannot be negative number")
	}

	// TODO: Add this check to the errors test
	if info.Partitions < 1 {
		return transport.NewInvalidOption("partitions is invalid; cannot be less than 1")
	}

	return nil
}

func (s QueuesValidation) validateAdd(info types.QueueInfo) error {
	if err := s.validateQueueInfo(info); err != nil {
		return err
	}

	if info.ReserveTimeout.Microseconds() == 0 {
		return transport.NewInvalidOption("reserve timeout is invalid; cannot be empty")
	}

	if info.DeadTimeout.Microseconds() == 0 {
		return transport.NewInvalidOption("dead timeout is invalid; cannot be empty")
	}

	if info.ReserveTimeout > info.DeadTimeout {
		return transport.NewInvalidOption("reserve timeout is too long; %s cannot be greater than the "+
			"dead timeout %s", info.ReserveTimeout.String(), info.DeadTimeout.String())
	}

	return nil
}

func (s QueuesValidation) validateUpdate(info types.QueueInfo) error {
	return s.validateQueueInfo(info)
}

func (s QueuesValidation) validateList(opts types.ListOptions) error {

	if opts.Limit < 0 {
		return transport.NewInvalidOption("limit is invalid; limit cannot be negative")
	}

	if opts.Limit > maxInt16 {
		return transport.NewInvalidOption("limit is invalid; cannot be greater than %d", maxInt16)
	}

	if bytes.Contains(opts.Pivot, []byte("~")) {
		return transport.NewInvalidOption("pivot is invalid; '%s' cannot contain '~' character", opts.Pivot)
	}

	return nil
}

func (s QueuesValidation) validateDelete(name string) error {
	if len(name) > maxQueueNameLength {
		return transport.NewInvalidOption("queue name is invalid; cannot be greater than '%d' characters", maxQueueNameLength)
	}

	if strings.TrimSpace(name) == "" {
		return ErrEmptyQueueName
	}

	if strings.Contains(name, "~") {
		return transport.NewInvalidOption("queue name is invalid; '%s' cannot contain '~' character", name)
	}
	return nil
}
