package types

import (
	"bytes"
	"fmt"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/tackle/clock"
	"google.golang.org/protobuf/types/known/timestamppb"
	"slices"
	"strings"
)

type ItemID []byte

func ToItemID(id string) ItemID {
	if strings.TrimSpace(id) == "" {
		return nil
	}
	return ItemID(id)
}

type Item struct {
	ID             ItemID
	SourceID       ItemID
	IsLeased       bool
	LeaseDeadline  clock.Time
	ExpireDeadline clock.Time
	EnqueueAt      clock.Time
	CreatedAt      clock.Time
	Attempts       int
	MaxAttempts    int
	Reference      string
	Encoding       string
	Kind           string
	Payload        []byte
}

func (i *Item) Compare(r *Item) bool {
	if !bytes.Equal(i.ID, r.ID) {
		return false
	}
	if !bytes.Equal(i.SourceID, r.SourceID) {
		return false
	}
	if i.IsLeased != r.IsLeased {
		return false
	}
	if i.ExpireDeadline.Compare(r.ExpireDeadline) != 0 {
		return false
	}
	if i.LeaseDeadline.Compare(r.LeaseDeadline) != 0 {
		return false
	}
	if i.CreatedAt.Compare(r.CreatedAt) != 0 {
		return false
	}
	if i.Attempts != r.Attempts {
		return false
	}
	if i.Reference != r.Reference {
		return false
	}
	if i.Encoding != r.Encoding {
		return false
	}
	if i.Kind != r.Kind {
		return false
	}
	if i.Payload != nil && !bytes.Equal(i.Payload, r.Payload) {
		return false
	}
	return true
}

func (i *Item) ToProto(in *pb.StorageItem) *pb.StorageItem {
	in.LeaseDeadline = timestamppb.New(i.LeaseDeadline)
	in.ExpireDeadline = timestamppb.New(i.ExpireDeadline)
	in.CreatedAt = timestamppb.New(i.CreatedAt)
	in.Attempts = int32(i.Attempts)
	in.MaxAttempts = int32(i.MaxAttempts)
	in.IsLeased = i.IsLeased
	in.Reference = i.Reference
	in.Encoding = i.Encoding
	in.Payload = i.Payload
	in.Kind = i.Kind
	in.Id = string(i.ID)
	in.SourceId = string(i.SourceID)
	return in
}

func (i *Item) FromProto(in *pb.StorageItem) *Item {
	i.LeaseDeadline = in.LeaseDeadline.AsTime()
	i.ExpireDeadline = in.ExpireDeadline.AsTime()
	i.CreatedAt = in.CreatedAt.AsTime()
	i.Attempts = int(in.Attempts)
	i.MaxAttempts = int(in.MaxAttempts)
	i.IsLeased = in.IsLeased
	i.Reference = in.Reference
	i.Encoding = in.Encoding
	i.Payload = in.Payload
	i.Kind = in.Kind
	i.ID = []byte(in.Id)
	if in.SourceId != "" {
		i.SourceID = []byte(in.SourceId)
	}
	return i
}

type PartitionInfo struct {
	Queue        QueueInfo
	StorageName  string
	PartitionNum int
	Draining     bool

	hashKey PartitionHash
}

const (
	PartitionAvailable = "available"
	PartitionDraining  = "draining"
)

func (p PartitionInfo) State() string {
	if p.Draining {
		return PartitionDraining
	}
	return PartitionAvailable
}

type PartitionHash string

func (p *PartitionInfo) HashKey() PartitionHash {
	if len(p.hashKey) != 0 {
		return p.hashKey
	}

	p.hashKey = PartitionHash(fmt.Sprintf("%s %d %s", p.Queue.Name, p.PartitionNum, p.StorageName))
	return p.hashKey
}

type QueueInfo struct {
	Name                string
	Namespace           string
	LeaseTimeout        clock.Duration
	DeadQueue           string
	ExpireTimeout       clock.Duration
	CreatedAt           clock.Time
	UpdatedAt           clock.Time
	MaxAttempts         int
	Reference           string
	RequestedPartitions int
	NextPartition       int
	Revision            int
	PartitionInfo       []PartitionInfo
}

func (i *QueueInfo) LinkPartitions() {
	owner := *i
	owner.PartitionInfo = nil
	i.PartitionInfo = slices.Clone(i.PartitionInfo)
	for idx := range i.PartitionInfo {
		i.PartitionInfo[idx].Queue = owner
	}
}

func (i *QueueInfo) ToProto(in *pb.QueueInfo) *pb.QueueInfo {
	in.RequestedPartitions = int32(i.RequestedPartitions)
	in.ExpireTimeout = i.ExpireTimeout.String()
	in.LeaseTimeout = i.LeaseTimeout.String()
	in.UpdatedAt = timestamppb.New(i.UpdatedAt)
	in.CreatedAt = timestamppb.New(i.CreatedAt)
	in.MaxAttempts = int32(i.MaxAttempts)
	in.DeadQueue = i.DeadQueue
	in.Namespace = i.Namespace
	in.Reference = i.Reference
	in.QueueName = i.Name
	in.Partitions = make([]*pb.PartitionInfo, 0, len(i.PartitionInfo))
	for _, p := range i.PartitionInfo {
		in.Partitions = append(in.Partitions, &pb.PartitionInfo{
			Partition:   int32(p.PartitionNum),
			StorageName: p.StorageName,
			State:       p.State(),
		})
	}
	return in
}

func (i *QueueInfo) Update(r QueueInfo) bool {
	if r.ExpireTimeout.Nanoseconds() != 0 {
		i.ExpireTimeout = r.ExpireTimeout
	}
	if r.LeaseTimeout.Nanoseconds() != 0 {
		i.LeaseTimeout = r.LeaseTimeout
	}
	if i.MaxAttempts != r.MaxAttempts {
		i.MaxAttempts = r.MaxAttempts
	}
	if i.Reference != r.Reference {
		i.Reference = r.Reference
	}
	if i.DeadQueue != r.DeadQueue {
		i.DeadQueue = r.DeadQueue
	}
	if i.UpdatedAt != r.UpdatedAt {
		i.UpdatedAt = r.UpdatedAt
	}
	if r.RequestedPartitions != 0 && i.RequestedPartitions != r.RequestedPartitions {
		i.RequestedPartitions = r.RequestedPartitions
	}
	return true
}

type PartitionList struct {
	QueueName     string
	Revision      int
	NextPartition int
	PartitionInfo []PartitionInfo
	UpdatedAt     clock.Time
}

func (i *QueueInfo) AdoptPartitions(r PartitionList) {
	i.PartitionInfo = slices.Clone(r.PartitionInfo)
	i.NextPartition = r.NextPartition
	i.UpdatedAt = r.UpdatedAt
	i.Revision++
}

type QueueUpdate struct {
	Name                string
	LeaseTimeout        clock.Duration
	ExpireTimeout       clock.Duration
	RequestedPartitions int
	DeadQueue           *string
	Reference           *string
	MaxAttempts         *int
}

func (u QueueUpdate) ApplyTo(info QueueInfo) QueueInfo {
	if u.LeaseTimeout != 0 {
		info.LeaseTimeout = u.LeaseTimeout
	}
	if u.ExpireTimeout != 0 {
		info.ExpireTimeout = u.ExpireTimeout
	}
	if u.RequestedPartitions != 0 {
		info.RequestedPartitions = u.RequestedPartitions
	}
	if u.DeadQueue != nil {
		info.DeadQueue = *u.DeadQueue
	}
	if u.Reference != nil {
		info.Reference = *u.Reference
	}
	if u.MaxAttempts != nil {
		info.MaxAttempts = *u.MaxAttempts
	}
	return info
}

type LifeCycleScan struct {
	Pivot                  ItemID
	Last                   ItemID
	Actions                []Action
	EarliestLeaseDeadline  clock.Time
	EarliestExpireDeadline clock.Time
	EarliestEnqueueAt      clock.Time
}

func (s *LifeCycleScan) MidPass() bool {
	return s.Pivot != nil
}

func (s *LifeCycleScan) Unread(id ItemID) bool {
	return s.MidPass() && bytes.Compare(id, s.Pivot) > 0 && bytes.Compare(id, s.Last) <= 0
}

func (s *LifeCycleScan) ResetPage() {
	s.Actions = s.Actions[:0]
	s.EarliestLeaseDeadline = clock.Time{}
	s.EarliestExpireDeadline = clock.Time{}
	s.EarliestEnqueueAt = clock.Time{}
}

func (s *LifeCycleScan) EndPass() {
	s.Pivot = nil
	s.Last = nil
}

func (s *LifeCycleScan) Add(a Action) {
	s.Actions = append(s.Actions, a)
}

func (s *LifeCycleScan) SeeLeaseDeadline(deadline clock.Time) {
	earliest(&s.EarliestLeaseDeadline, deadline)
}

func (s *LifeCycleScan) SeeExpireDeadline(deadline clock.Time) {
	earliest(&s.EarliestExpireDeadline, deadline)
}

func (s *LifeCycleScan) SeeEnqueueAt(at clock.Time) {
	earliest(&s.EarliestEnqueueAt, at)
}

func earliest(current *clock.Time, candidate clock.Time) {
	if current.IsZero() || candidate.Before(*current) {
		*current = candidate
	}
}
