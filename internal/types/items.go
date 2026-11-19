package types

import (
	"bytes"
	"fmt"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/tackle/clock"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	ReadOnly     bool
	PartitionNum int

	hashKey PartitionHash
}

type PartitionHash string

func (p *PartitionInfo) HashKey() PartitionHash {
	if len(p.hashKey) != 0 {
		return p.hashKey
	}

	p.hashKey = PartitionHash(fmt.Sprintf("%s%d%s%t", p.Queue.Name, p.PartitionNum, p.StorageName, p.ReadOnly))
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
	PartitionInfo       []PartitionInfo
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

type LifeCycleInfo struct {
	NextLeaseExpiry    clock.Time
	NextExpireDeadline clock.Time
	NextEnqueueAt      clock.Time
}
