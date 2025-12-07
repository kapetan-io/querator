package types

import (
	"bytes"
	"fmt"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/tackle/clock"
	"google.golang.org/protobuf/types/known/timestamppb"
	"strings"
)

// TODO: Consider using this instead of []byte for the Item ID
type ItemID []byte

func ToItemID(id string) ItemID {
	if strings.TrimSpace(id) == "" {
		return nil
	}
	return ItemID(id)
}

// Item is the store and queue representation of an item in the queue.
type Item struct {
	// ID is unique to each item in the data store. The ID style is different depending on the data store
	// implementation, and does not include the queue name.
	ID ItemID
	// SourceID is the original ID from the source queue (only set for DLQ items, nil otherwise)
	SourceID ItemID
	// IsLeased is true if the item has been leased by a client
	// TODO: Change this to a time stamp, which if non zero is the timestamp when the item was leased
	IsLeased bool
	// LeaseDeadline is the time in the future when the lease is
	// expired and can be leased by another consumer
	LeaseDeadline clock.Time
	// ExpireDeadline is the time in the future the item must be consumed,
	// before it is considered dead and moved to the dead letter queue if configured.
	ExpireDeadline clock.Time
	// EnqueueAt if this is not empty, then clock.Time indicates when this item should be enqueued.
	EnqueueAt clock.Time
	// CreatedAt is the time stamp when this item was added to the database.
	CreatedAt clock.Time
	// Attempts is how many attempts this item has seen
	Attempts int
	// MaxAttempts is the maximum number of times this message can be retried by a consumer before it is
	// placed in the dead letter queue.
	// TODO(thrawn01): We probably do NOT want to store this in the item, as the operator might want to
	// retroactively increase the max attempts during an outage. As such the queue info should be the source
	// of truth to max attempts.
	MaxAttempts int
	// Reference is a user supplied field which could contain metadata or specify who owns this queue
	// Examples: "jake@statefarm.com", "stapler@office-space.com", "account-0001"
	Reference string
	// Encoding is a user specified field which indicates the encoding the user used to encode the 'payload'
	Encoding string
	// Kind is the Kind or Type the payload contains. Consumers can use this field to determine handling
	// of the payload prior to unmarshalling. Examples: 'webhook-v2', 'webhook-v1',
	Kind string
	// Payload is the payload of the queue item
	Payload []byte
}

// TODO: Remove if not needed
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

// PartitionInfo is information about partition
type PartitionInfo struct {
	Queue QueueInfo
	// QueueName string
	// Which StorageConfig Instance this partition belong too
	StorageName string
	// If the partition is marked as read only
	ReadOnly bool
	// The partition number
	PartitionNum int

	// cached hashKey
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

// QueueInfo is information about a queue
type QueueInfo struct {
	// The name of the queue
	Name string
	// LeaseTimeout is how long the lease is valid for.
	LeaseTimeout clock.Duration
	// DeadQueue is the name of the dead letter queue for this queue.
	DeadQueue string
	// ExpireTimeout is the time an item can wait in the queue regardless of attempts before
	// it is moved to the dead letter queue. This value is used if no ExpireTimeout is provided
	// by the queued item.
	ExpireTimeout clock.Duration
	// CreatedAt is the time this queue was created
	CreatedAt clock.Time
	// UpdatedAt is the time this queue was last updated.
	UpdatedAt clock.Time
	// MaxAttempts is the maximum number of times this item can be retried
	MaxAttempts int
	// Reference is the user supplied field which could contain metadata or specify who owns this queue
	Reference string
	// RequestedPartitions is the number of partitions this queue expects to have. This might be different
	// from the number of StoragePartitions listed in PartitionInfo as the system grows or shrinks the number
	// of actual partitions.
	RequestedPartitions int
	// PartitionInfo is a list current partition details
	PartitionInfo []PartitionInfo
}

func (i *QueueInfo) ToProto(in *pb.QueueInfo) *pb.QueueInfo {
	in.LeaseTimeout = i.LeaseTimeout.String()
	in.UpdatedAt = timestamppb.New(i.UpdatedAt)
	in.CreatedAt = timestamppb.New(i.CreatedAt)
	in.ExpireTimeout = i.ExpireTimeout.String()
	in.MaxAttempts = int32(i.MaxAttempts)
	in.DeadQueue = i.DeadQueue
	in.Reference = i.Reference
	in.QueueName = i.Name
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
	NextLeaseExpiry clock.Time
}
