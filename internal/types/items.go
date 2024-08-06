package types

import (
	"bytes"
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
	// IsReserved is true if the item has been reserved by a client
	IsReserved bool
	// ReserveDeadline is the time in the future when the reservation is
	// expired and can be reserved by another consumer
	ReserveDeadline clock.Time
	// DeadDeadline is the time in the future the item must be consumed,
	// before it is considered dead and moved to the dead letter queue if configured.
	DeadDeadline clock.Time
	// CreatedAt is the time stamp when this item was added to the database.
	CreatedAt clock.Time
	// Attempts is how many attempts this item has seen
	Attempts int
	// MaxAttempts is the maximum number of times this message can be deferred by a consumer before it is
	// placed in the dead letter queue
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
	if i.IsReserved != r.IsReserved {
		return false
	}
	if i.DeadDeadline.Compare(r.DeadDeadline) != 0 {
		return false
	}
	if i.ReserveDeadline.Compare(r.ReserveDeadline) != 0 {
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

func (i *Item) ToProto(in *pb.StorageQueueItem) *pb.StorageQueueItem {
	in.ReserveDeadline = timestamppb.New(i.ReserveDeadline)
	in.DeadDeadline = timestamppb.New(i.DeadDeadline)
	in.CreatedAt = timestamppb.New(i.CreatedAt)
	in.Attempts = int32(i.Attempts)
	in.MaxAttempts = int32(i.MaxAttempts)
	in.IsReserved = i.IsReserved
	in.Reference = i.Reference
	in.Encoding = i.Encoding
	in.Payload = i.Payload
	in.Kind = i.Kind
	in.Id = string(i.ID)
	return in
}

func (i *Item) FromProto(in *pb.StorageQueueItem) *Item {
	i.ReserveDeadline = in.ReserveDeadline.AsTime()
	i.DeadDeadline = in.DeadDeadline.AsTime()
	i.CreatedAt = in.CreatedAt.AsTime()
	i.Attempts = int(in.Attempts)
	i.MaxAttempts = int(in.MaxAttempts)
	i.IsReserved = in.IsReserved
	i.Reference = in.Reference
	i.Encoding = in.Encoding
	i.Payload = in.Payload
	i.Kind = in.Kind
	i.ID = []byte(in.Id)
	return i
}

// QueueInfo is information about a queue
type QueueInfo struct {
	// The name of the queue
	Name string
	// ReserveTimeout is how long the reservation is valid for.
	// TODO: We must ensure the DeadTimeout is not less than the ReserveTimeout
	ReserveTimeout clock.Duration
	// DeadQueue is the name of the dead letter queue for this queue.
	DeadQueue string
	// DeadTimeout is the time an item can wait in the queue regardless of attempts before
	// it is moved to the dead letter queue. This value is used if no DeadTimeout is provided
	// by the queued item.
	DeadTimeout clock.Duration
	// CreatedAt is the time this queue was created
	CreatedAt clock.Time
	// UpdatedAt is the time this queue was last updated.
	UpdatedAt clock.Time
	// MaxAttempts is the maximum number of times this item can be retried
	MaxAttempts int
	// Reference is the user supplied field which could contain metadata or specify who owns this queue
	Reference string
}

func (i *QueueInfo) ToProto(in *pb.QueueInfo) *pb.QueueInfo {
	in.ReserveTimeout = i.ReserveTimeout.String()
	in.UpdatedAt = timestamppb.New(i.UpdatedAt)
	in.CreatedAt = timestamppb.New(i.CreatedAt)
	in.DeadTimeout = i.DeadTimeout.String()
	in.MaxAttempts = int32(i.MaxAttempts)
	in.DeadQueue = i.DeadQueue
	in.Reference = i.Reference
	in.QueueName = i.Name
	return in
}

func (i *QueueInfo) Update(r QueueInfo) bool {
	if r.DeadTimeout.Nanoseconds() != 0 {
		i.DeadTimeout = r.DeadTimeout
	}
	if r.ReserveTimeout.Nanoseconds() != 0 {
		i.ReserveTimeout = r.ReserveTimeout
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
	return true
}
