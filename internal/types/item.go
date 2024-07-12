package types

import (
	"bytes"
	pb "github.com/kapetan-io/querator/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

// Item is the store and queue representation of an item in the queue.
type Item struct {
	// ID is unique to each item in the data store. The ID style is different depending on the data store
	// implementation, and does not include the queue name.
	ID string
	// IsReserved is true if the item has been reserved by a client
	IsReserved bool
	// ReserveDeadline is the time in the future when the reservation is
	// expired and can be reserved by another consumer
	ReserveDeadline time.Time
	// DeadDeadline is the time in the future the item must be consumed,
	// before it is considered dead and moved to the dead letter queue if configured.
	DeadDeadline time.Time
	// CreatedAt is the time stamp when this item was added to the database.
	CreatedAt time.Time
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

func (i *Item) Compare(r *Item) bool {
	if i.ID != r.ID {
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

func (i *Item) ToStorageItemProto(in *pb.StorageItem) *pb.StorageItem {
	in.ReserveDeadline = timestamppb.New(i.ReserveDeadline)
	in.DeadDeadline = timestamppb.New(i.DeadDeadline)
	in.Attempts = int32(i.Attempts)
	in.MaxAttempts = int32(i.MaxAttempts)
	in.IsReserved = i.IsReserved
	in.Reference = i.Reference
	in.Encoding = i.Encoding
	in.Payload = i.Payload
	in.Kind = i.Kind
	in.Id = i.ID
	return in
}
