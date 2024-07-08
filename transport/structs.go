package transport

import (
	"bytes"
	"context"
	pb "github.com/kapetan-io/querator/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

// TODO(thrawn01): Consider creating a pool of ReserveRequest and Item to avoid GC

type Response struct {
	requestDeadline time.Time
	// Used by Reserve() to wait for this request to complete
	readyCh chan struct{}
	// The error to be returned to the caller
	err error
}

type ReserveRequest struct {
	// The number of items requested from the queue.
	NumRequested int // TODO: Rename the protobuf variable this maps too
	// How long the caller expects Reserve() to block before returning
	// if no items are available to be reserved. Max duration is 5 minutes.
	RequestTimeout time.Duration
	// The id of the client
	ClientID string
	// The context of the requesting client
	Context context.Context
	// The result of the reservation
	Items []*Item
	// The internal representation of the response
	Response Response
}

type ProduceRequest struct {
	// How long the caller expects Produce() to block before returning
	RequestTimeout time.Duration
	// The context of the requesting client
	Context context.Context
	// The items to produce
	Items []*Item

	// Is calculated from RequestTimeout and specifies a time in the future when this request
	// should be cancelled.
	requestDeadline time.Time
	// Used by Produce() to wait for this request to complete
	readyCh chan struct{}
	// The error to be returned to the caller
	err error
}

type CompleteRequest struct {
	// How long the caller expects Complete() to block before returning
	RequestTimeout time.Duration
	// The context of the requesting client
	Context context.Context
	// The ids to mark as complete
	Ids []string
	// The error to be returned to the caller
	Err error

	// Is calculated from RequestTimeout and specifies a time in the future when this request
	// should be cancelled.
	requestDeadline time.Time
	// Used by Complete() to wait for this request to complete
	readyCh chan struct{}
}

type StorageRequest struct {
	Context context.Context
	Items   []*Item
	ID      string
	Pivot   string
	Limit   int

	readyCh chan struct{}
	err     error
}

type ShutdownRequest struct {
	Context context.Context
	readyCh chan struct{}
	Err     error
}

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
