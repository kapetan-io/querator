package store

import (
	"bytes"
	"context"
	pb "github.com/kapetan-io/querator/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

// StorageID is the decoded storage StorageID
type StorageID struct {
	Queue string
	ID    string
}

// QueueOptions are options used when creating a new store.Queue
type QueueOptions struct {
	// The name of the queue
	Name string
	// WriteTimeout (Optional) The time it should take for a single batched write to complete
	WriteTimeout time.Duration
	// ReadTimeout (Optional) The time it should take for a single batched read to complete
	ReadTimeout time.Duration
}

// Storage is the primary storage interface
type Storage interface {
	NewQueue(opts QueueOptions) (Queue, error)
	ParseID(parse string, id *StorageID) error
	CreateID(queue, id string) string
}

type ReserveOptions struct {
	// ReserveDeadline is time in the future when a reservation should expire
	ReserveDeadline time.Time

	// Limit is the max number of items to reserve
	Limit int
}

type Stats struct {
	// Total is the number of items in the queue
	Total int

	// TotalReserved is the number of items in the queue that are in reserved state
	TotalReserved int

	// AverageAge is the average age of all items in the queue
	AverageAge time.Duration

	// AverageReservedAge is the average age of reserved items in the queue
	AverageReservedAge time.Duration
}

// QueueStorage is storage for listing and storing metadata about queues
type QueueStorage interface {
	// Get returns a store.Queue from storage ready to be used
	Get(ctx context.Context, name string, queue *Queue) error

	// List returns a list of available queues
	// Create a new queue in queue storage
	// Delete a queue from queue storage
}

// Queue represents storage for a single queue
type Queue interface {
	// Stats returns stats about the queue
	Stats(ctx context.Context, stats *Stats) error

	// Reserve list up to 'limit' reservable items from the queue and marks the items as reserved.
	Reserve(ctx context.Context, items *[]*Item, opts ReserveOptions) error

	// Read reads items in a queue. limit and offset allow the user to page through all the items
	// in the queue.
	Read(ctx context.Context, items *[]*Item, pivot string, limit int) error

	// Write writes the item to the queue and updates the item with the
	// unique id.
	Write(ctx context.Context, items []*Item) error

	// Delete removes the provided ids from the queue
	Delete(ctx context.Context, items []string) error

	Close(ctx context.Context) error

	Options() QueueOptions
}

// TODO: ScheduledStorage interface {} - A place to store scheduled items to be queued. (Defer)
// TODO: QueueOptionStorage interface {} - A place to store queue options and a list of valid queues

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

// CollectIDs is a convenience function which assists in calling QueueStore.Delete()
// when a list of items to be deleted is needed.
func CollectIDs(items []*Item) []string {
	var result []string
	for _, v := range items {
		result = append(result, v.ID)
	}
	return result
}
