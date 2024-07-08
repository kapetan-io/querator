package store

import (
	"context"
	"fmt"
	"github.com/kapetan-io/querator/transport"
	"time"
)

// StorageID is the decoded storage StorageID
type StorageID struct {
	Queue string
	ID    string
}

func (id StorageID) String() string {
	return fmt.Sprintf("%s/%s", id.Queue, id.ID)
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
	// ReserveDeadline is a time in the future when the reservation should expire
	ReserveDeadline time.Time
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
	// Produce writes the items for each batch to the data store, assigning an error for each
	// batch that fails.
	Produce(ctx context.Context, batch Batch[transport.ProduceRequest]) error

	// Reserve attempts to reserve items for each request in the provided batch.
	Reserve(ctx context.Context, batch Batch[transport.ReserveRequest], opts ReserveOptions) error

	// Complete marks ids in the batch as complete, assigning an error for each batch that fails.
	// If the underlying data storage fails for some reason, this call returns an error. In that case
	// the caller should assume none of the batched items were marked as "complete"
	Complete(ctx context.Context, batch Batch[transport.CompleteRequest]) error

	// Read reads items in a queue. limit and offset allow the user to page through all the items
	// in the queue.
	Read(ctx context.Context, items *[]*transport.Item, pivot string, limit int) error

	// Write writes the item to the queue and updates the item with the
	// unique id.
	Write(ctx context.Context, items []*transport.Item) error

	// Delete removes the provided ids from the queue
	Delete(ctx context.Context, ids []string) error

	// Stats returns stats about the queue
	Stats(ctx context.Context, stats *Stats) error

	Close(ctx context.Context) error

	Options() QueueOptions
}

type Batch[T any] struct {
	Requests []T
	// TotalRequested is currently only used by Queue.Reserve()
	TotalRequested int
}

// TODO: ScheduledStorage interface {} - A place to store scheduled items to be queued. (Defer)
// TODO: QueueOptionStorage interface {} - A place to store queue options and a list of valid queues

// CollectIDs is a convenience function which assists in calling QueueStore.Delete()
// when a list of items to be deleted is needed.
func CollectIDs(items []*transport.Item) []string {
	var result []string
	for _, v := range items {
		result = append(result, v.ID)
	}
	return result
}
