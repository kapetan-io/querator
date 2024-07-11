package store

import (
	"context"
	"fmt"
	"github.com/kapetan-io/querator/internal/types"
	"time"
)

// StorageID is the decoded storage StorageID
type StorageID struct {
	Queue string
	ID    []byte
}

func (id StorageID) String() string {
	return fmt.Sprintf("%s/%s", id.Queue, id.ID)
}

// Storage is the primary storage interface
type Storage interface {
	// NewQueue creates a store.Queue instance. The Queue is used to load and store
	// items in a singular queue, which is typically backed by a single table where
	// items for this queue are stored.
	NewQueue(info QueueInfo) (Queue, error)

	// NewQueueStore creates a new instance of the QueueStore. A QueueStore stores
	// QueueInfo structs, which hold information about all the available queues.
	NewQueueStore(opts QueueStoreOptions) (QueueStore, error)

	ParseID(parse string, id *StorageID) error
	CreateID(queue, id string) string
	Close(ctx context.Context) error
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

type QueueStoreOptions struct{}

// QueueStore is storage for listing and storing information about queues
type QueueStore interface {
	// Get returns a store.Queue from storage ready to be used
	Get(ctx context.Context, name string, queue *QueueInfo) error

	// Set a queue in the store
	Set(ctx context.Context, opts QueueInfo) error

	// List returns a list of queues
	List(ctx context.Context, queues *[]*QueueInfo, opts types.ListOptions) error

	// Delete deletes a queue
	Delete(ctx context.Context, queueName string) error

	Close(ctx context.Context) error
}

// QueueInfo is information about a queue
type QueueInfo struct {
	// The name of the queue
	Name string
}

// Queue represents storage for a single queue. An instance of Queue should not be considered thread safe,
// it is intended to be used from within the internal.Queue only!
type Queue interface {
	// Produce writes the items for each batch to the data store, assigning an error for each
	// batch that fails.
	Produce(ctx context.Context, batch types.Batch[types.ProduceRequest]) error

	// Reserve attempts to reserve items for each request in the provided batch.
	Reserve(ctx context.Context, batch types.ReserveBatch, opts ReserveOptions) error

	// Complete marks ids in the batch as complete, assigning an error for each batch that fails.
	// If the underlying data storage fails for some reason, this call returns an error. In that case
	// the caller should assume none of the batched items were marked as "complete"
	Complete(ctx context.Context, batch types.Batch[types.CompleteRequest]) error

	// Read reads items in a queue. limit and offset allow the user to page through all the items
	// in the queue.
	// TODO: Consider switching to `List()` with types.ListOptions
	Read(ctx context.Context, items *[]*types.Item, pivot string, limit int) error

	// Write writes the item to the queue and updates the item with the
	// unique id.
	// TODO: Consider renaming to `Add()`
	Write(ctx context.Context, items []*types.Item) error

	// Delete removes the provided ids from the queue
	Delete(ctx context.Context, ids []string) error

	// Stats returns stats about the queue
	Stats(ctx context.Context, stats *Stats) error

	Close(ctx context.Context) error
}

// TODO: ScheduledStorage interface {} - A place to store scheduled items to be queued. (Defer)

// CollectIDs is a convenience function which assists in calling QueueStore.Delete()
// when a list of items to be deleted is needed.
func CollectIDs(items []*types.Item) []string {
	var result []string
	for _, v := range items {
		result = append(result, v.ID)
	}
	return result
}
