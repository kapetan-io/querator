package store

import (
	"context"
	"fmt"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/tackle/clock"
)

var ErrQueueNotExist = transport.NewRequestFailed("queue does not exist")

// StorageID is the decoded storage StorageID
type StorageID struct {
	ID    types.ItemID
	Queue string
}

func (id StorageID) String() string {
	return fmt.Sprintf("%s~%s", id.Queue, id.ID)
}

// Storage is the primary storage interface
type Storage interface {
	// NewQueue creates a store.Queue instance. The Queue is used to load and store
	// items in a singular queue, which is typically backed by a single table where
	// items for this queue are stored.
	NewQueue(info types.QueueInfo) (Queue, error)

	// NewQueuesStore creates a new instance of the QueuesStore. A QueuesStore stores
	// QueueInfo structs, which hold information about all the available queues.
	NewQueuesStore(conf QueuesStoreConfig) (QueuesStore, error)

	ParseID(parse types.ItemID, id *StorageID) error
	BuildStorageID(queue string, id []byte) types.ItemID
	Close(ctx context.Context) error
}

type ReserveOptions struct {
	// ReserveDeadline is a time in the future when the reservation should expire
	ReserveDeadline clock.Time
}

// TODO: Not sure this should be here, its only used by memory.go
type QueuesStoreConfig struct {
}

// QueuesStore is storage for listing and storing information about queues
type QueuesStore interface {
	// Get returns a store.Queue from storage ready to be used. Returns ErrQueueNotExist if the
	// queue requested does not exist
	Get(ctx context.Context, name string, queue *types.QueueInfo) error

	// Add a queue in the store. if the queue already exists returns an error
	Add(ctx context.Context, info types.QueueInfo) error

	// Update a queue in the store if the queue already exists it updates the existing QueueInfo
	Update(ctx context.Context, info types.QueueInfo) error

	// List returns a list of queues
	List(ctx context.Context, queues *[]types.QueueInfo, opts types.ListOptions) error

	// Delete deletes a queue. Returns without error if the queue does not exist
	Delete(ctx context.Context, queueName string) error

	// Close the all open database connections or files
	Close(ctx context.Context) error
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

	// List lists items in a queue. limit and offset allow the user to page through all the items
	// in the queue.
	List(ctx context.Context, items *[]*types.Item, opts types.ListOptions) error

	// Add adds the item to the queue and updates the item with the unique id.
	Add(ctx context.Context, items []*types.Item) error

	// Delete removes the provided ids from the queue
	Delete(ctx context.Context, ids []types.ItemID) error

	// Clear removes all items from storage
	Clear(ctx context.Context, destructive bool) error

	// Stats returns stats about the queue
	Stats(ctx context.Context, stats *types.QueueStats) error

	Close(ctx context.Context) error
}

// TODO: ScheduledStorage interface {} - A place to store scheduled items to be queued. (Defer)
