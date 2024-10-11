package store

import (
	"context"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/tackle/clock"
	"iter"
	"log/slog"
	"time"
)

const (
	LevelDebugAll = slog.LevelDebug
	LevelDebug    = slog.LevelDebug + 1
)

var (
	ErrQueueNotExist = transport.NewRequestFailed("queue does not exist")
	theFuture        = time.Date(2800, 1, 1, 1, 1, 1, 1, time.UTC)
)

type ReserveOptions struct {
	// ReserveDeadline is a time in the future when the reservation should expire
	ReserveDeadline clock.Time
}

// QueueStore is storage for listing and storing information about queues.  The QueueStore should employ
// lazy storage initialization such that it makes contact or creates underlying tables only
// upon first invocation. See 0021-storage-lazy-initialization.md for details.
type QueueStore interface {
	// Get returns a store.Partition from storage ready to be used. Returns ErrQueueNotExist if the
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

// Partition represents storage for a single partition. An instance of Partition should not be considered
// thread safe as it is intended to be used by a Logical Queue only. The partition should employ
// lazy storage initialization such that it makes contact or creates underlying tables only
// upon first invocation. See 0021-storage-lazy-initialization.md for details.
type Partition interface {
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
	Stats(ctx context.Context, stats *types.PartitionStats) error

	// LifeCycleActions returns an iterator that traverses actions required for the partition life cycle.
	// This method must be thread safe as it will be called by a go routine that is separate from the
	// main request loop.
	//
	// ### Parameters
	// - `timeout`: The read timeout for each read operation to the underlying storage system.
	// If a read exceeds this timeout, the iterator is aborted.
	// - `now`: The time used by LifeCycleActions to determine which items need action.
	LifeCycleActions(timeout clock.Duration, now clock.Time) iter.Seq[types.Action]

	LifeCycleInfo(ctx context.Context, info *types.LifeCycleInfo) error

	// Info returns the Partition Info for this partition. This call should be thread
	// safe as it may be called from a separate go routine.
	Info() types.PartitionInfo

	// UpdateQueueInfo updates the queue information for this partition. This is called when
	// a user updates queue info that needs to take effect immediately. This call should be thread
	// safe as it may be called from a separate go routine.
	UpdateQueueInfo(info types.QueueInfo)

	Close(ctx context.Context) error
}

type Backends []Backend

func (b Backends) Find(name string) Backend {
	for _, backend := range b {
		if backend.Name == name {
			return backend
		}
	}
	return Backend{}
}

// TODO: Rename this to `store.Config` if possible
// StorageConfig is the configuration accepted by QueueManager to manage storage of queues, scheduled items,
// and partitions.
type StorageConfig struct {
	// How Queues are stored can be separate from PartitionInfo and Scheduled stores
	QueueStore QueueStore
	// The Backends configured for partitions to utilize
	Backends Backends
	// Clock is the clock provider the backend implementation should use
	Clock *clock.Provider
}

// Backend is the struct which holds the configured partition backend interfaces
type Backend struct {
	PartitionStore PartitionStore
	ScheduledStore ScheduledStore
	Affinity       float64
	Name           string
}

// PartitionStore manages the partitions
type PartitionStore interface {
	// Get returns a new Partition instance for the requested partition. NOTE: This method does
	// NOT return an error. See 0021-storage-lazy-initialization.md for an explanation.
	Get(types.PartitionInfo) Partition

	// TODO: List StoragePartitions, Delete StoragePartitions, etc...
}

// TODO: A scheduled store should probably be located or managed by a partition store, possibly in the same table
//  or a separate table on the same data storage as the partition table. Queued schedule items should be distributed
//  across partitions as to avoid a throughput bottle neck, and should be queued into whatever partition the logical
//  queue has access to when the scheduled item deadline is reached. This avoids complication
//

// ScheduledStore manages scheduled and deferred items
type ScheduledStore interface {
	Create(types.PartitionInfo) error
	Get(types.PartitionInfo) Scheduled
}

// TODO: Design the Scheduled interface
type Scheduled interface {
}
