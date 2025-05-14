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
	ErrQueueNotExist      = transport.NewRequestFailed("queue does not exist")
	ErrQueueAlreadyExists = transport.NewInvalidOption("queue already exists")
	theFuture             = time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
)

type LeaseOptions struct {
	// LeaseDeadline is a time in the future when the lease should expire
	LeaseDeadline clock.Time
}

// Queues is storage for listing and storing information about queues. The Queues store implementations
// should employ lazy storage initialization such that it makes contact or creates underlying tables only
// upon first invocation. See 0021-storage-lazy-initialization.md for details.
type Queues interface {
	// Get fetches QueueInfo from storage. Returns ErrQueueNotExist if the
	// queue requested does not exist
	Get(ctx context.Context, name string, queue *types.QueueInfo) error

	// Add a QueueInfo to the store. if the queue already exists returns an error
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
	// Produce writes the items in the batch to the data store.
	Produce(ctx context.Context, batch types.ProduceBatch) error

	// Lease attempts to lease items for each request in the lease batch. It uses the batch.Iterator()
	// to evenly distribute items to all the waiting lease requests represented in the batch.
	Lease(ctx context.Context, batch types.LeaseBatch, opts LeaseOptions) error

	// Complete marks item ids in the batch as complete. If the underlying data storage fails for some
	// reason, this call returns an error. In that case the caller should assume none of the batched
	// items were marked as "complete"
	Complete(ctx context.Context, batch types.Batch[types.CompleteRequest]) error

	// List lists items in a queue. limit and offset allow the user to page through all the items
	// in the queue.
	List(ctx context.Context, items *[]*types.Item, opts types.ListOptions) error

	// Add adds the item to the queue and updates the item with the unique id.
	Add(ctx context.Context, items []*types.Item) error

	// Delete removes the provided ids from the queue
	Delete(ctx context.Context, ids []types.ItemID) error

	// Clear removes all items from storage. If destructive is true it removes all items
	// in the queue regardless of status
	Clear(ctx context.Context, destructive bool) error

	// Stats returns stats about the queue
	Stats(ctx context.Context, stats *types.PartitionStats) error

	// ScanForActions returns an iterator of actions that should be preformed for the partition life cycle.
	// This method must be thread safe as it will be called by a go routine that is separate from the
	// main request loop. If an error is encountered. returns an empty iterator. Issues with the
	// underlying storage system are reported by LifeCycleInfo()
	//
	// ### Parameters
	// - `timeout`: The read timeout for each read operation to the underlying storage system.
	// If a read exceeds this timeout, the iterator is aborted.
	// - `now`: The time used by LifeCycleActions to determine which items need action.
	ScanForActions(timeout clock.Duration, now clock.Time) iter.Seq[types.Action]

	// TakeAction takes lifecycle requests and preforms the actions requested on the partition.
	TakeAction(ctx context.Context, batch types.Batch[types.LifeCycleRequest]) error

	// LifeCycleInfo fills out the LifeCycleInfo struct which is used to decide when the
	// next life cycle should run
	// TODO: Rename this, this is too generic for what it currently does.
	LifeCycleInfo(ctx context.Context, info *types.LifeCycleInfo) error

	// Info returns the Partition Info for this partition. This call needs to be thread
	// safe as it may be called from a separate go routine.
	Info() types.PartitionInfo

	// UpdateQueueInfo updates the queue information for this partition. This is called when
	// a user updates queue info that needs to take effect immediately. This call needs to be
	// thread safe as it may be called from a separate go routine.
	UpdateQueueInfo(info types.QueueInfo)

	// Close any open connections or files associated with the storage system
	Close(ctx context.Context) error
}

func Find(needle string, haystack []PartitionStorage) PartitionStorage {
	for _, i := range haystack {
		if i.Name == needle {
			return i
		}
	}
	return PartitionStorage{}
}

// Config is the configuration accepted by QueueManager to manage storage of queues, scheduled items,
// and partitions.
type Config struct {
	// Queues where metadata about the queues is stored
	Queues Queues
	// The PartitionStorage configured for partitions to utilize
	PartitionStorage []PartitionStorage
	// Clock is the clock provider the backend implementation should use
	Clock *clock.Provider
	// Log is the logger to be used
	Log *slog.Logger
}

// PartitionStorage is the struct which holds the configured partition backend interfaces
type PartitionStorage struct {
	PartitionStore PartitionStore
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
