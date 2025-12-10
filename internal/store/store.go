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
	ErrQueueAlreadyExists = transport.NewInvalidOption("queue already exists")
	ErrQueueNotExist      = transport.NewRequestFailed("queue does not exist")
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
	Produce(ctx context.Context, batch types.ProduceBatch, now clock.Time) error

	// Lease attempts to lease items for each request in the lease batch. It uses the batch.Iterator()
	// to evenly distribute items to all the waiting lease requests represented in the batch.
	Lease(ctx context.Context, batch types.LeaseBatch, opts LeaseOptions) error

	// Complete marks item ids in the batch as complete. If the underlying data storage fails for some
	// reason, this call returns an error. In that case the caller should assume none of the batched
	// items were marked as "complete"
	Complete(ctx context.Context, batch types.CompleteBatch) error

	// Retry retries items in the batch. Items can be retried immediately, scheduled for future retry,
	// or marked as dead for placement in the dead letter queue. If the underlying data storage fails
	// for some reason, this call returns an error. In that case the caller should assume none of the
	// batched items were retried.
	Retry(ctx context.Context, batch types.RetryBatch) error

	// List lists items in a queue. limit and offset allow the user to page through all the items
	// in the queue.
	List(ctx context.Context, items *[]*types.Item, opts types.ListOptions) error

	// TODO(scheduled) doc
	ListScheduled(ctx context.Context, items *[]*types.Item, opts types.ListOptions) error

	// Add adds the item to the queue and updates the item with the unique id.
	Add(ctx context.Context, items []*types.Item, now clock.Time) error

	// Delete removes the provided ids from the queue
	Delete(ctx context.Context, ids []types.ItemID) error

	// Clear removes items from storage based on the ClearRequest flags
	Clear(ctx context.Context, req types.ClearRequest) error

	// Stats returns stats about the queue
	Stats(ctx context.Context, stats *types.PartitionStats, now clock.Time) error

	// ScanForActions returns an iterator of actions that should be performed for the partition lifecycle.
	// Uses iter.Seq2 to yield (action, error) pairs - if error is non-nil, iteration should stop.
	//
	// ### Parameters
	// - `ctx`: Context for cancellation and timeout control.
	// - `now`: The time used to determine which items need action.
	ScanForActions(ctx context.Context, now clock.Time) iter.Seq2[types.Action, error]

	// ScanForScheduled returns an iterator of ONLY scheduled actions. This allows more efficient
	// handling of scheduled items, which operate on different timers and schedule times.
	// Uses iter.Seq2 to yield (action, error) pairs - if error is non-nil, iteration should stop.
	//
	// ### Parameters
	// - `ctx`: Context for cancellation and timeout control.
	// - `now`: The time used to determine which items are ready to be queued.
	ScanForScheduled(ctx context.Context, now clock.Time) iter.Seq2[types.Action, error]

	// TakeAction takes lifecycle requests and preforms the actions requested on the partition.
	TakeAction(ctx context.Context, batch types.LifeCycleBatch, stats *types.PartitionState) error

	// LifeCycleInfo fills out the LifeCycleInfo struct which is used to decide when the
	// next life cycle should run
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
