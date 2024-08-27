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
//type Storage interface {
//	// Partitions returns a list of store.Partition instances using the available partition info
//	// provided by types.QueueInfo. If a partition has more than one configuration, both configurations
//	// will be returned. It is therefore possible the list of partitions returned is greater than the
//	// list of requested partitions.
//	Partitions(info types.QueueInfo, partitions ...int) ([]Partition, error)
//
//	// QueueStore returns an instance of the QueueStore. A QueueStore stores
//	// QueueInfo structs, which hold information about all the available queues and partitions.
//	QueueStore(conf QueueStoreConfig) (QueueStore, error)
//
//	// TODO: Remove this, the StorageID should be owned by the storage implementation
//	ParseID(parse types.ItemID, id *StorageID) error
//	BuildStorageID(queue string, id []byte) types.ItemID
//
//	// Close closes all storage connections and open files
//	Close(ctx context.Context) error
//}

type ReserveOptions struct {
	// ReserveDeadline is a time in the future when the reservation should expire
	ReserveDeadline clock.Time
}

// TODO: Not sure this should be here, its only used by memory.go
type QueueStoreConfig struct {
}

// QueueStore is storage for listing and storing information about queues
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
// thread safe as it is intended to be used by a Logical Queue only.
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
	Stats(ctx context.Context, stats *types.QueueStats) error

	Close(ctx context.Context) error
}

// TODO: ScheduledStorage interface {} - A place to store scheduled items to be queued. (Defer)

type PartitionBackend struct {
	Name    string
	Backend Backend
}

// Backend is the thing that a store implements which returns QueueStore and Partition
// instances.
type Backend interface {
	GetQueueStore() (QueueStore, error)
	GetPartition(info types.PartitionInfo) (Partition, error)
}

type StorageConfig struct {
	QueueBackend      Backend
	PartitionBackends []PartitionBackend
}

func (c StorageConfig) ValidateConfig() error {
	return nil
}

func (c StorageConfig) GetPartition(info types.PartitionInfo) Partition {
	return nil
}

type Storage struct {
	conf StorageConfig
}

func NewStorage(conf StorageConfig) (*Storage, error) {
	// Compares the partitions in the queue store to the provided
	// partition backends.
	if err := conf.ValidateConfig(); err != nil {
		panic(err)
	}

	// Get the queue store from the configured QueueBackend
	queueStore, err := conf.QueueBackend.GetQueueStore()
	if err != nil {
		panic(err)
	}

	var info types.QueueInfo
	_ = queueStore.Get(context.Background(), "my-queue", &info)

	// Get partitions from the configured PartitionBackend
	//_ = Storage.GetPartition(info.Partitions[0])

	return nil, nil
}

func (s *Storage) Partitions(queue types.QueueInfo, partitions ...int) ([]Partition, error) {
	// TODO: Call GetPartition() on each partition backend until

	//for _, p := range partitions {
	//	for _, info := range queue.Partitions {
	//		if info.Partition == p {
	//			// Get the partition
	//			partition := s.conf.GetPartition(info)
	//		}
	//	}
	//}
	return nil, nil
}

func (s *Storage) QueueStore() (QueueStore, error) {
	return s.conf.QueueBackend.GetQueueStore()
}

// TODO: I don't think we need this
func (s *Storage) Close(ctx context.Context) error { return nil }
