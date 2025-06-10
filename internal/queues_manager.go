package internal

import (
	"context"
	"github.com/kapetan-io/errors"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/set"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
)

const MsgServiceInShutdown = "service is shutting down"

var ErrServiceShutdown = transport.NewRequestFailed(MsgServiceInShutdown)

type QueuesManagerConfig struct {
	StorageConfig store.Config
	LogicalConfig LogicalConfig
	Log           *slog.Logger
}

// Remote represents a remote instance of querator. Implementations proxy requests to the remote instance
type Remote interface {
	QueueLease(ctx context.Context, req *proto.QueueLeaseRequest, res *proto.QueueLeaseResponse) error
	QueueProduce(ctx context.Context, req *proto.QueueProduceRequest) error
	QueueComplete(ctx context.Context, req *proto.QueueCompleteRequest) error
	QueueRetry(ctx context.Context, req *proto.QueueRetryRequest) error
	StorageItemsList(ctx context.Context, req *proto.StorageItemsListRequest,
		res *proto.StorageItemsListResponse) error
	StorageItemsImport(ctx context.Context, req *proto.StorageItemsImportRequest,
		res *proto.StorageItemsImportResponse) error
	StorageItemsDelete(ctx context.Context, req *proto.StorageItemsDeleteRequest) error
}

// TODO: It is the job of the QueueManager to adjust the number of Logical Queues depending on the
//  number of consumers and partitions available.

// TODO: If there is only one client, and multiple Logical Queues, then we
//  should reduce the number of Logical Queues automatically. Using a congestion detection algorithm
//  similar to https://www.usenix.org/conference/nsdi19/presentation/ousterhout

// TODO: We need to figure out how clients register themselves as consumers before allowing
//  lease calls.

// QueuesManager manages queues in use, and information about that queue.
type QueuesManager struct {
	queues     map[string]*Queue
	conf       QueuesManagerConfig
	inShutdown atomic.Bool
	mutex      sync.RWMutex
	log        *slog.Logger
}

func NewQueuesManager(conf QueuesManagerConfig) (*QueuesManager, error) {
	set.Default(&conf.LogicalConfig.Clock, clock.NewProvider())
	set.Default(&conf.Log, slog.Default())

	if conf.StorageConfig.Queues == nil {
		return nil, errors.New("conf.Config.Queues cannot be nil")
	}

	if len(conf.StorageConfig.PartitionStorage) == 0 {
		return nil, errors.New("conf.Config.PartitionStorage cannot be empty")
	}

	qm := &QueuesManager{
		log:    conf.Log.With("code.namespace", "QueuesManager"),
		queues: make(map[string]*Queue),
		conf:   conf,
	}

	return qm, nil
}

// TODO: Implement a healthcheck method call, which will ensure access to Config is working

func (qm *QueuesManager) Create(ctx context.Context, info types.QueueInfo) (*Queue, error) {
	if qm.inShutdown.Load() {
		return nil, ErrServiceShutdown
	}
	defer qm.mutex.Unlock()
	qm.mutex.Lock()

	q, ok := qm.queues[info.Name]
	if ok {
		return q, transport.NewInvalidOption("invalid queue; '%s' already exists", info.Name)
	}

	// When creating a new Queue, info.PartitionInfo should have no details, but should
	// include the number of partitions requested. This QueueManager will decide where to
	// place the partitions depending on the storage backend configurations. As such
	// any details included by the caller will be ignored.

	// TODO: Test partitions are spread across multiple backends
	// Spread the partitions across the storage backends according to affinity
	var partitionIdx = 0
	for idx, count := range qm.assignPartitions(info.RequestedPartitions) {
		for i := 0; i < count; i++ {
			p := types.PartitionInfo{
				StorageName:  qm.conf.StorageConfig.PartitionStorage[idx].Name,
				PartitionNum: partitionIdx,
				Queue:        info,
			}
			partitionIdx++
			info.PartitionInfo = append(info.PartitionInfo, p)
			qm.log.LogAttrs(ctx, LevelDebugAll, "Partition Assigned",
				slog.String("queue", p.Queue.Name), slog.String("storage", p.StorageName),
				slog.Bool("read-only", p.ReadOnly), slog.Int("partition", p.PartitionNum))
		}
	}

	info.CreatedAt = qm.conf.LogicalConfig.Clock.Now().UTC()
	info.UpdatedAt = qm.conf.LogicalConfig.Clock.Now().UTC()
	if err := qm.conf.StorageConfig.Queues.Add(ctx, info); err != nil {
		return nil, errors.With("queue", info.Name).
			Errorf("Queues.Add(): %w", err)
	}

	return qm.get(ctx, info.Name)
}

func (qm *QueuesManager) Get(ctx context.Context, name string) (*Queue, error) {
	if qm.inShutdown.Load() {
		return nil, ErrServiceShutdown
	}
	defer qm.mutex.RUnlock()
	qm.mutex.RLock()

	return qm.get(ctx, name)
}

func (qm *QueuesManager) get(ctx context.Context, name string) (*Queue, error) {
	// If queue is already running
	q, ok := qm.queues[name]
	if ok {
		return q, nil
	}

	// Look for the queue in storage
	var queue types.QueueInfo
	if err := qm.conf.StorageConfig.Queues.Get(ctx, name, &queue); err != nil {
		if errors.Is(err, store.ErrQueueNotExist) {
			return nil, transport.NewInvalidOption("queue does not exist; no such queue named '%s'", name)
		}
		return nil, err
	}

	// Get the partitions from the store
	var partitions []store.Partition
	for _, info := range queue.PartitionInfo {

		b := store.Find(info.StorageName, qm.conf.StorageConfig.PartitionStorage)
		if b.Name == "" {
			return nil, errors.WithAttr(
				slog.Int("partition", info.PartitionNum),
				slog.String("storage-name", info.StorageName),
				slog.String("queue", queue.Name),
			).Error("queue partition references unknown storage name")
		}
		partitions = append(partitions, b.PartitionStore.Get(info))
	}

	// We currently start with a single logical queue, contention detection will eventually adjust the
	// number of logical queues as consumers join.
	l, err := SpawnLogicalQueue(LogicalConfig{
		MaxProduceBatchSize:  qm.conf.LogicalConfig.MaxProduceBatchSize,
		MaxLeaseBatchSize:    qm.conf.LogicalConfig.MaxLeaseBatchSize,
		MaxCompleteBatchSize: qm.conf.LogicalConfig.MaxCompleteBatchSize,
		MaxRequestsPerQueue:  qm.conf.LogicalConfig.MaxRequestsPerQueue,
		WriteTimeout:         qm.conf.LogicalConfig.WriteTimeout,
		ReadTimeout:          qm.conf.LogicalConfig.ReadTimeout,
		Clock:                qm.conf.LogicalConfig.Clock,
		Log:                  qm.conf.Log,
		StoragePartitions:    partitions,
		QueueInfo:            queue,
	})
	if err != nil {
		return nil, errors.Wrap(err)
	}
	qm.queues[queue.Name] = NewQueue(queue).AddLogical(l)
	return qm.queues[queue.Name], nil
}

//func (qm *QueuesManager) rebalanceLogical(info types.QueueInfo) error {
//	// TODO: See adr/0017-cluster-operation.md
//	return nil
//}

func (qm *QueuesManager) List(ctx context.Context, items *[]types.QueueInfo, opts types.ListOptions) error {
	if qm.inShutdown.Load() {
		return ErrServiceShutdown
	}
	defer qm.mutex.Unlock()
	qm.mutex.Lock()

	return qm.conf.StorageConfig.Queues.List(ctx, items, opts)
}

func (qm *QueuesManager) Update(ctx context.Context, info types.QueueInfo) error {
	if qm.inShutdown.Load() {
		return ErrServiceShutdown
	}
	defer qm.mutex.Unlock()
	qm.mutex.Lock()

	// Update the queue info in the data store
	info.UpdatedAt = qm.conf.LogicalConfig.Clock.Now().UTC()
	if err := qm.conf.StorageConfig.Queues.Update(ctx, info); err != nil {
		return errors.Errorf("Queues.Update(): %w", err)
	}

	// If the queue is currently in use
	q, ok := qm.queues[info.Name]
	if !ok {
		return nil
	}

	for _, l := range q.GetAll() {
		if err := l.UpdateInfo(ctx, info); err != nil {
			return errors.Errorf("LogicalQueue.UpdateInfo(): %w", err)
		}
	}

	return nil
}

func (qm *QueuesManager) LifeCycle(ctx context.Context, req *types.LifeCycleRequest) error {
	return nil // TODO(lifecycle)
}

func (qm *QueuesManager) Delete(ctx context.Context, name string) error {
	if qm.inShutdown.Load() {
		return ErrServiceShutdown
	}
	defer qm.mutex.Unlock()
	qm.mutex.Lock()

	// TODO: Delete should return transport.NewInvalidOption("queue does not exist; no such queue named '%s'", name)
	if err := qm.conf.StorageConfig.Queues.Delete(ctx, name); err != nil {
		return errors.Errorf("Queues.Delete(): %w", err)
	}

	// If the queue is currently in use
	q, ok := qm.queues[name]
	if !ok {
		return nil
	}

	for _, l := range q.GetAll() {
		if err := l.Shutdown(ctx); err != nil {
			return errors.Errorf("LogicalQueue.Shutdown(): %w", err)
		}
	}

	delete(qm.queues, name)
	return nil
}

func (qm *QueuesManager) Shutdown(ctx context.Context) error {
	if qm.inShutdown.Load() {
		return nil
	}

	qm.inShutdown.Store(true)
	defer qm.mutex.Unlock()
	qm.mutex.Lock()

	wait := make(chan error)
	go func() {
		for _, q := range qm.queues {
			qm.log.LogAttrs(ctx, LevelDebugAll, "shutdown logical",
				slog.Int("num_queues", len(qm.queues)),
				slog.String("queue", q.Info().Name))
			for _, l := range q.GetAll() {
				if err := l.Shutdown(ctx); err != nil {
					wait <- err
				}
			}
		}
		qm.log.LogAttrs(ctx, LevelDebugAll, "close queue store")
		if err := qm.conf.StorageConfig.Queues.Close(ctx); err != nil {
			wait <- err
			return
		}
		close(wait)
	}()

	select {
	case <-ctx.Done():
		qm.log.Warn("ctx cancelled while waiting for shutdown")
		return errors.Wrap(ctx.Err())
	case err := <-wait:
		qm.log.LogAttrs(ctx, LevelDebugAll, "Shutdown complete")
		return errors.Wrap(err)
	}
}

// assignPartitions assigns partition counts to a backend based on affinity
func (qm *QueuesManager) assignPartitions(totalPartitions int) []int {
	var totalAffinity float64
	for _, backend := range qm.conf.StorageConfig.PartitionStorage {
		totalAffinity += backend.Affinity
	}
	assignments := make([]int, len(qm.conf.StorageConfig.PartitionStorage))
	for i, b := range qm.conf.StorageConfig.PartitionStorage {
		assignments[i] = int(math.Floor((b.Affinity / totalAffinity) * float64(totalPartitions)))
	}
	return assignments
}
