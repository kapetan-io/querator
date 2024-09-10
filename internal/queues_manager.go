package internal

import (
	"context"
	"fmt"
	"github.com/duh-rpc/duh-go"
	"github.com/kapetan-io/errors"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/set"
	"log/slog"
	"sync"
	"sync/atomic"
)

const MsgServiceInShutdown = "service is shutting down"

var ErrServiceShutdown = transport.NewRequestFailed(MsgServiceInShutdown)

type QueuesManagerConfig struct {
	StorageConfig store.StorageConfig
	Logger        duh.StandardLogger
	LogicalConfig LogicalConfig
}

// QueuesManager manages queues in use, and information about that queue.
type QueuesManager struct {
	queues     map[string]*Logical
	conf       QueuesManagerConfig
	inShutdown atomic.Bool
	mutex      sync.Mutex
}

func NewQueuesManager(conf QueuesManagerConfig) (*QueuesManager, error) {
	set.Default(&conf.LogicalConfig.Clock, clock.NewProvider())
	set.Default(&conf.Logger, slog.Default())

	if conf.StorageConfig.QueueStore == nil {
		return nil, errors.New("conf.StorageConfig.QueueStore cannot be nil")
	}

	if conf.StorageConfig.Backends == nil || len(conf.StorageConfig.Backends) == 0 {
		return nil, errors.New("conf.StorageConfig.Backends cannot be empty")
	}

	qm := &QueuesManager{
		queues: make(map[string]*Logical),
		conf:   conf,
	}

	return qm, nil
}

// TODO: Implement a healthcheck method call, which will ensure access to StorageConfig is working

func (qm *QueuesManager) Get(ctx context.Context, name string) (*Logical, error) {
	if qm.inShutdown.Load() {
		return nil, ErrServiceShutdown
	}
	defer qm.mutex.Unlock()
	qm.mutex.Lock()

	// If queue is already running
	q, ok := qm.queues[name]
	if ok {
		return q, nil
	}

	// Look for the queue in storage
	var queue types.QueueInfo
	if err := qm.conf.StorageConfig.QueueStore.Get(ctx, name, &queue); err != nil {
		if errors.Is(err, store.ErrQueueNotExist) {
			return nil, transport.NewInvalidOption("queue does not exist; no such queue named '%s'", name)
		}
		return nil, err
	}

	return qm.startLogicalQueue(ctx, queue)
}

func (qm *QueuesManager) Create(ctx context.Context, info types.QueueInfo) (*Logical, error) {
	if qm.inShutdown.Load() {
		return nil, ErrServiceShutdown
	}
	f := errors.Fields{"category", "querator", "func", "QueuesManager.Create"}
	defer qm.mutex.Unlock()
	qm.mutex.Lock()

	// When creating a new Queue, info.PartitionInfo should have no details, but should
	// include the number of partitions requested. The manager will decide where to
	// place the partitions depending on the storage backend configurations. As such
	// any details included by the caller will be ignored.

	for i := 0; i < info.Partitions; i++ {
		// TODO: Spread the partitions across the storage backends according to affinity
		p := types.PartitionInfo{
			StorageName: qm.conf.StorageConfig.Backends[0].Name,
			QueueName:   info.Name,
			Partition:   i,
		}
		fmt.Printf("Create partition %+v\n", p)
		info.PartitionInfo = append(info.PartitionInfo, p)
	}

	info.CreatedAt = qm.conf.LogicalConfig.Clock.Now().UTC()
	info.UpdatedAt = qm.conf.LogicalConfig.Clock.Now().UTC()
	if err := qm.conf.StorageConfig.QueueStore.Add(ctx, info); err != nil {
		f = append(f, "queue", info.Name)
		return nil, f.Errorf("QueueStore.Add(): %w", err)
	}

	for _, p := range info.PartitionInfo {
		if err := qm.conf.StorageConfig.Backends[0].PartitionStore.Create(p); err != nil {
			f = append(f, "partition", p.Partition, "storage-name", p.StorageName)
			return nil, f.Errorf("PartitionStore.Create(): %w", err)
		}
	}

	// Assertion that we are not crazy
	if _, ok := qm.queues[info.Name]; ok {
		// TODO(thrawn01): Consider a preforming a queue.UpdateInfo() if this happens instead of a panic.
		//  It's possible the data store where we keep queue info is out of sync with our actual state, in
		//  this case, it's probably better for us to update the queues when this happens.
		panic(fmt.Sprintf("queue '%s' does not exist in data store, but is running!", info.Name))
	}

	return qm.startLogicalQueue(ctx, info)
}

//func (qm *QueuesManager) rebalanceLogical(info types.QueueInfo) error {
//	// TODO: See adr/0017-cluster-operation.md
//	return nil
//}

func (qm *QueuesManager) startLogicalQueue(ctx context.Context, info types.QueueInfo) (*Logical, error) {
	f := errors.Fields{"category", "querator", "func", "QueuesManager.startLogicalQueue"}

	// Each logical queue has their own copy of these options to avoid race conditions with any
	// reconfiguration the QueuesManager may preform during cluster operation. Additionally,
	// each logical queue may independently change these options as they see fit.

	// NOTE: It is the job of the QueueManager to adjust the number of Logical Queues depending on the
	// number of consumers and partitions available.

	// TODO: The queue store should also know if the config provided is valid, IE: does every partition
	//  storage name exist in the config? If not, then it's a bad config and Querator should not start.
	// TODO: Should eventually support more than one partition depending on the current number
	//  of consumers.

	// TODO: If there is only one client, and multiple Logical Queues, then we
	//  should reduce the number of Logical Queues automatically. Using a congestion detection algorithm
	//  similar to https://www.usenix.org/conference/nsdi19/presentation/ousterhout
	// TODO: We need to figure out how clients register themselves as consumers before allowing
	//  reservation calls.

	// Get all the partitions we want associated with this logical queue instance
	p := qm.conf.StorageConfig.Backends[0].PartitionStore.Get(info.PartitionInfo[0])

	l, err := SpawnLogicalQueue(LogicalConfig{
		MaxProduceBatchSize:  qm.conf.LogicalConfig.MaxProduceBatchSize,
		MaxReserveBatchSize:  qm.conf.LogicalConfig.MaxReserveBatchSize,
		MaxCompleteBatchSize: qm.conf.LogicalConfig.MaxCompleteBatchSize,
		MaxRequestsPerQueue:  qm.conf.LogicalConfig.MaxRequestsPerQueue,
		WriteTimeout:         qm.conf.LogicalConfig.WriteTimeout,
		ReadTimeout:          qm.conf.LogicalConfig.ReadTimeout,
		Partitions:           []store.Partition{p},
		Logger:               qm.conf.Logger,
		QueueInfo:            info,
	})
	if err != nil {
		return nil, f.Wrap(err)
	}

	// TODO: This should become a list of queues which hold logical queues
	qm.queues[info.Name] = l
	return l, nil
}

func (qm *QueuesManager) List(ctx context.Context, items *[]types.QueueInfo, opts types.ListOptions) error {
	if qm.inShutdown.Load() {
		return ErrServiceShutdown
	}
	defer qm.mutex.Unlock()
	qm.mutex.Lock()

	return qm.conf.StorageConfig.QueueStore.List(ctx, items, opts)
}

func (qm *QueuesManager) Update(ctx context.Context, info types.QueueInfo) error {
	if qm.inShutdown.Load() {
		return ErrServiceShutdown
	}
	f := errors.Fields{"category", "querator", "func", "QueuesManager.Update"}
	defer qm.mutex.Unlock()
	qm.mutex.Lock()

	// Update the queue info in the data store
	info.UpdatedAt = qm.conf.LogicalConfig.Clock.Now().UTC()
	if err := qm.conf.StorageConfig.QueueStore.Update(ctx, info); err != nil {
		return f.Errorf("QueueStore.Update(): %w", err)
	}

	// If the queue is currently in use
	q, ok := qm.queues[info.Name]
	if !ok {
		return nil
	}

	// Update the active queue with the latest queue info
	if err := q.UpdateInfo(ctx, info); err != nil {
		return f.Errorf("LogicalQueue.UpdateInfo(): %w", err)
	}

	return nil
}

func (qm *QueuesManager) Delete(ctx context.Context, name string) error {
	if qm.inShutdown.Load() {
		return ErrServiceShutdown
	}
	f := errors.Fields{"category", "querator", "func", "QueuesManager.Delete"}
	defer qm.mutex.Unlock()
	qm.mutex.Lock()

	// TODO: Delete should return transport.NewInvalidOption("queue does not exist; no such queue named '%s'", name)
	if err := qm.conf.StorageConfig.QueueStore.Delete(ctx, name); err != nil {
		return f.Errorf("QueueStore.Delete(): %w", err)
	}

	// If the queue is currently in use
	q, ok := qm.queues[name]
	if !ok {
		return nil
	}

	// Then shutdown the queue
	if err := q.Shutdown(ctx); err != nil {
		return f.Errorf("LogicalQueue.Shutdown(): %w", err)
	}

	delete(qm.queues, name)
	return nil
}

func (qm *QueuesManager) Shutdown(ctx context.Context) error {
	if qm.inShutdown.Load() {
		return nil
	}
	f := errors.Fields{"category", "querator", "func", "QueuesManager.Shutdown"}

	fmt.Printf("QueuesManager.Shutdown()\n")
	qm.inShutdown.Store(true)
	defer qm.mutex.Unlock()
	qm.mutex.Lock()

	fmt.Printf("QueuesManager.Shutdown() queues len %d\n", len(qm.queues))
	wait := make(chan error)
	go func() {
		for _, q := range qm.queues {
			fmt.Printf("QueuesManager.Shutdown() queue '%s'\n", q.conf.Name)
			if err := q.Shutdown(ctx); err != nil {
				wait <- err
				return
			}
			fmt.Printf("QueuesManager.Shutdown() queue '%s' - DONE\n", q.conf.Name)
		}
		fmt.Printf("QueuesManager.Shutdown() store.Close()\n")
		if err := qm.conf.StorageConfig.QueueStore.Close(ctx); err != nil {
			wait <- err
			return
		}
		close(wait)
	}()

	fmt.Printf("QueuesManager.Shutdown() wait\n")
	select {
	case <-ctx.Done():
		fmt.Printf("QueuesManager.Shutdown() wait ctx cancel\n")
		return f.Wrap(ctx.Err())
	case err := <-wait:
		fmt.Printf("QueuesManager.Shutdown() wait done\n")
		return f.Wrap(err)
	}
}
