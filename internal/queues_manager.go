package internal

import (
	"context"
	"errors"
	"fmt"
	"github.com/duh-rpc/duh-go"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/tackle/set"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

const MsgServiceInShutdown = "service is shutting down"

var ErrServiceShutdown = transport.NewRequestFailed(MsgServiceInShutdown)

type QueuesManagerConfig struct {
	Logger      duh.StandardLogger
	Storage     store.Storage
	QueueConfig QueueConfig
}

// QueuesManager manages queues in use, and information about that queue.
type QueuesManager struct {
	queues     map[string]*Queue
	conf       QueuesManagerConfig
	store      store.QueuesStore
	inShutdown atomic.Bool
	mutex      sync.Mutex
}

func NewQueuesManager(conf QueuesManagerConfig) (*QueuesManager, error) {
	set.Default(&conf.Logger, slog.Default())

	s, err := conf.Storage.NewQueuesStore(store.QueuesStoreConfig{})
	if err != nil {
		return nil, err
	}

	return &QueuesManager{
		queues: make(map[string]*Queue),
		conf:   conf,
		store:  s,
	}, nil
}

func (qm *QueuesManager) Get(ctx context.Context, name string) (*Queue, error) {
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
	if err := qm.store.Get(ctx, name, &queue); err != nil {
		if errors.Is(err, store.ErrQueueNotExist) {
			return nil, transport.NewInvalidOption("queue does not exist; no such queue named '%s'", name)
		}
		return nil, err
	}

	return qm.startQueue(queue)
}

func (qm *QueuesManager) Create(ctx context.Context, info types.QueueInfo) (*Queue, error) {
	if qm.inShutdown.Load() {
		return nil, ErrServiceShutdown
	}
	defer qm.mutex.Unlock()
	qm.mutex.Lock()

	//set.Default(&info.ReserveTimeout, time.Minute)
	//set.Default(&info.DeadTimeout, 24*time.Hour)

	info.CreatedAt = time.Now().UTC()
	info.UpdatedAt = time.Now().UTC()
	if err := qm.store.Add(ctx, info); err != nil {
		return nil, err
	}

	// Assertion that we are not crazy
	if _, ok := qm.queues[info.Name]; ok {
		// TODO(thrawn01): Consider a preforming a queue.UpdateInfo() if this happens instead of a panic.
		//  It's possible the data store where we keep queue info is out of sync with our actual state, in
		//  this case, it's probably better for us to update the queues when this happens.
		panic(fmt.Sprintf("queue '%s' does not exist in data store, but is running!", info.Name))
	}

	return qm.startQueue(info)
}

func (qm *QueuesManager) startQueue(info types.QueueInfo) (*Queue, error) {

	// Each queue has their own copy of these options to avoid race conditions with any
	// reconfiguration the QueuesManager may preform during cluster operation. Additionally,
	// each queue may independently change these options as they see fit.

	conf := QueueConfig{
		MaxProduceBatchSize:  qm.conf.QueueConfig.MaxProduceBatchSize,
		MaxReserveBatchSize:  qm.conf.QueueConfig.MaxReserveBatchSize,
		MaxCompleteBatchSize: qm.conf.QueueConfig.MaxCompleteBatchSize,
		MaxRequestsPerQueue:  qm.conf.QueueConfig.MaxRequestsPerQueue,
		WriteTimeout:         qm.conf.QueueConfig.WriteTimeout,
		ReadTimeout:          qm.conf.QueueConfig.ReadTimeout,
		Logger:               qm.conf.Logger,
		QueueInfo:            info,
	}

	var err error
	conf.QueueStore, err = qm.conf.Storage.NewQueue(info)
	if err != nil {
		return nil, err
	}

	q, err := NewQueue(conf)
	if err != nil {
		return nil, err
	}

	qm.queues[conf.Name] = q
	return q, nil
}

func (qm *QueuesManager) List(ctx context.Context, items *[]types.QueueInfo, opts types.ListOptions) error {
	if qm.inShutdown.Load() {
		return ErrServiceShutdown
	}
	defer qm.mutex.Unlock()
	qm.mutex.Lock()

	return qm.store.List(ctx, items, opts)
}

func (qm *QueuesManager) Update(ctx context.Context, info types.QueueInfo) error {
	if qm.inShutdown.Load() {
		return ErrServiceShutdown
	}
	defer qm.mutex.Unlock()
	qm.mutex.Lock()

	// Update the queue info in the data store
	info.UpdatedAt = time.Now().UTC()
	if err := qm.store.Update(ctx, info); err != nil {
		return err
	}

	// If the queue is currently in use
	q, ok := qm.queues[info.Name]
	if !ok {
		return nil
	}

	// Update the active queue with the latest queue info
	if err := q.UpdateInfo(ctx, info); err != nil {
		return err
	}

	return nil
}

func (qm *QueuesManager) Delete(ctx context.Context, name string) error {
	if qm.inShutdown.Load() {
		return ErrServiceShutdown
	}
	defer qm.mutex.Unlock()
	qm.mutex.Lock()

	// TODO: Delete should return transport.NewInvalidOption("queue does not exist; no such queue named '%s'", name)
	if err := qm.store.Delete(ctx, name); err != nil {
		return err
	}

	// If the queue is currently in use
	q, ok := qm.queues[name]
	if !ok {
		return nil
	}

	// Then shutdown the queue
	if err := q.Shutdown(ctx); err != nil {
		return err
	}

	delete(qm.queues, name)
	return nil
}

func (qm *QueuesManager) Shutdown(ctx context.Context) error {
	if qm.inShutdown.Load() {
		return nil
	}

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
		if err := qm.store.Close(ctx); err != nil {
			wait <- err
			return
		}
		close(wait)
	}()

	fmt.Printf("QueuesManager.Shutdown() wait\n")
	select {
	case <-ctx.Done():
		fmt.Printf("QueuesManager.Shutdown() wait ctx cancel\n")
		return ctx.Err()
	case err := <-wait:
		fmt.Printf("QueuesManager.Shutdown() wait done\n")
		return err
	}
}
