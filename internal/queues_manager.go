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

var ErrServiceShutdown = transport.NewRequestFailed("service is shutting down")

type QueuesManagerOptions struct {
	Logger       duh.StandardLogger
	Storage      store.Storage
	QueueOptions QueueOptions
}

// QueuesManager manages queues in use, and information about that queue.
type QueuesManager struct {
	queues     map[string]*Queue
	opts       QueuesManagerOptions
	store      store.QueuesStore
	inShutdown atomic.Bool
	mutex      sync.Mutex
}

func NewQueuesManager(opts QueuesManagerOptions) (*QueuesManager, error) {
	set.Default(&opts.Logger, slog.Default())

	s, err := opts.Storage.NewQueuesStore(store.QueuesStoreOptions{})
	if err != nil {
		return nil, err
	}

	return &QueuesManager{
		queues: make(map[string]*Queue),
		opts:   opts,
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
	opts := QueueOptions{QueueInfo: info}

	// Each queue has their own copy of these options to avoid race conditions with any
	// reconfiguration the QueuesManager may preform during cluster operation. Additionally,
	// each queue may independently change these options as they see fit.

	// Assign all the server level configuration to QueueOptions.
	set.Default(&opts.MaxProduceBatchSize, qm.opts.QueueOptions.MaxProduceBatchSize)
	set.Default(&opts.MaxReserveBatchSize, qm.opts.QueueOptions.MaxReserveBatchSize)
	set.Default(&opts.MaxCompleteBatchSize, qm.opts.QueueOptions.MaxCompleteBatchSize)
	set.Default(&opts.WriteTimeout, qm.opts.QueueOptions.WriteTimeout)
	set.Default(&opts.ReadTimeout, qm.opts.QueueOptions.ReadTimeout)
	set.Default(&opts.Logger, qm.opts.Logger)

	var err error
	opts.QueueStore, err = qm.opts.Storage.NewQueue(info)
	if err != nil {
		return nil, err
	}

	q, err := NewQueue(opts)
	if err != nil {
		return nil, err
	}

	qm.queues[opts.Name] = q
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

	qm.inShutdown.Store(true)
	defer qm.mutex.Unlock()
	qm.mutex.Lock()

	wait := make(chan error)
	go func() {
		for _, q := range qm.queues {
			if err := q.Shutdown(ctx); err != nil {
				wait <- err
				return
			}
		}
		if err := qm.store.Close(ctx); err != nil {
			wait <- err
			return
		}
		close(wait)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-wait:
		return err
	}
}
