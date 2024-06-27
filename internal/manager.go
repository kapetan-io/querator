package internal

import (
	"context"
	"github.com/kapetan-io/querator/store"
	"github.com/kapetan-io/querator/transport"
	"time"
)

type QueueManagerOptions struct {
	// Instantiates a new store for the given queue name
	NewQueueStorage func(name string) store.QueueStorage
}

type QueueManager struct {
	queues map[string]*Queue
	opts   QueueManagerOptions
}

func NewQueueManager(opts QueueManagerOptions) *QueueManager {
	return &QueueManager{
		queues: make(map[string]*Queue),
		opts:   opts,
	}
}

func (qm *QueueManager) Get(_ context.Context, name string) (*Queue, error) {
	q, ok := qm.queues[name]
	if !ok {
		return nil, transport.NewInvalidRequest("queue '%s' does not exist", name)
	}
	return q, nil
}

func (qm *QueueManager) Create(_ context.Context, opts QueueOptions) (*Queue, error) {
	if _, ok := qm.queues[opts.Name]; ok {
		return nil, transport.NewInvalidRequest("queue '%s' already exists", opts.Name)
	}

	// Set Defaults if none are provided
	if opts.ReserveTimeout == 0 {
		opts.ReserveTimeout = time.Minute
	}

	if opts.DeadTimeout == 0 {
		opts.DeadTimeout = 24 * time.Hour
	}

	if opts.ReserveTimeout > opts.DeadTimeout {
		return nil, transport.NewInvalidRequest("ReserveTimeout cannot be longer than the DeadTimeout (%s > %s)",
			opts.ReserveTimeout.String(), opts.DeadTimeout.String())
	}

	// TODO: Use the user chosen store via qm.opts.NewQueueStorage()

	var err error
	opts.QueueStorage, err = store.NewBuntStore(store.BuntOptions{File: ":memory:"})
	if err != nil {
		if store.IsErrInvalidOption(err) {
			return nil, transport.NewInvalidRequest(err.Error())
		}
		return nil, err
	}

	q, err := NewQueue(opts)
	if err != nil {
		return nil, err
	}

	// TODO: Set createdAt and persist to to the store

	qm.queues[opts.Name] = q
	return q, nil
}

func (qm *QueueManager) Delete(ctx context.Context, name string) error {
	q, ok := qm.queues[name]
	if !ok {
		return transport.NewInvalidRequest("queue '%s' does not exist", name)
	}

	if err := q.Shutdown(ctx); err != nil {
		return err
	}

	delete(qm.queues, name)
	return nil
}

func (qm *QueueManager) Shutdown(ctx context.Context) error {
	wait := make(chan error)
	go func() {
		for _, q := range qm.queues {
			if err := q.Shutdown(ctx); err != nil {
				wait <- err
				return
			}
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
