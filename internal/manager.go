package internal

import (
	"context"
	"github.com/kapetan-io/querator/store"
	"github.com/kapetan-io/querator/transport"
	"strings"
	"time"
)

type QueueManagerOptions struct {
	Storage      store.Storage
	QueueOptions QueueOptions
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

	if strings.TrimSpace(name) == "" {
		return nil, transport.NewInvalidOption("invalid queue_name; cannot be empty")
	}

	if strings.Contains(name, "~") {
		return nil, transport.NewInvalidOption("invalid queue_name; '%s' cannot contain '~' character", name)
	}

	q, ok := qm.queues[name]
	if !ok {
		return nil, transport.NewInvalidOption("queue does not exist; no such queue named '%s'", name)
	}
	return q, nil
}

func (qm *QueueManager) Create(_ context.Context, opts QueueOptions) (*Queue, error) {

	if strings.TrimSpace(opts.Name) == "" {
		return nil, transport.NewInvalidOption("queue 'name' cannot be empty")
	}

	if _, ok := qm.queues[opts.Name]; ok {
		return nil, transport.NewInvalidOption("duplicate queue name; '%s' already exists", opts.Name)
	}

	if opts.ReserveTimeout == 0 {
		opts.ReserveTimeout = time.Minute
	}

	if opts.DeadTimeout == 0 {
		opts.DeadTimeout = 24 * time.Hour
	}

	if opts.ReserveTimeout > opts.DeadTimeout {
		return nil, transport.NewInvalidOption("reserve_timeout is too long; %s cannot be greater than the "+
			"dead_timeout %s", opts.ReserveTimeout.String(), opts.DeadTimeout.String())
	}

	var err error
	// TODO: Use the user chosen store via qm.opts.NewQueue()
	opts.QueueStore, err = qm.opts.Storage.NewQueue(store.QueueOptions{Name: opts.Name})
	if err != nil {
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
		return transport.NewInvalidOption("queue does not exist; no such queue named '%s'", name)
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
