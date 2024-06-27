package internal

import (
	"context"
	"github.com/kapetan-io/querator/store"
	"github.com/kapetan-io/querator/transport"
)

type QueueManagerOptions struct {
	// Instantiates a new store for the given queue name
	NewQueueStorage func(name string) store.QueueStorage
}

type QueueManager struct {
	queues map[string]*Queue
	opts   QueueManagerOptions
}

func (qm *QueueManager) Get(_ context.Context, queueName string) (*Queue, error) {
	q, ok := qm.queues[queueName]
	if !ok {
		return nil, transport.NewInvalidRequest("queue '%s' does not exist", queueName)
	}
	return q, nil
}

func (qm *QueueManager) Create(_ context.Context, opts QueueOptions) (*Queue, error) {
	if _, ok := qm.queues[opts.Name]; ok {
		return nil, transport.NewInvalidRequest("queue '%s' already exists", opts.Name)
	}

	if opts.ReserveTimeout > opts.DeadTimeout {
		return nil, transport.NewInvalidRequest("ReserveTimeout cannot be longer than the DeadTimeout (%s > %s)",
			opts.ReserveTimeout.String(), opts.DeadTimeout.String())
	}

	var err error
	// TODO: Use the user chosen store via qm.opts.NewQueueStorage()
	opts.QueueStorage, err = store.NewBuntStore(store.BuntOptions{File: ":memory:"})
	if err != nil {
		if store.IsErrInvalid(err) {
			return nil, transport.NewInvalidRequest(err.Error())
		}
		return nil, err
	}

	q, err := NewQueue(opts)
	if err != nil {
		return nil, err
	}

	qm.queues[opts.Name] = q
	return q, nil
}

func (qm *QueueManager) Delete(_ context.Context, name string) error {
	delete(qm.queues, name)
	return nil
}
