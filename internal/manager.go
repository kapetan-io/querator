package internal

import (
	"context"
	"github.com/kapetan-io/querator/transport"
)

type QueueManager struct {
	queues map[string]*Queue
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
