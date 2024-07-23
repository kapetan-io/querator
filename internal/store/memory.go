package store

import (
	"context"
	"errors"
	"fmt"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport"
	"github.com/segmentio/ksuid"
	"strings"
	"time"
)

type MemoryStorage struct{}

var _ Storage = &MemoryStorage{}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{}
}

func (s *MemoryStorage) NewQueuesStore(opts QueuesStoreOptions) (QueuesStore, error) {
	return &MemoryQueueStore{mem: make(map[string]*types.QueueInfo)}, nil
}

func (s *MemoryStorage) NewQueue(info types.QueueInfo) (Queue, error) {
	return &MemoryQueue{info: info, parent: s, mem: make([]types.Item, 0, 1_000)}, nil
}

func (s *MemoryStorage) ParseID(parse string, id *StorageID) error {
	parts := strings.Split(parse, "~")
	if len(parts) != 2 {
		return errors.New("expected format <queue_name>~<storage_id>")
	}
	id.Queue = parts[0]
	id.ID = []byte(parts[1])
	return nil
}

func (s *MemoryStorage) BuildStorageID(queue, id string) string {
	return fmt.Sprintf("%s~%s", queue, id)
}

func (s *MemoryStorage) Close(_ context.Context) error {
	return nil
}

type MemoryQueue struct {
	info   types.QueueInfo
	parent *MemoryStorage
	mem    []types.Item
	uid    ksuid.KSUID
}

func (q *MemoryQueue) Produce(_ context.Context, batch types.Batch[types.ProduceRequest]) error {
	for _, r := range batch.Requests {
		for _, item := range r.Items {
			q.uid = q.uid.Next()
			item.ID = q.uid.String()
			item.CreatedAt = time.Now().UTC()

			q.mem = append(q.mem, *item)
			item.ID = q.parent.BuildStorageID(q.info.Name, item.ID)
		}
	}
	return nil
}

func (q *MemoryQueue) Reserve(ctx context.Context, batch types.ReserveBatch, opts ReserveOptions) error {
	//TODO implement me
	panic("implement me")
}

func (q *MemoryQueue) Complete(ctx context.Context, batch types.Batch[types.CompleteRequest]) error {
	//TODO implement me
	panic("implement me")
}

func (q *MemoryQueue) List(ctx context.Context, items *[]*types.Item, opts types.ListOptions) error {
	//TODO implement me
	panic("implement me")
}

func (q *MemoryQueue) Add(ctx context.Context, items []*types.Item) error {
	//TODO implement me
	panic("implement me")
}

func (q *MemoryQueue) Delete(ctx context.Context, ids []string) error {
	//TODO implement me
	panic("implement me")
}

func (q *MemoryQueue) Clear(ctx context.Context, d bool) error {
	//TODO implement me
	panic("implement me")
}

func (q *MemoryQueue) Stats(ctx context.Context, stats *types.QueueStats) error {
	//TODO implement me
	panic("implement me")
}

func (q *MemoryQueue) Close(ctx context.Context) error {
	q.mem = nil
	return nil
}

// ---------------------------------------------
// Queue Repository Implementation
// ---------------------------------------------

type MemoryQueueStore struct {
	mem map[string]*types.QueueInfo
}

var _ QueuesStore = &MemoryQueueStore{}

func (s *MemoryQueueStore) Get(_ context.Context, name string, queue *types.QueueInfo) error {

	if strings.TrimSpace(name) == "" {
		return ErrEmptyQueueName
	}

	o, ok := s.mem[name]
	if !ok {
		return ErrQueueNotExist
	}
	*queue = *o
	return nil
}

func (s *MemoryQueueStore) Set(_ context.Context, info types.QueueInfo) error {

	if strings.TrimSpace(info.Name) == "" {
		return ErrEmptyQueueName
	}

	if info.ReserveTimeout > info.DeadTimeout {
		return transport.NewInvalidOption("reserve_timeout is too long; %s cannot be greater than the "+
			"dead_timeout %s", info.ReserveTimeout.String(), info.DeadTimeout.String())
	}

	s.mem[info.Name] = &info
	return nil
}

func (s *MemoryQueueStore) List(_ context.Context, queues *[]types.QueueInfo, opts types.ListOptions) error {
	//TODO implement me
	panic("implement me")
}

func (s *MemoryQueueStore) Delete(_ context.Context, name string) error {
	delete(s.mem, name)
	return nil
}

func (s *MemoryQueueStore) Close(_ context.Context) error {
	s.mem = nil
	return nil
}
