package store

import (
	"context"
	"errors"
	"fmt"
	"github.com/kapetan-io/querator/internal/types"
	"strings"
)

type MockOptions struct {
	Methods map[string]func(args []any) error
}

type MockStorage struct {
	opts *MockOptions
}

var _ Storage = &MockStorage{}

func NewMockStorage(opts *MockOptions) *MockStorage {
	return &MockStorage{opts: opts}
}

func (m *MockStorage) NewQueuesStore(opts QueuesStoreOptions) (QueuesStore, error) {
	return &MockQueueStore{}, nil
}

func (m *MockStorage) NewQueue(opts QueueInfo) (Queue, error) {
	return &MockQueue{opts: opts, parent: m}, nil
}

func (m *MockStorage) ParseID(parse string, id *StorageID) error {
	parts := strings.Split(parse, "~")
	if len(parts) != 2 {
		return errors.New("expected format <queue_name>~<storage_id>")
	}
	id.Queue = parts[0]
	id.ID = []byte(parts[1])
	return nil
}

func (m *MockStorage) BuildStorageID(queue, id string) string {
	return fmt.Sprintf("%s~%s", queue, id)
}

func (b *MockStorage) Close(_ context.Context) error {
	return nil
}

type MockQueue struct {
	opts   QueueInfo
	parent *MockStorage
}

func (m *MockQueue) Produce(ctx context.Context, batch types.Batch[types.ProduceRequest]) error {
	f, ok := m.parent.opts.Methods["Queue.Produce"]
	if !ok {
		panic("no mock for method \"Queue.Produce\" defined")
	}
	return f([]any{ctx, batch})
}

func (m *MockQueue) Reserve(ctx context.Context, batch types.ReserveBatch, opts ReserveOptions) error {
	//TODO implement me
	panic("implement me")
}

func (m *MockQueue) Complete(ctx context.Context, batch types.Batch[types.CompleteRequest]) error {
	//TODO implement me
	panic("implement me")
}

func (m *MockQueue) List(ctx context.Context, items *[]*types.Item, opts types.ListOptions) error {
	//TODO implement me
	panic("implement me")
}

func (m *MockQueue) Add(ctx context.Context, items []*types.Item) error {
	//TODO implement me
	panic("implement me")
}

func (m *MockQueue) Delete(ctx context.Context, ids []string) error {
	//TODO implement me
	panic("implement me")
}

func (m *MockQueue) Clear(ctx context.Context, d bool) error {
	//TODO implement me
	panic("implement me")
}

func (m *MockQueue) Stats(ctx context.Context, stats *types.QueueStats) error {
	//TODO implement me
	panic("implement me")
}

func (m *MockQueue) Close(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

// ---------------------------------------------
// Queue Repository Implementation
// ---------------------------------------------

type MockQueueStore struct{}

var _ QueuesStore = &MockQueueStore{}

func (r MockQueueStore) Get(ctx context.Context, name string, queue *QueueInfo) error {
	//TODO implement me
	panic("implement me")
}

func (r MockQueueStore) Set(ctx context.Context, opts QueueInfo) error {
	//TODO implement me
	panic("implement me")
}

func (r MockQueueStore) List(ctx context.Context, queues *[]*QueueInfo, opts types.ListOptions) error {
	//TODO implement me
	panic("implement me")
}

func (r MockQueueStore) Delete(ctx context.Context, queueName string) error {
	//TODO implement me
	panic("implement me")
}

func (r MockQueueStore) Close(_ context.Context) error {
	return nil
}
