package store

import (
	"bytes"
	"context"
	"errors"
	"github.com/kapetan-io/querator/internal/types"
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
	return &MockQueuesStore{}, nil
}

func (m *MockStorage) NewQueue(opts types.QueueInfo) (Queue, error) {
	return &MockQueue{opts: opts, parent: m}, nil
}

func (m *MockStorage) ParseID(parse types.ItemID, id *StorageID) error {
	parts := bytes.Split(parse, []byte("~"))
	if len(parts) != 2 {
		return errors.New("expected format <queue_name>~<storage_id>")
	}
	id.Queue = string(parts[0])
	id.ID = parts[1]
	return nil
}

func (m *MockStorage) BuildStorageID(queue string, id []byte) types.ItemID {
	return append([]byte(queue+"~"), id...)
}

func (b *MockStorage) Close(_ context.Context) error {
	return nil
}

type MockQueue struct {
	opts   types.QueueInfo
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

func (m *MockQueue) Delete(ctx context.Context, ids []types.ItemID) error {
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

type MockQueuesStore struct{}

var _ QueuesStore = &MockQueuesStore{}

func (r MockQueuesStore) Get(ctx context.Context, name string, queue *types.QueueInfo) error {
	//TODO implement me
	panic("implement me")
}

func (r MockQueuesStore) Add(ctx context.Context, opts types.QueueInfo) error {
	//TODO implement me
	panic("implement me")
}

func (r MockQueuesStore) Update(ctx context.Context, opts types.QueueInfo) error {
	//TODO implement me
	panic("implement me")
}

func (r MockQueuesStore) List(ctx context.Context, queues *[]types.QueueInfo, opts types.ListOptions) error {
	//TODO implement me
	panic("implement me")
}

func (r MockQueuesStore) Delete(ctx context.Context, queueName string) error {
	//TODO implement me
	panic("implement me")
}

func (r MockQueuesStore) Close(_ context.Context) error {
	return nil
}
