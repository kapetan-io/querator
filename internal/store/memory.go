package store

import (
	"bytes"
	"context"
	"errors"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/set"
	"github.com/segmentio/ksuid"
	"strings"
)

type MemoryBackendConfig struct {
	Clock *clock.Provider
}

type MemoryBackend struct {
	conf MemoryBackendConfig
}

var _ Backend = &MemoryBackend{}

func NewMemoryBackend(conf MemoryBackendConfig) *MemoryBackend {
	set.Default(&conf.Clock, clock.NewProvider())
	return &MemoryBackend{
		conf: conf,
	}
}

func (s *MemoryBackend) GetQueueStore() (QueueStore, error) {
	return &MemoryQueuesStore{
		mem:    make([]types.QueueInfo, 0, 1_000),
		parent: s,
	}, nil
}

func (s *MemoryBackend) GetPartition(info types.PartitionInfo) (Partition, error) {
	return &MemoryQueue{
		mem:    make([]types.Item, 0, 1_000),
		uid:    ksuid.New(),
		info:   info.Queue,
		parent: s,
	}, nil
}

func (s *MemoryBackend) ParseID(parse types.ItemID, id *StorageID) error {
	parts := bytes.Split(parse, []byte("~"))
	if len(parts) != 2 {
		return errors.New("expected format <queue_name>~<storage_id>")
	}
	id.Queue = string(parts[0])
	id.ID = parts[1]
	return nil
}

func (s *MemoryBackend) BuildStorageID(queue string, id []byte) types.ItemID {
	return append([]byte(queue+"~"), id...)
}

func (s *MemoryBackend) Close(_ context.Context) error {
	return nil
}

type MemoryQueue struct {
	info   types.QueueInfo
	parent *MemoryBackend
	mem    []types.Item
	uid    ksuid.KSUID
}

func (q *MemoryQueue) Produce(_ context.Context, batch types.Batch[types.ProduceRequest]) error {
	for _, r := range batch.Requests {
		for _, item := range r.Items {
			q.uid = q.uid.Next()
			item.ID = []byte(q.uid.String())
			item.CreatedAt = q.parent.conf.Clock.Now().UTC()

			q.mem = append(q.mem, *item)
			item.ID = q.parent.BuildStorageID(q.info.Name, item.ID)
		}
	}
	return nil
}

func (q *MemoryQueue) Reserve(_ context.Context, batch types.ReserveBatch, opts ReserveOptions) error {
	batchIter := batch.Iterator()
	var count int

	for i, item := range q.mem {
		if item.IsReserved {
			continue
		}

		item.ReserveDeadline = opts.ReserveDeadline
		item.IsReserved = true
		count++

		// Item returned gets the public StorageID
		itemPtr := new(types.Item) // TODO: Memory Pool
		*itemPtr = item
		itemPtr.ID = q.parent.BuildStorageID(q.info.Name, item.ID)

		// Assign the item to the next waiting reservation in the batch,
		// returns false if there are no more reservations available to fill
		if batchIter.Next(itemPtr) {
			// If assignment was a success, put the updated item into the array
			q.mem[i] = item
			continue
		}
		break
	}
	return nil
}

func (q *MemoryQueue) Complete(_ context.Context, batch types.Batch[types.CompleteRequest]) error {
nextBatch:
	for i := range batch.Requests {
		for _, id := range batch.Requests[i].Ids {
			var sid StorageID
			if err := q.parent.ParseID(id, &sid); err != nil {
				batch.Requests[i].Err = transport.NewInvalidOption("invalid storage id; '%s': %s", id, err)
				continue nextBatch
			}

			idx, ok := q.findID(sid.ID)
			if !ok {
				batch.Requests[i].Err = transport.NewInvalidOption("invalid storage id; '%s' does not exist", id)
				continue nextBatch
			}

			if !q.mem[idx].IsReserved {
				batch.Requests[i].Err = transport.NewConflict("item(s) cannot be completed; '%s' is not "+
					"marked as reserved", sid.ID)
				continue nextBatch
			}
			// Remove the item from the array
			q.mem = append(q.mem[:idx], q.mem[idx+1:]...)
		}
	}
	return nil
}

func (q *MemoryQueue) List(_ context.Context, items *[]*types.Item, opts types.ListOptions) error {

	var sid StorageID
	if opts.Pivot != nil {
		if err := q.parent.ParseID(opts.Pivot, &sid); err != nil {
			return transport.NewInvalidOption("invalid storage id; '%s': %s", opts.Pivot, err)
		}
	}

	var count, idx int

	if sid.ID != nil {
		idx, _ = q.findID(sid.ID)
	}
	for _, item := range q.mem[idx:] {
		if count >= opts.Limit {
			return nil
		}
		item.ID = q.parent.BuildStorageID(q.info.Name, item.ID)
		*items = append(*items, &item)
		count++
	}
	return nil
}

func (q *MemoryQueue) Add(_ context.Context, items []*types.Item) error {
	for _, item := range items {
		q.uid = q.uid.Next()
		item.ID = []byte(q.uid.String())
		item.CreatedAt = q.parent.conf.Clock.Now().UTC()

		q.mem = append(q.mem, *item)
		item.ID = q.parent.BuildStorageID(q.info.Name, item.ID)
	}
	return nil
}

func (q *MemoryQueue) Delete(_ context.Context, ids []types.ItemID) error {
	for _, id := range ids {
		var sid StorageID
		if err := q.parent.ParseID(id, &sid); err != nil {
			return transport.NewInvalidOption("invalid storage id; '%s': %s", id, err)
		}

		idx, ok := q.findID(sid.ID)
		if !ok {
			continue
		}

		// Remove the item from the array
		q.mem = append(q.mem[:idx], q.mem[idx+1:]...)
	}
	return nil
}

func (q *MemoryQueue) Clear(_ context.Context, destructive bool) error {
	if destructive {
		q.mem = make([]types.Item, 0, 1_000)
		return nil
	}

	mem := make([]types.Item, 0, len(q.mem))
	for _, item := range q.mem {
		if item.IsReserved {
			mem = append(mem, item)
			continue
		}
	}
	q.mem = mem
	return nil
}

func (q *MemoryQueue) Stats(_ context.Context, stats *types.QueueStats) error {
	now := q.parent.conf.Clock.Now().UTC()
	for _, item := range q.mem {
		stats.Total++
		stats.AverageAge += now.Sub(item.CreatedAt)
		if item.IsReserved {
			stats.AverageReservedAge += item.ReserveDeadline.Sub(now)
			stats.TotalReserved++
		}
	}
	if stats.Total != 0 {
		stats.AverageAge = clock.Duration(int64(stats.AverageAge) / int64(stats.Total))
	}
	if stats.TotalReserved != 0 {
		stats.AverageReservedAge = clock.Duration(int64(stats.AverageReservedAge) / int64(stats.TotalReserved))
	}
	return nil
}

func (q *MemoryQueue) Close(_ context.Context) error {
	q.mem = nil
	return nil
}

// findID attempts to find the provided id in q.mem. If found returns the index and true.
// If not found returns the next nearest item in the list.
func (q *MemoryQueue) findID(id []byte) (int, bool) {
	var nearest, nearestIdx int
	for i, item := range q.mem {
		lex := bytes.Compare(item.ID, id)
		if lex == 0 {
			return i, true
		}
		if lex > nearest {
			nearestIdx = i
			nearest = lex
		}
	}
	return nearestIdx, false
}

// ---------------------------------------------
// Partition Repository Implementation
// ---------------------------------------------

type MemoryQueuesStore struct {
	QueuesValidation
	mem    []types.QueueInfo
	parent *MemoryBackend
}

var _ QueueStore = &MemoryQueuesStore{}

func (s *MemoryQueuesStore) Get(_ context.Context, name string, queue *types.QueueInfo) error {
	if err := s.validateGet(name); err != nil {
		return err
	}

	idx, ok := s.findQueue(name)
	if !ok {
		return ErrQueueNotExist
	}
	*queue = s.mem[idx]
	return nil
}

func (s *MemoryQueuesStore) Add(_ context.Context, info types.QueueInfo) error {
	if err := s.validateAdd(info); err != nil {
		return err
	}

	_, ok := s.findQueue(info.Name)
	if ok {
		return transport.NewInvalidOption("invalid queue; '%s' already exists", info.Name)
	}

	s.mem = append(s.mem, info)
	return nil
}

func (s *MemoryQueuesStore) Update(_ context.Context, info types.QueueInfo) error {
	if err := s.validateUpdate(info); err != nil {
		return err
	}

	idx, ok := s.findQueue(info.Name)
	if !ok {
		return ErrQueueNotExist
	}

	if info.DeadTimeout.Nanoseconds() != 0 {
		if info.ReserveTimeout > info.DeadTimeout {
			return transport.NewInvalidOption("reserve timeout is too long; %s cannot be greater than the "+
				"dead timeout %s", info.ReserveTimeout.String(), info.DeadTimeout.String())
		}
	}

	if info.ReserveTimeout > s.mem[idx].DeadTimeout {
		return transport.NewInvalidOption("reserve timeout is too long; %s cannot be greater than the "+
			"dead timeout %s", info.ReserveTimeout.String(), s.mem[idx].DeadTimeout.String())
	}

	s.mem[idx].Update(info)
	return nil

}

func (s *MemoryQueuesStore) List(_ context.Context, queues *[]types.QueueInfo, opts types.ListOptions) error {
	if err := s.validateList(opts); err != nil {
		return err
	}

	var count, idx int
	if opts.Pivot != nil {
		idx, _ = s.findQueue(string(opts.Pivot))
	}

	for _, info := range s.mem[idx:] {
		if count >= opts.Limit {
			return nil
		}
		*queues = append(*queues, info)
		count++
	}
	return nil
}

func (s *MemoryQueuesStore) Delete(_ context.Context, name string) error {
	if err := s.validateDelete(name); err != nil {
		return err
	}

	idx, ok := s.findQueue(name)
	if !ok {
		return nil
	}
	s.mem = append(s.mem[:idx], s.mem[idx+1:]...)
	return nil
}

func (s *MemoryQueuesStore) Close(_ context.Context) error {
	s.mem = nil
	return nil
}

// findID attempts to find the provided queue in q.mem. If found returns the index and true.
// If not found returns the next nearest item in the list.
func (s *MemoryQueuesStore) findQueue(name string) (int, bool) {
	var nearest, nearestIdx int
	for i, queue := range s.mem {
		lex := strings.Compare(queue.Name, name)
		if lex == 0 {
			return i, true
		}
		if lex > nearest {
			nearestIdx = i
			nearest = lex
		}
	}
	return nearestIdx, false
}
