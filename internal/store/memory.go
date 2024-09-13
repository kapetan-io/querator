package store

import (
	"bytes"
	"context"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/tackle/clock"
	"github.com/segmentio/ksuid"
	"strings"
)

// ---------------------------------------------
// Partition Implementation
// ---------------------------------------------

type MemoryPartition struct {
	info types.PartitionInfo
	conf StorageConfig
	mem  []types.Item
	uid  ksuid.KSUID
}

func (q *MemoryPartition) Produce(_ context.Context, batch types.Batch[types.ProduceRequest]) error {
	for _, r := range batch.Requests {
		for _, item := range r.Items {
			q.uid = q.uid.Next()
			item.ID = []byte(q.uid.String())
			item.CreatedAt = q.conf.Clock.Now().UTC()

			q.mem = append(q.mem, *item)
		}
	}
	return nil
}

func (q *MemoryPartition) Reserve(_ context.Context, batch types.ReserveBatch, opts ReserveOptions) error {
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

func (q *MemoryPartition) Complete(_ context.Context, batch types.Batch[types.CompleteRequest]) error {
nextBatch:
	for i := range batch.Requests {
		for _, id := range batch.Requests[i].Ids {
			if err := q.validateID(id); err != nil {
				batch.Requests[i].Err = transport.NewInvalidOption("invalid storage id; '%s': %s", id, err)
				continue nextBatch
			}

			idx, ok := q.findID(id)
			if !ok {
				batch.Requests[i].Err = transport.NewInvalidOption("invalid storage id; '%s' does not exist", id)
				continue nextBatch
			}

			if !q.mem[idx].IsReserved {
				batch.Requests[i].Err = transport.NewConflict("item(s) cannot be completed; '%s' is not "+
					"marked as reserved", id)
				continue nextBatch
			}
			// Remove the item from the array
			q.mem = append(q.mem[:idx], q.mem[idx+1:]...)
		}
	}
	return nil
}

func (q *MemoryPartition) validateID(id []byte) error {
	_, err := ksuid.Parse(string(id))
	if err != nil {
		return err
	}
	return nil
}

func (q *MemoryPartition) List(_ context.Context, items *[]*types.Item, opts types.ListOptions) error {
	var count, idx int

	if opts.Pivot != nil {
		if err := q.validateID(opts.Pivot); err != nil {
			return transport.NewInvalidOption("invalid storage id; '%s': %s", opts.Pivot, err)
		}
		idx, _ = q.findID(opts.Pivot)
	}
	for _, item := range q.mem[idx:] {
		if count >= opts.Limit {
			return nil
		}
		*items = append(*items, &item)
		count++
	}
	return nil
}

func (q *MemoryPartition) Add(_ context.Context, items []*types.Item) error {
	if len(items) == 0 {
		return transport.NewInvalidOption("items is invalid; cannot be empty")
	}

	for _, item := range items {
		q.uid = q.uid.Next()
		item.ID = []byte(q.uid.String())
		item.CreatedAt = q.conf.Clock.Now().UTC()

		q.mem = append(q.mem, *item)
	}
	return nil
}

func (q *MemoryPartition) Delete(_ context.Context, ids []types.ItemID) error {
	for _, id := range ids {
		if err := q.validateID(id); err != nil {
			return transport.NewInvalidOption("invalid storage id; '%s': %s", id, err)
		}

		idx, ok := q.findID(id)
		if !ok {
			continue
		}

		// Remove the item from the array
		q.mem = append(q.mem[:idx], q.mem[idx+1:]...)
	}
	return nil
}

func (q *MemoryPartition) Clear(_ context.Context, destructive bool) error {
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

func (q *MemoryPartition) Info() types.PartitionInfo {
	return q.info
}

func (q *MemoryPartition) Stats(_ context.Context, stats *types.PartitionStats) error {
	now := q.conf.Clock.Now().UTC()
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

func (q *MemoryPartition) Close(_ context.Context) error {
	q.mem = nil
	return nil
}

// findID attempts to find the provided id in q.mem. If found returns the index and true.
// If not found returns the next nearest item in the list.
func (q *MemoryPartition) findID(id []byte) (int, bool) {
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
// QueueStore Implementation
// ---------------------------------------------

type MemoryQueueStore struct {
	QueuesValidation
	mem []types.QueueInfo
}

var _ QueueStore = &MemoryQueueStore{}

func NewMemoryQueueStore() *MemoryQueueStore {
	return &MemoryQueueStore{
		mem: make([]types.QueueInfo, 0, 1_000),
	}
}

func (s *MemoryQueueStore) Get(_ context.Context, name string, queue *types.QueueInfo) error {
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

func (s *MemoryQueueStore) Add(_ context.Context, info types.QueueInfo) error {
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

func (s *MemoryQueueStore) Update(_ context.Context, info types.QueueInfo) error {
	if err := s.validateQueueName(info); err != nil {
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

	found := s.mem[idx]
	found.Update(info)

	if err := s.validateUpdate(found); err != nil {
		return err
	}
	s.mem[idx] = found
	return nil

}

func (s *MemoryQueueStore) List(_ context.Context, queues *[]types.QueueInfo, opts types.ListOptions) error {
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

func (s *MemoryQueueStore) Delete(_ context.Context, name string) error {
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

func (s *MemoryQueueStore) Close(_ context.Context) error {
	s.mem = nil
	return nil
}

// findID attempts to find the provided queue in q.mem. If found returns the index and true.
// If not found returns the next nearest item in the list.
func (s *MemoryQueueStore) findQueue(name string) (int, bool) {
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

// ---------------------------------------------
// PartitionStore Implementation
// ---------------------------------------------

type MemoryPartitionStore struct {
	conf StorageConfig
}

var _ PartitionStore = &MemoryPartitionStore{}

func NewMemoryPartitionStore(conf StorageConfig) *MemoryPartitionStore {
	return &MemoryPartitionStore{conf: conf}
}

func (m MemoryPartitionStore) Create(info types.PartitionInfo) error {
	// Does nothing as memory has nothing to create. Calls to Get() create the partition
	// when requested.
	return nil
}

func (m MemoryPartitionStore) Get(info types.PartitionInfo) Partition {
	return &MemoryPartition{
		mem:  make([]types.Item, 0, 1_000),
		uid:  ksuid.New(),
		conf: m.conf,
		info: info,
	}

}
