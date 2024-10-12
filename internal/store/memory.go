package store

import (
	"bytes"
	"context"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/set"
	"github.com/segmentio/ksuid"
	"iter"
	"log/slog"
	"strings"
	"sync"
)

// ---------------------------------------------
// Partition Implementation
// ---------------------------------------------

type MemoryPartition struct {
	info types.PartitionInfo
	conf StorageConfig
	mem  []types.Item
	mu   sync.RWMutex
	log  *slog.Logger
	uid  ksuid.KSUID
}

func (m *MemoryPartition) Produce(_ context.Context, batch types.Batch[types.ProduceRequest]) error {
	defer m.mu.Unlock()
	m.mu.Lock()

	for _, r := range batch.Requests {
		for _, item := range r.Items {
			m.uid = m.uid.Next()
			item.ID = []byte(m.uid.String())
			item.CreatedAt = m.conf.Clock.Now().UTC()

			m.mem = append(m.mem, *item)
		}
	}
	return nil
}

func (m *MemoryPartition) Reserve(_ context.Context, batch types.ReserveBatch, opts ReserveOptions) error {
	defer m.mu.Unlock()
	m.mu.Lock()

	batchIter := batch.Iterator()
	var count int

	for i, item := range m.mem {
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
			m.mem[i] = item
			continue
		}
		break
	}
	return nil
}

func (m *MemoryPartition) Complete(_ context.Context, batch types.Batch[types.CompleteRequest]) error {
	defer m.mu.Unlock()
	m.mu.Lock()

nextBatch:
	for i := range batch.Requests {
		for _, id := range batch.Requests[i].Ids {
			if err := m.validateID(id); err != nil {
				batch.Requests[i].Err = transport.NewInvalidOption("invalid storage id; '%s': %s", id, err)
				continue nextBatch
			}

			idx, ok := m.findID(id)
			if !ok {
				batch.Requests[i].Err = transport.NewInvalidOption("invalid storage id; '%s' does not exist", id)
				continue nextBatch
			}

			if !m.mem[idx].IsReserved {
				batch.Requests[i].Err = transport.NewConflict("item(s) cannot be completed; '%s' is not "+
					"marked as reserved", id)
				continue nextBatch
			}
			// Remove the item from the array
			m.mem = append(m.mem[:idx], m.mem[idx+1:]...)
		}
	}
	return nil
}

func (m *MemoryPartition) validateID(id []byte) error {
	_, err := ksuid.Parse(string(id))
	if err != nil {
		return err
	}
	return nil
}

func (m *MemoryPartition) List(_ context.Context, items *[]*types.Item, opts types.ListOptions) error {
	defer m.mu.RUnlock()
	var count, idx int
	m.mu.RLock()

	if opts.Pivot != nil {
		if err := m.validateID(opts.Pivot); err != nil {
			return transport.NewInvalidOption("invalid storage id; '%s': %s", opts.Pivot, err)
		}
		idx, _ = m.findID(opts.Pivot)
	}
	for _, item := range m.mem[idx:] {
		if count >= opts.Limit {
			return nil
		}
		*items = append(*items, &item)
		count++
	}
	return nil
}

func (m *MemoryPartition) Add(_ context.Context, items []*types.Item) error {
	defer m.mu.Unlock()
	m.mu.Lock()

	if len(items) == 0 {
		return transport.NewInvalidOption("items is invalid; cannot be empty")
	}

	for _, item := range items {
		m.uid = m.uid.Next()
		item.ID = []byte(m.uid.String())
		item.CreatedAt = m.conf.Clock.Now().UTC()

		m.mem = append(m.mem, *item)
	}
	return nil
}

func (m *MemoryPartition) Delete(_ context.Context, ids []types.ItemID) error {
	defer m.mu.Unlock()
	m.mu.Lock()

	for _, id := range ids {
		if err := m.validateID(id); err != nil {
			return transport.NewInvalidOption("invalid storage id; '%s': %s", id, err)
		}

		idx, ok := m.findID(id)
		if !ok {
			continue
		}

		// Remove the item from the array
		m.mem = append(m.mem[:idx], m.mem[idx+1:]...)
	}
	return nil
}

func (m *MemoryPartition) Clear(_ context.Context, destructive bool) error {
	defer m.mu.Unlock()
	m.mu.Lock()

	if destructive {
		m.mem = make([]types.Item, 0, 1_000)
		return nil
	}

	mem := make([]types.Item, 0, len(m.mem))
	for _, item := range m.mem {
		if item.IsReserved {
			mem = append(mem, item)
			continue
		}
	}
	m.mem = mem
	return nil
}

func (m *MemoryPartition) Info() types.PartitionInfo {
	defer m.mu.RUnlock()
	m.mu.RLock()

	return m.info
}

func (m *MemoryPartition) UpdateQueueInfo(info types.QueueInfo) {
	defer m.mu.Unlock()
	m.mu.Lock()
	m.info.Queue = info
}

func (m *MemoryPartition) ReadActions(_ clock.Duration, now clock.Time) iter.Seq[types.Action] {
	defer m.mu.RUnlock()
	m.mu.RLock()

	return func(yield func(types.Action) bool) {
		for _, item := range m.mem {
			// Is the reserved item expired?
			if item.IsReserved {
				if now.After(item.ReserveDeadline) {
					yield(types.Action{
						Action:       types.ActionReserveExpired,
						PartitionNum: m.info.PartitionNum,
						Queue:        m.info.Queue.Name,
						Item:         item,
					})
					continue
				}
			}
			// Is the item expired?
			if now.After(item.ExpireDeadline) {
				yield(types.Action{
					Action:       types.ActionItemExpired,
					PartitionNum: m.info.PartitionNum,
					Queue:        m.info.Queue.Name,
					Item:         item,
				})
				continue
			}
			if item.Attempts >= m.info.Queue.MaxAttempts {
				yield(types.Action{
					Action:       types.ActionItemMaxAttempts,
					PartitionNum: m.info.PartitionNum,
					Queue:        m.info.Queue.Name,
					Item:         item,
				})
				continue
			}
		}
	}
}

func (m *MemoryPartition) WriteActions(ctx context.Context, batch types.Batch[types.LifeCycleRequest]) error {
	defer m.mu.RUnlock()
	m.mu.RLock()

	for _, req := range batch.Requests {
		for _, a := range req.Actions {
			switch a.Action {
			case types.ActionQueueScheduledItem:
				// TODO: Find the scheduled item and add it to the partition queue
			case types.ActionReserveExpired:
				idx, ok := m.findID(a.Item.ID)
				if !ok {
					m.log.Warn("unable to find item while processing action; ignoring action",
						"id", a.Item.ID, "action", types.ActionToString(a.Action))
				}
				// TODO: Re-add this item to m.mem and give it a new id <---- DO THIS NEXT
				m.mem[idx].ReserveDeadline = clock.Time{}
				m.mem[idx].IsReserved = false
			case types.ActionDeleteItem:
				// TODO: Find the item and remove it
			default:
				m.log.Warn("undefined action", "action", types.ActionToString(a.Action))
			}
		}
	}
	return nil // TODO(lifecycle):
}

func (m *MemoryPartition) LifeCycleInfo(ctx context.Context, info *types.LifeCycleInfo) error {
	defer m.mu.RUnlock()
	m.mu.RLock()
	next := theFuture

	// Find the item that will expire next
	for _, item := range m.mem {
		if item.ReserveDeadline.Before(next) {
			next = item.ReserveDeadline
		}
	}

	if next != theFuture {
		info.NextReserveExpiry = next
	}

	m.log.LogAttrs(ctx, LevelDebugAll, "life cycle status",
		slog.String("next-reserve-expire", info.NextReserveExpiry.String()))
	return nil
}

func (m *MemoryPartition) Stats(_ context.Context, stats *types.PartitionStats) error {
	defer m.mu.RUnlock()
	m.mu.RLock()

	now := m.conf.Clock.Now().UTC()
	for _, item := range m.mem {
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

func (m *MemoryPartition) Close(_ context.Context) error {
	defer m.mu.Unlock()
	m.mu.Lock()

	m.mem = nil
	return nil
}

// findID attempts to find the provided id in q.mem. If found returns the index and true.
// If not found returns the next nearest item in the list.
func (m *MemoryPartition) findID(id []byte) (int, bool) {
	var nearest, nearestIdx int
	for i, item := range m.mem {
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
// Queues Implementation
// ---------------------------------------------

type MemoryQueues struct {
	QueuesValidation
	mem []types.QueueInfo
	log *slog.Logger
}

var _ Queues = &MemoryQueues{}

func NewMemoryQueues(log *slog.Logger) *MemoryQueues {
	set.Default(&log, slog.Default())
	return &MemoryQueues{
		mem: make([]types.QueueInfo, 0, 1_000),
		log: log,
	}
}

func (s *MemoryQueues) Get(_ context.Context, name string, queue *types.QueueInfo) error {
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

func (s *MemoryQueues) Add(_ context.Context, info types.QueueInfo) error {
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

func (s *MemoryQueues) Update(_ context.Context, info types.QueueInfo) error {
	if err := s.validateQueueName(info); err != nil {
		return err
	}

	idx, ok := s.findQueue(info.Name)
	if !ok {
		return ErrQueueNotExist
	}

	if info.ExpireTimeout.Nanoseconds() != 0 {
		if info.ReserveTimeout > info.ExpireTimeout {
			return transport.NewInvalidOption("reserve timeout is too long; %s cannot be greater than the "+
				"expire timeout %s", info.ReserveTimeout.String(), info.ExpireTimeout.String())
		}
	}

	if info.ReserveTimeout > s.mem[idx].ExpireTimeout {
		return transport.NewInvalidOption("reserve timeout is too long; %s cannot be greater than the "+
			"expire timeout %s", info.ReserveTimeout.String(), s.mem[idx].ExpireTimeout.String())
	}

	found := s.mem[idx]
	found.Update(info)

	if err := s.validateUpdate(found); err != nil {
		return err
	}
	s.mem[idx] = found
	return nil

}

func (s *MemoryQueues) List(_ context.Context, queues *[]types.QueueInfo, opts types.ListOptions) error {
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

func (s *MemoryQueues) Delete(_ context.Context, name string) error {
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

func (s *MemoryQueues) Close(_ context.Context) error {
	s.mem = nil
	return nil
}

// findID attempts to find the provided queue in q.mem. If found returns the index and true.
// If not found returns the next nearest item in the list.
func (s *MemoryQueues) findQueue(name string) (int, bool) {
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
	partitions map[types.PartitionHash]*MemoryPartition
	conf       StorageConfig
	log        *slog.Logger
}

var _ PartitionStore = &MemoryPartitionStore{}

func NewMemoryPartitionStore(conf StorageConfig, log *slog.Logger) *MemoryPartitionStore {
	set.Default(&log, slog.Default())

	return &MemoryPartitionStore{
		partitions: make(map[types.PartitionHash]*MemoryPartition),
		log:        log.With("store", "memory"),
		conf:       conf,
	}
}

func (m MemoryPartitionStore) Get(info types.PartitionInfo) Partition {
	p, ok := m.partitions[info.HashKey()]
	if !ok {
		m.partitions[info.HashKey()] = &MemoryPartition{
			log:  m.log.With("queue", info.Queue.Name, "partition", info.PartitionNum),
			mem:  make([]types.Item, 0, 1_000),
			uid:  ksuid.New(),
			conf: m.conf,
			info: info,
		}
		return m.partitions[info.HashKey()]
	}
	return p
}
