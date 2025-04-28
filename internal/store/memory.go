package store

import (
	"bytes"
	"context"
	"fmt"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/set"
	"github.com/segmentio/ksuid"
	"iter"
	"log/slog"
	"strings"
	"sync"
	"time"
)

var bucketName = []byte("partition")

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

func (m *MemoryPartition) Produce(_ context.Context, batch types.ProduceBatch, now clock.Time) error {
	defer m.mu.Unlock()
	m.mu.Lock()

	for _, r := range batch.Requests {
		for _, item := range r.Items {
			m.uid = m.uid.Next()
			item.ID = []byte(m.uid.String())
			item.CreatedAt = now

			// If the EnqueueAt is less than 100 milliseconds from now, we want to enqueue
			// the item immediately. This avoids unnecessary work for something that will be moved into the
			// queue less than a few milliseconds from now. Programmers may want to be "complete", so they may
			// include an EnqueueAt time when producing items, which is set to now or near to now, due to clock
			// drift between systems. Because of this, Querator may end up doing more work and be less efficient
			// simply because of programmers who desire to completely fill out the pb.QueueProduceItem{} struct
			// with every possible field, regardless of whether the field is needed.

			// If EnqueueAt dates are in the past, enqueue the item instead of adding it as a scheduled item.
			if item.EnqueueAt.Before(now.Add(time.Millisecond * 100)) {
				item.EnqueueAt = time.Time{}
			}

			m.mem = append(m.mem, *item)
		}
	}
	return nil
}

func (m *MemoryPartition) Lease(_ context.Context, batch types.LeaseBatch, opts LeaseOptions) error {
	defer m.mu.Unlock()
	m.mu.Lock()

	batchIter := batch.Iterator()
	var count int

	for i, item := range m.mem {
		// If the item is leased, or is a scheduled item
		if item.IsLeased || !item.EnqueueAt.IsZero() {
			continue
		}

		item.LeaseDeadline = opts.LeaseDeadline
		item.IsLeased = true
		item.Attempts++
		count++

		// Item returned gets the public StorageID
		itemPtr := new(types.Item) // TODO: Memory Pool
		*itemPtr = item

		// Assign the item to the next waiting lease in the batch,
		// returns false if there are no more leases available to fill
		if batchIter.Next(itemPtr) {
			// If assignment was a success, put the updated item into the array
			m.mem[i] = item
			continue
		}
		break
	}
	return nil
}

func (m *MemoryPartition) Complete(ctx context.Context, batch types.Batch[types.CompleteRequest]) error {
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

			// This should not happen, but we need to handle it anyway.
			if !m.mem[idx].EnqueueAt.IsZero() {
				m.log.LogAttrs(ctx, slog.LevelWarn, "attempted to complete a scheduled item; reported does not exist",
					slog.String("id", string(m.mem[idx].ID)))
				batch.Requests[i].Err = transport.NewInvalidOption("invalid storage id; '%s' does not exist", id)
				continue nextBatch
			}

			if !m.mem[idx].IsLeased {
				batch.Requests[i].Err = transport.NewConflict("item(s) cannot be completed; '%s' is not "+
					"marked as leased", id)
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

		// Do not report scheduled items in the queued items list
		if !item.EnqueueAt.IsZero() {
			continue
		}

		*items = append(*items, &item)
		count++
	}
	return nil
}

func (m *MemoryPartition) ListScheduled(_ context.Context, items *[]*types.Item, opts types.ListOptions) error {
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

		// Do not report scheduled items in the queued items list
		if item.EnqueueAt.IsZero() {
			continue
		}

		*items = append(*items, &item)
		count++
	}
	return nil
}

func (m *MemoryPartition) Add(_ context.Context, items []*types.Item, now clock.Time) error {
	defer m.mu.Unlock()
	m.mu.Lock()

	if len(items) == 0 {
		return transport.NewInvalidOption("items is invalid; cannot be empty")
	}

	for _, item := range items {
		m.uid = m.uid.Next()
		item.ID = []byte(m.uid.String())
		item.CreatedAt = now

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
		if item.IsLeased {
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

func (m *MemoryPartition) ScanForScheduled(_ clock.Duration, now clock.Time) iter.Seq[types.Action] {
	defer m.mu.RUnlock()
	m.mu.RLock()

	return func(yield func(types.Action) bool) {
		for _, item := range m.mem {
			// Skip non-scheduled items
			if item.EnqueueAt.IsZero() {
				continue
			}

			// NOTE: Different implementations might not include the entire item in the action if
			// only the id is needed to enqueue the item in place.
			if now.After(item.EnqueueAt) {
				yield(types.Action{
					Action:       types.ActionQueueScheduledItem,
					PartitionNum: m.info.PartitionNum,
					Queue:        m.info.Queue.Name,
					Item:         item,
				})
				continue
			}
		}
	}
}

func (m *MemoryPartition) ScanForActions(_ clock.Duration, now clock.Time) iter.Seq[types.Action] {
	defer m.mu.RUnlock()
	m.mu.RLock()

	return func(yield func(types.Action) bool) {
		for _, item := range m.mem {
			// Skip scheduled items
			if !item.EnqueueAt.IsZero() {
				continue
			}
			// Is the leased item expired?
			if item.IsLeased {
				if now.After(item.LeaseDeadline) {
					yield(types.Action{
						Action:       types.ActionLeaseExpired,
						PartitionNum: m.info.PartitionNum,
						Queue:        m.info.Queue.Name,
						Item:         item,
					})
					continue
				}
				// NOTE: This wll catch any items which may have been left in the data store
				// due to a failed /queue.retry call. This could happen if there is a dead letter queue
				// defined and retry inserted the item into the dead letter, but failed to remove it
				// due to partition error or catastrophic failure of querator before it could be removed.
				if m.info.Queue.MaxAttempts != 0 && item.Attempts >= m.info.Queue.MaxAttempts {
					yield(types.Action{
						Action:       types.ActionItemMaxAttempts,
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
		}
	}
}

func (m *MemoryPartition) TakeAction(_ context.Context, batch types.Batch[types.LifeCycleRequest],
	state *types.PartitionState) error {

	if len(batch.Requests) == 0 {
		return nil
	}

	defer m.mu.Unlock()
	m.mu.Lock()

	for _, req := range batch.Requests {
		for _, a := range req.Actions {
			switch a.Action {
			case types.ActionLeaseExpired:
				idx, ok := m.findID(a.Item.ID)
				if !ok {
					m.log.Warn("unable to find item while processing action; ignoring action",
						"id", a.Item.ID, "action", types.ActionToString(a.Action))
					continue
				}
				// make a copy of the item and mark as un-leased
				item := m.mem[idx]
				item.LeaseDeadline = clock.Time{}
				item.IsLeased = false

				// Assign a new ID to the item, as it is placed at the start of the queue
				m.uid = m.uid.Next()
				item.ID = []byte(m.uid.String())

				// Remove the item from the array
				m.mem = append(m.mem[:idx], m.mem[idx+1:]...)
				// Add the item to the tail of the array
				// See doc/adr/0022-managing-item-lifecycles.md for and explanation
				m.mem = append(m.mem, item)
			case types.ActionDeleteItem:
				idx, ok := m.findID(a.Item.ID)
				if !ok {
					continue
				}
				// Remove the item from the array
				m.mem = append(m.mem[:idx], m.mem[idx+1:]...)
			case types.ActionQueueScheduledItem:
				// NOTE: Different implementations might take the item provided and insert the item into
				// the partition, or they could update the item in place depending upon the capabilities
				// of the data store used.
				idx, ok := m.findID(a.Item.ID)
				if !ok {
					m.log.Warn("assertion failed; scheduled item is should be in partition; action ignored",
						"id", string(a.Item.ID))
					continue
				}

				// Make a copy of the item, then remove it
				item := m.mem[idx]
				if item.EnqueueAt.IsZero() {
					m.log.Warn("assertion failed; expected to be a scheduled item; action ignored",
						"id", string(a.Item.ID))
					continue
				}
				m.mem = append(m.mem[:idx], m.mem[idx+1:]...)

				// Assign a new id, and add the item to the tail
				m.uid = m.uid.Next()
				item.EnqueueAt = clock.Time{}
				item.ID = []byte(m.uid.String())
				m.mem = append(m.mem, item)
				// Tell the partition it gained a new un-leased item.
				state.UnLeased++
			default:
				m.log.Warn("assertion failed; undefined action", "action",
					fmt.Sprintf("0x%X", int(a.Action)))
			}
		}
	}
	return nil
}

func (m *MemoryPartition) LifeCycleInfo(ctx context.Context, info *types.LifeCycleInfo) error {
	defer m.mu.RUnlock()
	m.mu.RLock()
	next := theFuture

	// Find the item that will expire next
	for _, item := range m.mem {
		// Skip scheduled items
		if !item.EnqueueAt.IsZero() {
			continue
		}

		if item.LeaseDeadline.Before(next) {
			next = item.LeaseDeadline
		}
	}

	if next != theFuture {
		info.NextLeaseExpiry = next
	}
	return nil
}

func (m *MemoryPartition) Stats(_ context.Context, stats *types.PartitionStats, now clock.Time) error {
	defer m.mu.RUnlock()
	m.mu.RLock()

	for _, item := range m.mem {
		// Count scheduled and do not include them in other stats
		if !item.EnqueueAt.IsZero() {
			stats.Scheduled++
			continue
		}

		stats.Total++
		stats.AverageAge += now.Sub(item.CreatedAt)
		if item.IsLeased && item.EnqueueAt.IsZero() {
			stats.AverageLeasedAge += item.LeaseDeadline.Sub(now)
			stats.NumLeased++
		}
	}
	if stats.Total != 0 {
		stats.AverageAge = clock.Duration(int64(stats.AverageAge) / int64(stats.Total))
	}
	if stats.NumLeased != 0 {
		stats.AverageLeasedAge = clock.Duration(int64(stats.AverageLeasedAge) / int64(stats.NumLeased))
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
		if info.LeaseTimeout > info.ExpireTimeout {
			return transport.NewInvalidOption("lease timeout is too long; %s cannot be greater than the "+
				"expire timeout %s", info.LeaseTimeout.String(), info.ExpireTimeout.String())
		}
	}

	if info.LeaseTimeout > s.mem[idx].ExpireTimeout {
		return transport.NewInvalidOption("lease timeout is too long; %s cannot be greater than the "+
			"expire timeout %s", info.LeaseTimeout.String(), s.mem[idx].ExpireTimeout.String())
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
