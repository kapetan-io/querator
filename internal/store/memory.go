package store

import (
	"bytes"
	"context"
	"fmt"
	"iter"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport/reply"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/set"
	"github.com/segmentio/ksuid"
)

var bucketName = []byte("partition")

// ---------------------------------------------
// Partition Implementation
// ---------------------------------------------

type MemoryPartition struct {
	info       types.PartitionInfo
	conf       Config
	mem        []types.Item
	bySourceID map[string]struct{}
	mu         sync.RWMutex
	log        *slog.Logger
	uid        ksuid.KSUID
}

func (m *MemoryPartition) Produce(_ context.Context, batch types.ProduceBatch, now clock.Time) error {
	defer m.mu.Unlock()
	m.mu.Lock()

	for _, r := range batch.Requests {
		for _, item := range r.Items {
			// Check for duplicate SourceID
			if item.SourceID != nil {
				if _, exists := m.bySourceID[string(item.SourceID)]; exists {
					// Skip item - duplicate SourceID (idempotent)
					continue
				}
			}

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

			// Add to SourceID index if set
			if item.SourceID != nil {
				m.bySourceID[string(item.SourceID)] = struct{}{}
			}
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

func (m *MemoryPartition) Complete(ctx context.Context, batch types.CompleteBatch) error {
	defer m.mu.Unlock()
	m.mu.Lock()

nextBatch:
	for i := range batch.Requests {
		for _, id := range batch.Requests[i].Ids {
			if err := m.validateID(id); err != nil {
				batch.Requests[i].Err = reply.NewInvalidOption("invalid storage id; '%s': %s", id, err)
				continue nextBatch
			}

			idx, ok := m.findID(id)
			if !ok {
				batch.Requests[i].Err = reply.NewInvalidOption("invalid storage id; '%s' does not exist", id)
				continue nextBatch
			}

			// This should not happen, but we need to handle it anyway.
			if !m.mem[idx].EnqueueAt.IsZero() {
				m.log.LogAttrs(ctx, slog.LevelWarn, "attempted to complete a scheduled item; reported does not exist",
					slog.String("id", string(m.mem[idx].ID)))
				batch.Requests[i].Err = reply.NewInvalidOption("invalid storage id; '%s' does not exist", id)
				continue nextBatch
			}

			if !m.mem[idx].IsLeased {
				batch.Requests[i].Err = reply.NewConflict("item(s) cannot be completed; '%s' is not "+
					"marked as leased", id)
				continue nextBatch
			}
			// Remove the item from the array
			m.mem = append(m.mem[:idx], m.mem[idx+1:]...)
		}
	}
	return nil
}

func (m *MemoryPartition) Retry(ctx context.Context, batch types.RetryBatch) error {
	defer m.mu.Unlock()
	m.mu.Lock()

nextBatch:
	for i := range batch.Requests {
		for _, retryItem := range batch.Requests[i].Items {
			if err := m.validateID(retryItem.ID); err != nil {
				batch.Requests[i].Err = reply.NewInvalidOption("invalid storage id; '%s': %s", retryItem.ID, err)
				continue nextBatch
			}

			idx, ok := m.findID(retryItem.ID)
			if !ok {
				batch.Requests[i].Err = reply.NewInvalidOption("invalid storage id; '%s' does not exist", retryItem.ID)
				continue nextBatch
			}

			// This should not happen, but we need to handle it anyway.
			if !m.mem[idx].EnqueueAt.IsZero() {
				m.log.LogAttrs(ctx, slog.LevelWarn, "attempted to retry a scheduled item; reported does not exist",
					slog.String("id", string(m.mem[idx].ID)))
				batch.Requests[i].Err = reply.NewInvalidOption("invalid storage id; '%s' does not exist", retryItem.ID)
				continue nextBatch
			}

			if !m.mem[idx].IsLeased {
				batch.Requests[i].Err = reply.NewConflict("item(s) cannot be retried; '%s' is not "+
					"marked as leased", retryItem.ID)
				continue nextBatch
			}

			// Clear lease status (attempts incremented on next lease)
			
			m.mem[idx].IsLeased = false
			m.mem[idx].LeaseDeadline = clock.Time{}

			if retryItem.Dead {
				// TODO: Move to dead letter queue when implemented
				// For now, just remove the item
				m.mem = append(m.mem[:idx], m.mem[idx+1:]...)
			} else if !retryItem.RetryAt.IsZero() {
				// If RetryAt is in the past or less than 100ms from now, treat as immediate retry
				now := clock.Now().UTC()
				if retryItem.RetryAt.Before(now.Add(time.Millisecond * 100)) {
					// Immediate retry - EnqueueAt stays zero
					m.mem[idx].EnqueueAt = clock.Time{}
				} else {
					// Schedule for future retry
					m.mem[idx].EnqueueAt = retryItem.RetryAt
				}
			}
			// For immediate retry (empty RetryAt), item stays in queue with incremented attempts
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
			return reply.NewInvalidOption("invalid storage id; '%s': %s", opts.Pivot, err)
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
			return reply.NewInvalidOption("invalid storage id; '%s': %s", opts.Pivot, err)
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
		return reply.NewInvalidOption("items is invalid; cannot be empty")
	}

	for _, item := range items {
		// Check for duplicate SourceID
		if item.SourceID != nil {
			if _, exists := m.bySourceID[string(item.SourceID)]; exists {
				// Skip item - duplicate SourceID (idempotent)
				continue
			}
		}

		m.uid = m.uid.Next()
		item.ID = []byte(m.uid.String())
		item.CreatedAt = now

		m.mem = append(m.mem, *item)

		// Add to SourceID index if set
		if item.SourceID != nil {
			m.bySourceID[string(item.SourceID)] = struct{}{}
		}
	}
	return nil
}

func (m *MemoryPartition) Delete(_ context.Context, ids []types.ItemID) error {
	defer m.mu.Unlock()
	m.mu.Lock()

	for _, id := range ids {
		if err := m.validateID(id); err != nil {
			return reply.NewInvalidOption("invalid storage id; '%s': %s", id, err)
		}

		idx, ok := m.findID(id)
		if !ok {
			continue
		}

		// Remove from SourceID index if set
		if m.mem[idx].SourceID != nil {
			delete(m.bySourceID, string(m.mem[idx].SourceID))
		}

		// Remove the item from the array
		m.mem = append(m.mem[:idx], m.mem[idx+1:]...)
	}
	return nil
}

func (m *MemoryPartition) Clear(_ context.Context, req types.ClearRequest) error {
	defer m.mu.Unlock()
	m.mu.Lock()

	if req.Destructive && req.Queue {
		m.mem = make([]types.Item, 0, 1_000)
		m.bySourceID = make(map[string]struct{})
		return nil
	}

	mem := make([]types.Item, 0, len(m.mem))
	for _, item := range m.mem {
		shouldKeep := true

		// Clear scheduled items
		if req.Scheduled && !item.EnqueueAt.IsZero() {
			shouldKeep = false
		}

		// Clear queue items (those with EnqueueAt zero or in past)
		if req.Queue && item.EnqueueAt.IsZero() {
			if req.Destructive || !item.IsLeased {
				shouldKeep = false
			}
		}

		if shouldKeep {
			mem = append(mem, item)
		} else if item.SourceID != nil {
			// Remove from SourceID index if set
			delete(m.bySourceID, string(item.SourceID))
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

func (m *MemoryPartition) ScanForScheduled(_ context.Context, now clock.Time) iter.Seq2[types.Action, error] {
	m.mu.RLock()
	memCopy := make([]types.Item, len(m.mem))
	copy(memCopy, m.mem)
	m.mu.RUnlock()

	return func(yield func(types.Action, error) bool) {
		for _, item := range memCopy {
			// Skip non-scheduled items
			if item.EnqueueAt.IsZero() {
				continue
			}

			// NOTE: Different implementations might not include the entire item in the action if
			// only the id is needed to enqueue the item in place.
			if now.After(item.EnqueueAt) {
				if !yield(types.Action{
					Action:       types.ActionQueueScheduledItem,
					PartitionNum: m.info.PartitionNum,
					Queue:        m.info.Queue.Name,
					Item:         item,
				}, nil) {
					return
				}
			}
		}
	}
}

func (m *MemoryPartition) ScanForActions(_ context.Context, now clock.Time) iter.Seq2[types.Action, error] {
	m.mu.RLock()
	memCopy := make([]types.Item, len(m.mem))
	copy(memCopy, m.mem)
	m.mu.RUnlock()

	return func(yield func(types.Action, error) bool) {
		for _, item := range memCopy {
			// Skip scheduled items
			if !item.EnqueueAt.IsZero() {
				continue
			}
			// Is the leased item expired?
			if item.IsLeased {
				if now.After(item.LeaseDeadline) {
					if !yield(types.Action{
						Action:       types.ActionLeaseExpired,
						PartitionNum: m.info.PartitionNum,
						Queue:        m.info.Queue.Name,
						Item:         item,
					}, nil) {
						return
					}
					continue
				}
				// NOTE: This wll catch any items which may have been left in the data store
				// due to a failed /queue.retry call. This could happen if there is a dead letter queue
				// defined and retry inserted the item into the dead letter, but failed to remove it
				// due to partition error or catastrophic failure of querator before it could be removed.
				if m.info.Queue.MaxAttempts != 0 && item.Attempts >= m.info.Queue.MaxAttempts {
					if !yield(types.Action{
						Action:       types.ActionItemMaxAttempts,
						PartitionNum: m.info.PartitionNum,
						Queue:        m.info.Queue.Name,
						Item:         item,
					}, nil) {
						return
					}
					continue
				}
			}
			// Is the item expired?
			if now.After(item.ExpireDeadline) {
				if !yield(types.Action{
					Action:       types.ActionItemExpired,
					PartitionNum: m.info.PartitionNum,
					Queue:        m.info.Queue.Name,
					Item:         item,
				}, nil) {
					return
				}
				continue
			}
		}
	}
}

func (m *MemoryPartition) TakeAction(_ context.Context, batch types.LifeCycleBatch,
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
				// See docs/adr/0022-managing-item-lifecycles.md for and explanation
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
	nextLease := theFuture
	nextExpire := theFuture

	// Find the item that will expire next (lease or item expiry)
	for _, item := range m.mem {
		// Skip scheduled items
		if !item.EnqueueAt.IsZero() {
			continue
		}

		if item.LeaseDeadline.Before(nextLease) {
			nextLease = item.LeaseDeadline
		}

		if item.ExpireDeadline.Before(nextExpire) {
			nextExpire = item.ExpireDeadline
		}
	}

	if nextLease != theFuture {
		info.NextLeaseExpiry = nextLease
	}

	if nextExpire != theFuture {
		info.NextExpireDeadline = nextExpire
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
		return reply.NewInvalidOption("invalid queue; '%s' already exists", info.Name)
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
			return reply.NewInvalidOption("lease timeout is too long; %s cannot be greater than the "+
				"expire timeout %s", info.LeaseTimeout.String(), info.ExpireTimeout.String())
		}
	}

	if info.LeaseTimeout > s.mem[idx].ExpireTimeout {
		return reply.NewInvalidOption("lease timeout is too long; %s cannot be greater than the "+
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
		if opts.Namespace != "" && info.Namespace != opts.Namespace {
			continue
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
	conf       Config
	log        *slog.Logger
}

var _ PartitionStore = &MemoryPartitionStore{}

func NewMemoryPartitionStore(conf Config, log *slog.Logger) *MemoryPartitionStore {
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
			log:        m.log.With("queue", info.Queue.Name, "partition", info.PartitionNum),
			mem:        make([]types.Item, 0, 1_000),
			bySourceID: make(map[string]struct{}),
			uid:        ksuid.New(),
			conf:       m.conf,
			info:       info,
		}
		return m.partitions[info.HashKey()]
	}
	return p
}

// ---------------------------------------------
// Namespaces Implementation
// ---------------------------------------------

type MemoryNamespaces struct {
	mu  sync.RWMutex
	mem []types.Namespace
	log *slog.Logger
}

var _ Namespaces = &MemoryNamespaces{}

func NewMemoryNamespaces(log *slog.Logger) *MemoryNamespaces {
	set.Default(&log, slog.Default())
	return &MemoryNamespaces{
		mem: make([]types.Namespace, 0, 100),
		log: log,
	}
}

func (s *MemoryNamespaces) Get(_ context.Context, name string, ns *types.Namespace) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if strings.TrimSpace(name) == "" {
		return types.NewErrNamespaceNotExist(name)
	}

	idx, ok := s.findNamespace(name)
	if !ok {
		return types.NewErrNamespaceNotExist(name)
	}
	*ns = s.mem[idx]
	return nil
}

func (s *MemoryNamespaces) Add(_ context.Context, ns types.Namespace) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if strings.TrimSpace(ns.Name) == "" {
		return reply.NewInvalidOption("namespace name is invalid; cannot be empty")
	}

	_, ok := s.findNamespace(ns.Name)
	if ok {
		return types.NewErrNamespaceAlreadyExists(ns.Name)
	}

	s.mem = append(s.mem, ns)
	return nil
}

func (s *MemoryNamespaces) Update(_ context.Context, ns types.Namespace) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	idx, ok := s.findNamespace(ns.Name)
	if !ok {
		return types.NewErrNamespaceNotExist(ns.Name)
	}
	s.mem[idx] = ns
	return nil
}

func (s *MemoryNamespaces) List(_ context.Context, namespaces *[]types.Namespace, opts types.ListOptions) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var count, idx int
	if opts.Pivot != nil {
		idx, _ = s.findNamespace(string(opts.Pivot))
	}

	for _, ns := range s.mem[idx:] {
		if count >= opts.Limit {
			return nil
		}
		*namespaces = append(*namespaces, ns)
		count++
	}
	return nil
}

func (s *MemoryNamespaces) Delete(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if strings.TrimSpace(name) == "" {
		return types.NewErrNamespaceNotExist(name)
	}

	idx, ok := s.findNamespace(name)
	if !ok {
		return types.NewErrNamespaceNotExist(name)
	}
	s.mem = append(s.mem[:idx], s.mem[idx+1:]...)
	return nil
}

func (s *MemoryNamespaces) Close(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.mem = nil
	return nil
}

// findNamespace attempts to find the provided namespace in s.mem. If found returns the index and true.
// If not found returns the next nearest item in the list.
func (s *MemoryNamespaces) findNamespace(name string) (int, bool) {
	var nearest, nearestIdx int
	for i, ns := range s.mem {
		lex := strings.Compare(ns.Name, name)
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
// Users Implementation
// ---------------------------------------------

type MemoryUsers struct {
	byUsername map[string]string // username -> ID index
	users      map[string]types.User
	mu         sync.RWMutex
	log        *slog.Logger
}

var _ Users = &MemoryUsers{}

func NewMemoryUsers(log *slog.Logger) *MemoryUsers {
	set.Default(&log, slog.Default())
	return &MemoryUsers{
		byUsername: make(map[string]string),
		users:      make(map[string]types.User),
		log:        log,
	}
}

func (m *MemoryUsers) Get(_ context.Context, id string, user *types.User) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if strings.TrimSpace(id) == "" {
		return types.NewErrUserNotExist(id)
	}

	u, ok := m.users[id]
	if !ok {
		return types.NewErrUserNotExist(id)
	}
	*user = u
	return nil
}

func (m *MemoryUsers) GetByUsername(_ context.Context, username string, user *types.User) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if strings.TrimSpace(username) == "" {
		return types.NewErrUserNotExist(username)
	}

	id, ok := m.byUsername[username]
	if !ok {
		return types.NewErrUserNotExist(username)
	}

	u, ok := m.users[id]
	if !ok {
		return types.NewErrUserNotExist(username)
	}
	*user = u
	return nil
}

func (m *MemoryUsers) Add(_ context.Context, user types.User) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if strings.TrimSpace(user.ID) == "" {
		return reply.NewInvalidOption("user id is invalid; cannot be empty")
	}

	if strings.TrimSpace(user.Username) == "" {
		return reply.NewInvalidOption("username is invalid; cannot be empty")
	}

	if _, ok := m.users[user.ID]; ok {
		return types.NewErrUserAlreadyExists(user.ID)
	}

	if _, ok := m.byUsername[user.Username]; ok {
		return types.NewErrUsernameAlreadyTaken(user.Username)
	}

	m.users[user.ID] = user
	m.byUsername[user.Username] = user.ID
	return nil
}

func (m *MemoryUsers) List(_ context.Context, users *[]types.User, opts types.ListOptions) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var count int
	var pastPivot bool

	if opts.Pivot == nil {
		pastPivot = true
	}

	ids := make([]string, 0, len(m.users))
	for id := range m.users {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	for _, id := range ids {
		if count >= opts.Limit {
			return nil
		}

		if !pastPivot {
			if id == string(opts.Pivot) {
				pastPivot = true
			} else {
				continue
			}
		}

		*users = append(*users, m.users[id])
		count++
	}
	return nil
}

func (m *MemoryUsers) Delete(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if strings.TrimSpace(id) == "" {
		return types.NewErrUserNotExist(id)
	}

	user, ok := m.users[id]
	if !ok {
		return types.NewErrUserNotExist(id)
	}

	delete(m.byUsername, user.Username)
	delete(m.users, id)
	return nil
}

func (m *MemoryUsers) Close(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.users = nil
	m.byUsername = nil
	return nil
}

// ---------------------------------------------
// API Keys Implementation
// ---------------------------------------------

type MemoryAPIKeys struct {
	byHash map[string]string   // keyHash -> ID index
	byUser map[string][]string // userID -> []ID index
	keys   map[string]types.APIKey
	mu     sync.RWMutex
	log    *slog.Logger
}

var _ APIKeys = &MemoryAPIKeys{}

func NewMemoryAPIKeys(log *slog.Logger) *MemoryAPIKeys {
	set.Default(&log, slog.Default())
	return &MemoryAPIKeys{
		byHash: make(map[string]string),
		byUser: make(map[string][]string),
		keys:   make(map[string]types.APIKey),
		log:    log,
	}
}

func (m *MemoryAPIKeys) Get(_ context.Context, id string, key *types.APIKey) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if strings.TrimSpace(id) == "" {
		return types.ErrAPIKeyNotExist
	}

	k, ok := m.keys[id]
	if !ok {
		return types.ErrAPIKeyNotExist
	}
	*key = k
	return nil
}

func (m *MemoryAPIKeys) GetByHash(_ context.Context, hash string, key *types.APIKey) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if strings.TrimSpace(hash) == "" {
		return types.ErrAPIKeyNotExist
	}

	id, ok := m.byHash[hash]
	if !ok {
		return types.ErrAPIKeyNotExist
	}

	k, ok := m.keys[id]
	if !ok {
		return types.ErrAPIKeyNotExist
	}
	*key = k
	return nil
}

func (m *MemoryAPIKeys) Add(_ context.Context, key types.APIKey) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if strings.TrimSpace(key.ID) == "" {
		return reply.NewInvalidOption("api key id is invalid; cannot be empty")
	}

	if strings.TrimSpace(key.KeyHash) == "" {
		return reply.NewInvalidOption("api key hash is invalid; cannot be empty")
	}

	if _, ok := m.keys[key.ID]; ok {
		return reply.NewInvalidOption("api key already exists")
	}

	if _, ok := m.byHash[key.KeyHash]; ok {
		return reply.NewInvalidOption("api key hash already exists")
	}

	m.keys[key.ID] = key
	m.byHash[key.KeyHash] = key.ID
	m.byUser[key.UserID] = append(m.byUser[key.UserID], key.ID)
	return nil
}

func (m *MemoryAPIKeys) List(_ context.Context, keys *[]types.APIKey, opts types.ListOptions) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var count int
	var pastPivot bool

	if opts.Pivot == nil {
		pastPivot = true
	}

	ids := make([]string, 0, len(m.keys))
	for id := range m.keys {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	for _, id := range ids {
		if count >= opts.Limit {
			return nil
		}

		if !pastPivot {
			if id == string(opts.Pivot) {
				pastPivot = true
			} else {
				continue
			}
		}

		*keys = append(*keys, m.keys[id])
		count++
	}
	return nil
}

func (m *MemoryAPIKeys) ListByUser(_ context.Context, userID string, keys *[]types.APIKey, opts types.ListOptions) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ids, ok := m.byUser[userID]
	if !ok {
		return nil
	}

	var count int
	var pastPivot bool

	if opts.Pivot == nil {
		pastPivot = true
	}

	sortedIDs := make([]string, len(ids))
	copy(sortedIDs, ids)
	sort.Strings(sortedIDs)

	for _, id := range sortedIDs {
		if count >= opts.Limit {
			return nil
		}

		if !pastPivot {
			if id == string(opts.Pivot) {
				pastPivot = true
			} else {
				continue
			}
		}

		if k, ok := m.keys[id]; ok {
			*keys = append(*keys, k)
			count++
		}
	}
	return nil
}

func (m *MemoryAPIKeys) Delete(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if strings.TrimSpace(id) == "" {
		return nil
	}

	key, ok := m.keys[id]
	if !ok {
		return nil
	}

	delete(m.byHash, key.KeyHash)
	delete(m.keys, id)

	// Remove from user index
	userKeys := m.byUser[key.UserID]
	for i, k := range userKeys {
		if k == id {
			m.byUser[key.UserID] = append(userKeys[:i], userKeys[i+1:]...)
			break
		}
	}
	return nil
}

func (m *MemoryAPIKeys) DeleteByUser(_ context.Context, userID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	ids, ok := m.byUser[userID]
	if !ok {
		return nil
	}

	for _, id := range ids {
		if key, ok := m.keys[id]; ok {
			delete(m.byHash, key.KeyHash)
			delete(m.keys, id)
		}
	}
	delete(m.byUser, userID)
	return nil
}

func (m *MemoryAPIKeys) UpdateLastUsed(_ context.Context, id string, lastUsed clock.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key, ok := m.keys[id]
	if !ok {
		return types.ErrAPIKeyNotExist
	}

	key.LastUsedAt = &lastUsed
	m.keys[id] = key
	return nil
}

func (m *MemoryAPIKeys) Close(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.keys = nil
	m.byHash = nil
	m.byUser = nil
	return nil
}

// ---------------------------------------------
// Roles Implementation
// ---------------------------------------------

type MemoryRoles struct {
	byNamespaceName map[string]string // "namespace:name" -> ID index
	roles           map[string]types.Role
	mu              sync.RWMutex
	log             *slog.Logger
}

var _ Roles = &MemoryRoles{}

func NewMemoryRoles(log *slog.Logger) *MemoryRoles {
	set.Default(&log, slog.Default())
	return &MemoryRoles{
		byNamespaceName: make(map[string]string),
		roles:           make(map[string]types.Role),
		log:             log,
	}
}

func (m *MemoryRoles) Get(_ context.Context, namespace, name string, role *types.Role) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := namespace + "\x00" + name
	id, ok := m.byNamespaceName[key]
	if !ok {
		return types.NewErrRoleNotExist(namespace + ":" + name)
	}

	r, ok := m.roles[id]
	if !ok {
		return types.NewErrRoleNotExist(namespace + ":" + name)
	}
	*role = r
	return nil
}

func (m *MemoryRoles) GetByID(_ context.Context, id string, role *types.Role) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if strings.TrimSpace(id) == "" {
		return types.NewErrRoleNotExist(id)
	}

	r, ok := m.roles[id]
	if !ok {
		return types.NewErrRoleNotExist(id)
	}
	*role = r
	return nil
}

func (m *MemoryRoles) Add(_ context.Context, role types.Role) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if strings.TrimSpace(role.ID) == "" {
		return reply.NewInvalidOption("role id is invalid; cannot be empty")
	}

	if strings.TrimSpace(role.Name) == "" {
		return reply.NewInvalidOption("role name is invalid; cannot be empty")
	}

	if strings.TrimSpace(role.Namespace) == "" {
		return reply.NewInvalidOption("role namespace is invalid; cannot be empty")
	}

	key := role.Namespace + "\x00" + role.Name
	if _, ok := m.byNamespaceName[key]; ok {
		return types.NewErrRoleAlreadyExists(role.Namespace, role.Name)
	}

	if _, ok := m.roles[role.ID]; ok {
		return types.NewErrRoleAlreadyExists(role.Namespace, role.Name)
	}

	m.roles[role.ID] = role
	m.byNamespaceName[key] = role.ID
	return nil
}

func (m *MemoryRoles) Update(_ context.Context, role types.Role) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if strings.TrimSpace(role.ID) == "" {
		return types.NewErrRoleNotExist(role.ID)
	}

	existing, ok := m.roles[role.ID]
	if !ok {
		return types.NewErrRoleNotExist(role.ID)
	}

	// Update permissions only, keep other fields
	existing.Permissions = role.Permissions
	m.roles[role.ID] = existing
	return nil
}

func (m *MemoryRoles) List(_ context.Context, namespace string, roles *[]types.Role, opts types.ListOptions) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var count int
	var pastPivot bool

	if opts.Pivot == nil {
		pastPivot = true
	}

	ids := make([]string, 0, len(m.roles))
	for id, role := range m.roles {
		if namespace != "" && role.Namespace != namespace {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)

	for _, id := range ids {
		if count >= opts.Limit {
			return nil
		}

		if !pastPivot {
			if id == string(opts.Pivot) {
				pastPivot = true
			} else {
				continue
			}
		}

		*roles = append(*roles, m.roles[id])
		count++
	}
	return nil
}

func (m *MemoryRoles) Delete(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if strings.TrimSpace(id) == "" {
		return types.NewErrRoleNotExist(id)
	}

	role, ok := m.roles[id]
	if !ok {
		return types.NewErrRoleNotExist(id)
	}

	key := role.Namespace + "\x00" + role.Name
	delete(m.byNamespaceName, key)
	delete(m.roles, id)
	return nil
}

func (m *MemoryRoles) Close(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.roles = nil
	m.byNamespaceName = nil
	return nil
}

// ---------------------------------------------
// RoleBindings Implementation
// ---------------------------------------------

type MemoryRoleBindings struct {
	byUserNamespaceRole map[string]string   // "userID\x00namespace\x00roleID" -> ID (for uniqueness)
	byUser              map[string][]string // userID -> []binding IDs
	byRole              map[string][]string // roleID -> []binding IDs
	bindings            map[string]types.RoleBinding
	mu                  sync.RWMutex
	log                 *slog.Logger
}

var _ RoleBindings = &MemoryRoleBindings{}

func NewMemoryRoleBindings(log *slog.Logger) *MemoryRoleBindings {
	set.Default(&log, slog.Default())
	return &MemoryRoleBindings{
		byUserNamespaceRole: make(map[string]string),
		byUser:              make(map[string][]string),
		byRole:              make(map[string][]string),
		bindings:            make(map[string]types.RoleBinding),
		log:                 log,
	}
}

func (m *MemoryRoleBindings) Get(_ context.Context, id string, binding *types.RoleBinding) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if strings.TrimSpace(id) == "" {
		return types.NewErrRoleBindingNotExist(id)
	}

	b, ok := m.bindings[id]
	if !ok {
		return types.NewErrRoleBindingNotExist(id)
	}
	*binding = b
	return nil
}

func (m *MemoryRoleBindings) Add(_ context.Context, binding types.RoleBinding) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if strings.TrimSpace(binding.ID) == "" {
		return reply.NewInvalidOption("role binding id is invalid; cannot be empty")
	}

	if strings.TrimSpace(binding.UserID) == "" {
		return reply.NewInvalidOption("role binding user_id is invalid; cannot be empty")
	}

	if strings.TrimSpace(binding.RoleID) == "" {
		return reply.NewInvalidOption("role binding role_id is invalid; cannot be empty")
	}

	if strings.TrimSpace(binding.Namespace) == "" {
		return reply.NewInvalidOption("role binding namespace is invalid; cannot be empty")
	}

	// Check for duplicate (same user, namespace, role combination)
	uniqueKey := binding.UserID + "\x00" + binding.Namespace + "\x00" + binding.RoleID
	if _, ok := m.byUserNamespaceRole[uniqueKey]; ok {
		return types.NewErrRoleBindingAlreadyExists(binding.UserID, binding.RoleID, binding.Namespace)
	}

	if _, ok := m.bindings[binding.ID]; ok {
		return types.NewErrRoleBindingAlreadyExists(binding.UserID, binding.RoleID, binding.Namespace)
	}

	m.bindings[binding.ID] = binding
	m.byUserNamespaceRole[uniqueKey] = binding.ID
	m.byUser[binding.UserID] = append(m.byUser[binding.UserID], binding.ID)
	m.byRole[binding.RoleID] = append(m.byRole[binding.RoleID], binding.ID)
	return nil
}

func (m *MemoryRoleBindings) List(_ context.Context, namespace string, bindings *[]types.RoleBinding, opts types.ListOptions) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var count int
	var pastPivot bool

	if opts.Pivot == nil {
		pastPivot = true
	}

	ids := make([]string, 0, len(m.bindings))
	for id, binding := range m.bindings {
		if namespace != "" && binding.Namespace != namespace {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)

	for _, id := range ids {
		if count >= opts.Limit {
			return nil
		}

		if !pastPivot {
			if id == string(opts.Pivot) {
				pastPivot = true
			} else {
				continue
			}
		}

		*bindings = append(*bindings, m.bindings[id])
		count++
	}
	return nil
}

func (m *MemoryRoleBindings) ListByUser(_ context.Context, userID string, bindings *[]types.RoleBinding) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ids, ok := m.byUser[userID]
	if !ok {
		return nil
	}

	for _, id := range ids {
		if b, ok := m.bindings[id]; ok {
			*bindings = append(*bindings, b)
		}
	}
	return nil
}

func (m *MemoryRoleBindings) ListByRole(_ context.Context, roleID string, bindings *[]types.RoleBinding) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ids, ok := m.byRole[roleID]
	if !ok {
		return nil
	}

	for _, id := range ids {
		if b, ok := m.bindings[id]; ok {
			*bindings = append(*bindings, b)
		}
	}
	return nil
}

func (m *MemoryRoleBindings) DeleteByUserAndRole(_ context.Context, namespace, userID, roleID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	uniqueKey := userID + "\x00" + namespace + "\x00" + roleID
	id, ok := m.byUserNamespaceRole[uniqueKey]
	if !ok {
		return types.NewErrRoleBindingNotExist(namespace + ":" + userID + ":" + roleID)
	}

	if _, ok := m.bindings[id]; !ok {
		return types.NewErrRoleBindingNotExist(namespace + ":" + userID + ":" + roleID)
	}

	delete(m.byUserNamespaceRole, uniqueKey)
	delete(m.bindings, id)

	userBindings := m.byUser[userID]
	for i, bid := range userBindings {
		if bid == id {
			m.byUser[userID] = append(userBindings[:i], userBindings[i+1:]...)
			break
		}
	}

	roleBindings := m.byRole[roleID]
	for i, bid := range roleBindings {
		if bid == id {
			m.byRole[roleID] = append(roleBindings[:i], roleBindings[i+1:]...)
			break
		}
	}

	return nil
}

func (m *MemoryRoleBindings) Delete(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if strings.TrimSpace(id) == "" {
		return types.NewErrRoleBindingNotExist(id)
	}

	binding, ok := m.bindings[id]
	if !ok {
		return types.NewErrRoleBindingNotExist(id)
	}

	// Remove from uniqueness index
	uniqueKey := binding.UserID + "\x00" + binding.Namespace + "\x00" + binding.RoleID
	delete(m.byUserNamespaceRole, uniqueKey)
	delete(m.bindings, id)

	// Remove from user index
	userBindings := m.byUser[binding.UserID]
	for i, bid := range userBindings {
		if bid == id {
			m.byUser[binding.UserID] = append(userBindings[:i], userBindings[i+1:]...)
			break
		}
	}

	// Remove from role index
	roleBindings := m.byRole[binding.RoleID]
	for i, bid := range roleBindings {
		if bid == id {
			m.byRole[binding.RoleID] = append(roleBindings[:i], roleBindings[i+1:]...)
			break
		}
	}

	return nil
}

func (m *MemoryRoleBindings) DeleteByUser(_ context.Context, userID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	ids, ok := m.byUser[userID]
	if !ok {
		return nil
	}

	for _, id := range ids {
		if binding, ok := m.bindings[id]; ok {
			// Remove from uniqueness index
			uniqueKey := binding.UserID + "\x00" + binding.Namespace + "\x00" + binding.RoleID
			delete(m.byUserNamespaceRole, uniqueKey)
			delete(m.bindings, id)

			// Remove from role index
			roleBindings := m.byRole[binding.RoleID]
			for i, bid := range roleBindings {
				if bid == id {
					m.byRole[binding.RoleID] = append(roleBindings[:i], roleBindings[i+1:]...)
					break
				}
			}
		}
	}
	delete(m.byUser, userID)
	return nil
}

func (m *MemoryRoleBindings) Close(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.bindings = nil
	m.byUserNamespaceRole = nil
	m.byUser = nil
	m.byRole = nil
	return nil
}
