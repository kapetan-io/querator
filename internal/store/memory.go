package store

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport/reply"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/set"
	"github.com/kapetan-io/tiger/assert"
	"github.com/segmentio/ksuid"
)

var bucketName = []byte("partition")

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
			if item.SourceID != nil {
				if _, exists := m.bySourceID[string(item.SourceID)]; exists {
					continue
				}
			}

			m.uid = m.uid.Next()
			item.ID = []byte(m.uid.String())
			item.CreatedAt = now

			if item.EnqueueAt.Before(now.Add(time.Millisecond * 100)) {
				item.EnqueueAt = time.Time{}
			}

			m.mem = append(m.mem, *item)

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
		if item.IsLeased || !item.EnqueueAt.IsZero() {
			continue
		}

		item.LeaseDeadline = opts.LeaseDeadline
		item.IsLeased = true
		item.Attempts++
		count++

		itemPtr := new(types.Item)
		*itemPtr = item

		if batchIter.Next(itemPtr) {
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

	for _, req := range batch.Requests {
		m.completeItems(ctx, req)
	}
	return nil
}

func (m *MemoryPartition) completeItems(ctx context.Context, req *types.CompleteRequest) {
	for _, id := range req.Ids {
		if err := m.validateID(id); err != nil {
			req.Err = reply.NewInvalidOption("invalid storage id; '%s': %s", id, err)
			return
		}

		idx, ok := m.findID(id)
		if !ok {
			req.Err = reply.NewInvalidOption("invalid storage id; '%s' does not exist", id)
			return
		}

		if !m.mem[idx].EnqueueAt.IsZero() {
			m.log.LogAttrs(ctx, slog.LevelWarn, "attempted to complete a scheduled item; reported does not exist",
				slog.String("id", string(m.mem[idx].ID)))
			req.Err = reply.NewInvalidOption("invalid storage id; '%s' does not exist", id)
			return
		}

		if !m.mem[idx].IsLeased {
			req.Err = reply.NewConflict("item(s) cannot be completed; '%s' is not "+
				"marked as leased", id)
			return
		}
		if m.mem[idx].SourceID != nil {
			delete(m.bySourceID, string(m.mem[idx].SourceID))
		}
		m.mem = append(m.mem[:idx], m.mem[idx+1:]...)
		req.Completed++
	}
}

func (m *MemoryPartition) Retry(ctx context.Context, batch types.RetryBatch) error {
	defer m.mu.Unlock()
	m.mu.Lock()

	for _, req := range batch.Requests {
		m.retryItems(ctx, req)
	}
	return nil
}

func (m *MemoryPartition) retryItems(ctx context.Context, req *types.RetryRequest) {
	for _, retryItem := range req.Items {
		if err := m.validateID(retryItem.ID); err != nil {
			req.Err = reply.NewInvalidOption("invalid storage id; '%s': %s", retryItem.ID, err)
			return
		}

		idx, ok := m.findID(retryItem.ID)
		if !ok {
			req.Err = reply.NewInvalidOption("invalid storage id; '%s' does not exist", retryItem.ID)
			return
		}

		if !m.mem[idx].EnqueueAt.IsZero() {
			m.log.LogAttrs(ctx, slog.LevelWarn, "attempted to retry a scheduled item; reported does not exist",
				slog.String("id", string(m.mem[idx].ID)))
			req.Err = reply.NewInvalidOption("invalid storage id; '%s' does not exist", retryItem.ID)
			return
		}

		if !m.mem[idx].IsLeased {
			req.Err = reply.NewConflict("item(s) cannot be retried; '%s' is not "+
				"marked as leased", retryItem.ID)
			return
		}

		switch {
		case retryItem.Dead:
			if m.mem[idx].SourceID != nil {
				delete(m.bySourceID, string(m.mem[idx].SourceID))
			}
			m.mem = append(m.mem[:idx], m.mem[idx+1:]...)
		case !retryItem.RetryAt.IsZero():
			now := clock.Now().UTC()
			if retryItem.RetryAt.Before(now.Add(time.Millisecond * 100)) {
				item := m.mem[idx]
				item.IsLeased = false
				item.LeaseDeadline = clock.Time{}
				item.EnqueueAt = clock.Time{}
				m.uid = m.uid.Next()
				item.ID = []byte(m.uid.String())
				m.mem = append(m.mem[:idx], m.mem[idx+1:]...)
				m.mem = append(m.mem, item)
				req.UnLeased++
			} else {
				m.mem[idx].IsLeased = false
				m.mem[idx].LeaseDeadline = clock.Time{}
				m.mem[idx].EnqueueAt = retryItem.RetryAt
			}
		default:
			item := m.mem[idx]
			item.IsLeased = false
			item.LeaseDeadline = clock.Time{}
			m.uid = m.uid.Next()
			item.ID = []byte(m.uid.String())
			m.mem = append(m.mem[:idx], m.mem[idx+1:]...)
			m.mem = append(m.mem, item)
			req.UnLeased++
		}
		req.Retried++
	}
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
		if item.SourceID != nil {
			if _, exists := m.bySourceID[string(item.SourceID)]; exists {
				continue
			}
		}

		m.uid = m.uid.Next()
		item.ID = []byte(m.uid.String())
		item.CreatedAt = now

		m.mem = append(m.mem, *item)

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

		if m.mem[idx].SourceID != nil {
			delete(m.bySourceID, string(m.mem[idx].SourceID))
		}

		m.mem = append(m.mem[:idx], m.mem[idx+1:]...)
	}
	return nil
}

func (m *MemoryPartition) Clear(_ context.Context, req types.ClearRequest) error {
	defer m.mu.Unlock()
	m.mu.Lock()

	if req.Destructive && req.Queue && req.Scheduled {
		m.mem = make([]types.Item, 0, 1_000)
		m.bySourceID = make(map[string]struct{})
		return nil
	}

	mem := make([]types.Item, 0, len(m.mem))
	for _, item := range m.mem {
		shouldKeep := true

		if req.Scheduled && !item.EnqueueAt.IsZero() {
			shouldKeep = false
		}

		if req.Queue && item.EnqueueAt.IsZero() {
			if req.Destructive || !item.IsLeased {
				shouldKeep = false
			}
		}

		if shouldKeep {
			mem = append(mem, item)
		} else if item.SourceID != nil {
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

func (m *MemoryPartition) ScanForScheduled(_ context.Context, scan *types.LifeCycleScan, now clock.Time) error {
	defer m.mu.RUnlock()
	m.mu.RLock()

	page := m.lifeCyclePage(scan)
	for _, item := range page {
		if item.EnqueueAt.IsZero() {
			continue
		}
		if item.EnqueueAt.After(now) {
			scan.SeeEnqueueAt(item.EnqueueAt)
			continue
		}
		scan.Add(types.Action{
			Action:       types.ActionQueueScheduledItem,
			PartitionNum: m.info.PartitionNum,
			Queue:        m.info.Queue.Name,
			Item:         item,
		})
	}
	return nil
}

func (m *MemoryPartition) ScanForActions(_ context.Context, scan *types.LifeCycleScan, now clock.Time) error {
	defer m.mu.RUnlock()
	m.mu.RLock()

	page := m.lifeCyclePage(scan)
	for _, item := range page {
		if !item.EnqueueAt.IsZero() {
			continue
		}
		if item.IsLeased {
			if !item.LeaseDeadline.After(now) {
				scan.Add(types.Action{
					Action:       types.ActionLeaseExpired,
					PartitionNum: m.info.PartitionNum,
					Queue:        m.info.Queue.Name,
					Item:         item,
				})
				continue
			}
			if m.info.Queue.MaxAttempts != 0 && item.Attempts >= m.info.Queue.MaxAttempts {
				scan.Add(types.Action{
					Action:       types.ActionItemMaxAttempts,
					PartitionNum: m.info.PartitionNum,
					Queue:        m.info.Queue.Name,
					Item:         item,
				})
				continue
			}
			scan.SeeLeaseDeadline(item.LeaseDeadline)
		}
		if !item.ExpireDeadline.After(now) {
			scan.Add(types.Action{
				Action:       types.ActionItemExpired,
				PartitionNum: m.info.PartitionNum,
				Queue:        m.info.Queue.Name,
				Item:         item,
			})
			continue
		}
		scan.SeeExpireDeadline(item.ExpireDeadline)
	}
	return nil
}

func (m *MemoryPartition) lifeCyclePage(scan *types.LifeCycleScan) []types.Item {
	if len(m.mem) == 0 {
		scan.EndPass()
		return nil
	}
	if !scan.MidPass() {
		scan.Last = m.mem[len(m.mem)-1].ID
	}
	start := sort.Search(len(m.mem), func(i int) bool {
		return bytes.Compare(m.mem[i].ID, scan.Pivot) > 0
	})
	end := sort.Search(len(m.mem), func(i int) bool {
		return bytes.Compare(m.mem[i].ID, scan.Last) > 0
	})
	if start >= end {
		scan.EndPass()
		return nil
	}
	last := min(end, start+LifeCyclePageSize)
	if last == end {
		scan.EndPass()
	} else {
		scan.Pivot = m.mem[last-1].ID
	}
	return m.mem[start:last]
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
				item := m.mem[idx]
				item.LeaseDeadline = clock.Time{}
				item.IsLeased = false

				m.uid = m.uid.Next()
				item.ID = []byte(m.uid.String())

				m.mem = append(m.mem[:idx], m.mem[idx+1:]...)
				m.mem = append(m.mem, item)
				state.LeasesExpired(1)
			case types.ActionDeleteItem:
				idx, ok := m.findID(a.Item.ID)
				if !ok {
					continue
				}
				if m.mem[idx].SourceID != nil {
					delete(m.bySourceID, string(m.mem[idx].SourceID))
				}
				m.mem = append(m.mem[:idx], m.mem[idx+1:]...)
			case types.ActionQueueScheduledItem:
				idx, ok := m.findID(a.Item.ID)
				if !ok {
					m.log.Warn("assertion failed; scheduled item is should be in partition; action ignored",
						"id", string(a.Item.ID))
					continue
				}

				item := m.mem[idx]
				if item.EnqueueAt.IsZero() {
					m.log.Warn("assertion failed; expected to be a scheduled item; action ignored",
						"id", string(a.Item.ID))
					continue
				}
				m.mem = append(m.mem[:idx], m.mem[idx+1:]...)

				m.uid = m.uid.Next()
				item.EnqueueAt = clock.Time{}
				item.ID = []byte(m.uid.String())
				m.mem = append(m.mem, item)
				state.UnLeased++
			default:
				assert.Unreachable(fmt.Sprintf("undefined action kind '0x%X'", int(a.Action)))
			}
		}
	}
	return nil
}

func (m *MemoryPartition) Stats(_ context.Context, stats *types.PartitionStats, now clock.Time) error {
	defer m.mu.RUnlock()
	m.mu.RLock()

	for _, item := range m.mem {
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

func (m *MemoryPartition) findID(id []byte) (int, bool) {
	for i, item := range m.mem {
		lex := bytes.Compare(item.ID, id)
		if lex == 0 {
			return i, true
		}
		if lex > 0 {
			return i, false
		}
	}
	return len(m.mem), false
}

type MemoryQueues struct {
	QueuesValidation
	mu       sync.RWMutex
	mem      []types.QueueInfo
	backends []types.Backend
	log      *slog.Logger
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

	s.mu.RLock()
	defer s.mu.RUnlock()

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
	info.Namespace = systemIfEmpty(info.Namespace)

	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.findQueue(info.Name)
	if ok {
		return reply.NewInvalidOptionKind(types.KindQueueAlreadyExists, "invalid queue; '%s' already exists", info.Name)
	}

	s.mem = append(s.mem, info)
	return nil
}

func (s *MemoryQueues) Update(_ context.Context, info types.QueueInfo) error {
	if err := s.validateQueueName(info); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	idx, ok := s.findQueue(info.Name)
	if !ok {
		return ErrQueueNotExist
	}

	found := s.mem[idx]
	if err := s.validateTimeoutChange(timeoutChange{Stored: found, Proposed: info}); err != nil {
		return err
	}

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

	s.mu.RLock()
	defer s.mu.RUnlock()

	var count, idx int
	if opts.Pivot != nil {
		idx, _ = s.findQueue(string(opts.Pivot))
	}

	for _, info := range s.mem[idx:] {
		if opts.Namespace != "" && info.Namespace != opts.Namespace {
			continue
		}
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

	s.mu.Lock()
	defer s.mu.Unlock()

	idx, ok := s.findQueue(name)
	if !ok {
		return nil
	}
	s.mem = append(s.mem[:idx], s.mem[idx+1:]...)
	return nil
}

func (s *MemoryQueues) AddBackend(_ context.Context, backend types.Backend) error {
	if err := s.validateBackend(backend); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.findBackend(backend.Name); ok {
		return types.ErrBackendAlreadyExists
	}
	s.backends = append(s.backends, backend)
	slices.SortFunc(s.backends, func(a, b types.Backend) int { return strings.Compare(a.Name, b.Name) })
	return nil
}

func (s *MemoryQueues) UpdateBackend(_ context.Context, backend types.Backend) error {
	if err := s.validateBackend(backend); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	idx, ok := s.findBackend(backend.Name)
	if !ok {
		return types.ErrBackendNotExist
	}
	s.backends[idx] = backend
	return nil
}

func (s *MemoryQueues) ListBackends(_ context.Context, backends *[]types.Backend) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	*backends = append(*backends, s.backends...)
	return nil
}

func (s *MemoryQueues) findBackend(name string) (int, bool) {
	for i, backend := range s.backends {
		if backend.Name == name {
			return i, true
		}
	}
	return 0, false
}

func (s *MemoryQueues) Close(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.mem = nil
	s.backends = nil
	return nil
}

func (s *MemoryQueues) findQueue(name string) (int, bool) {
	nearestIdx := len(s.mem)
	for i, queue := range s.mem {
		lex := strings.Compare(queue.Name, name)
		if lex == 0 {
			return i, true
		}
		if lex > 0 && i < nearestIdx {
			nearestIdx = i
		}
	}
	return nearestIdx, false
}

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

func (s *MemoryNamespaces) findNamespace(name string) (int, bool) {
	nearestIdx := len(s.mem)
	for i, ns := range s.mem {
		lex := strings.Compare(ns.Name, name)
		if lex == 0 {
			return i, true
		}
		if lex > 0 && i < nearestIdx {
			nearestIdx = i
		}
	}
	return nearestIdx, false
}

type MemoryUsers struct {
	byUsername map[string]string
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

type MemoryAPIKeys struct {
	byHash map[string]string
	byUser map[string][]string
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

type MemoryRoles struct {
	byNamespaceName map[string]string
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

func (m *MemoryRoles) Get(_ context.Context, roleName RoleName, role *types.Role) error {
	namespace, name := roleName.Namespace, roleName.Name
	m.mu.RLock()
	defer m.mu.RUnlock()

	id, ok := m.byNamespaceName[namespace+"\x00"+name]
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
		return types.NewErrRoleAlreadyExists(role)
	}

	if _, ok := m.roles[role.ID]; ok {
		return types.NewErrRoleAlreadyExists(role)
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

type MemoryRoleBindings struct {
	byUserNamespaceRole map[string]string
	byUser              map[string][]string
	byRole              map[string][]string
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

	uniqueKey := binding.UserID + "\x00" + binding.Namespace + "\x00" + binding.RoleID
	if _, ok := m.byUserNamespaceRole[uniqueKey]; ok {
		return types.NewErrRoleBindingAlreadyExists(binding)
	}

	if _, ok := m.bindings[binding.ID]; ok {
		return types.NewErrRoleBindingAlreadyExists(binding)
	}

	var held int
	for _, id := range m.byUser[binding.UserID] {
		if m.bindings[id].Namespace == binding.Namespace {
			held++
		}
	}
	if held >= types.MaxRoleBindingsPerUserInNamespace {
		return types.NewErrRoleBindingLimitReached(binding)
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

func (m *MemoryRoleBindings) ListByUser(_ context.Context, user UserInNamespace, bindings *[]types.RoleBinding, limit int) error {
	userID, namespace := user.UserID, user.Namespace
	m.mu.RLock()
	defer m.mu.RUnlock()

	ids, ok := m.byUser[userID]
	if !ok {
		return nil
	}

	for _, id := range ids {
		if len(*bindings) >= limit {
			break
		}
		if b, ok := m.bindings[id]; ok && b.Namespace == namespace {
			*bindings = append(*bindings, b)
		}
	}
	return nil
}

func (m *MemoryRoleBindings) ListByRole(_ context.Context, roleID string, bindings *[]types.RoleBinding, limit int) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ids, ok := m.byRole[roleID]
	if !ok {
		return nil
	}

	for _, id := range ids {
		if limit > 0 && len(*bindings) >= limit {
			break
		}
		if b, ok := m.bindings[id]; ok {
			*bindings = append(*bindings, b)
		}
	}
	return nil
}

func (m *MemoryRoleBindings) DeleteByUserAndRole(_ context.Context, binding RoleForUser) error {
	namespace, userID, roleID := binding.Namespace, binding.UserID, binding.RoleID
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

	uniqueKey := binding.UserID + "\x00" + binding.Namespace + "\x00" + binding.RoleID
	delete(m.byUserNamespaceRole, uniqueKey)
	delete(m.bindings, id)

	userBindings := m.byUser[binding.UserID]
	for i, bid := range userBindings {
		if bid == id {
			m.byUser[binding.UserID] = append(userBindings[:i], userBindings[i+1:]...)
			break
		}
	}

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
			uniqueKey := binding.UserID + "\x00" + binding.Namespace + "\x00" + binding.RoleID
			delete(m.byUserNamespaceRole, uniqueKey)
			delete(m.bindings, id)

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
