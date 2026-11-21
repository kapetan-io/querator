package internal

import (
	"context"
	"log/slog"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"

	"github.com/kapetan-io/errors"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport/reply"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/set"
)

const MsgServiceInShutdown = "service is shutting down"

var ErrServiceShutdown = reply.NewRequestFailed(MsgServiceInShutdown)

type QueuesManagerConfig struct {
	StorageConfig store.Config
	LogicalConfig LogicalConfig
	Log           *slog.Logger
}

type QueuesManager struct {
	queues     map[string]*Queue
	conf       QueuesManagerConfig
	inShutdown atomic.Bool
	mutex      sync.RWMutex
	log        *slog.Logger
}

func NewQueuesManager(conf QueuesManagerConfig) (*QueuesManager, error) {
	set.Default(&conf.LogicalConfig.Clock, clock.NewProvider())
	set.Default(&conf.Log, slog.Default())

	if conf.StorageConfig.Queues == nil {
		return nil, errors.New("conf.Config.Queues cannot be nil")
	}

	if len(conf.StorageConfig.PartitionStorage) == 0 {
		return nil, errors.New("conf.Config.PartitionStorage cannot be empty")
	}

	var total float64
	seen := make(map[string]struct{}, len(conf.StorageConfig.PartitionStorage))
	for _, b := range conf.StorageConfig.PartitionStorage {
		if _, ok := seen[b.Name]; ok {
			return nil, errors.Errorf("duplicate partition storage name '%s'; every partition storage must have a unique name", b.Name)
		}
		seen[b.Name] = struct{}{}
		if math.IsNaN(b.Affinity) || math.IsInf(b.Affinity, 0) {
			return nil, errors.Errorf("partition storage '%s' affinity must be a finite number, got %v", b.Name, b.Affinity)
		}
		if b.Affinity > 0 {
			total += b.Affinity
		}
	}
	if math.IsInf(total, 0) {
		return nil, errors.New("the sum of partition storage affinities is too large; lower the affinities")
	}

	qm := &QueuesManager{
		log:    conf.Log.With("code.namespace", "QueuesManager"),
		queues: make(map[string]*Queue),
		conf:   conf,
	}

	return qm, nil
}

func (qm *QueuesManager) Create(ctx context.Context, info types.QueueInfo) (*Queue, error) {
	if qm.inShutdown.Load() {
		return nil, ErrServiceShutdown
	}
	defer qm.mutex.Unlock()
	qm.mutex.Lock()

	q, ok := qm.queues[info.Name]
	if ok {
		return q, reply.NewInvalidOption("invalid queue; '%s' already exists", info.Name)
	}

	if !store.CanPlacePartitions(qm.conf.StorageConfig.PartitionStorage) {
		return nil, reply.NewRequestFailed("no storage backend can take partitions; " +
			"at least one partition storage needs an affinity above 0")
	}

	if err := qm.validateDeadQueue(ctx, info); err != nil {
		return nil, err
	}

	if err := qm.validateAsDeadQueue(ctx, info); err != nil {
		return nil, err
	}

	placed, err := store.PlacePartitions(qm.conf.StorageConfig.PartitionStorage, info)
	if err != nil {
		return nil, err
	}
	info.PartitionInfo = placed
	for _, p := range info.PartitionInfo {
		qm.log.LogAttrs(ctx, LevelDebugAll, "Partition Assigned",
			slog.String("queue", p.Queue.Name), slog.String("storage", p.StorageName),
			slog.Bool("read-only", p.ReadOnly), slog.Int("partition", p.PartitionNum))
	}

	info.CreatedAt = qm.conf.LogicalConfig.Clock.Now().UTC()
	info.UpdatedAt = qm.conf.LogicalConfig.Clock.Now().UTC()
	if err := qm.conf.StorageConfig.Queues.Add(ctx, info); err != nil {
		return nil, errors.With("queue", info.Name).
			Errorf("Queues.Add(): %w", err)
	}

	return qm.get(ctx, info.Name)
}

func (qm *QueuesManager) Get(ctx context.Context, name string) (*Queue, error) {
	if qm.inShutdown.Load() {
		return nil, ErrServiceShutdown
	}
	defer qm.mutex.Unlock()
	qm.mutex.Lock()

	return qm.get(ctx, name)
}

func (qm *QueuesManager) get(ctx context.Context, name string) (*Queue, error) {
	q, ok := qm.queues[name]
	if ok {
		return q, nil
	}

	var queue types.QueueInfo
	if err := qm.conf.StorageConfig.Queues.Get(ctx, name, &queue); err != nil {
		if errors.Is(err, store.ErrQueueNotExist) {
			return nil, reply.NewInvalidOption("queue does not exist; no such queue named '%s'", name)
		}
		return nil, err
	}

	var partitions []store.Partition
	for _, info := range queue.PartitionInfo {

		b := store.Find(info.StorageName, qm.conf.StorageConfig.PartitionStorage)
		if b.Name == "" {
			return nil, errors.WithAttr(
				slog.Int("partition", info.PartitionNum),
				slog.String("storage-name", info.StorageName),
				slog.String("queue", queue.Name),
			).Error("queue partition references unknown storage name")
		}
		partitions = append(partitions, b.PartitionStore.Get(info))
	}

	if len(partitions) == 0 {
		qm.queues[queue.Name] = NewQueue(queue)
		return qm.queues[queue.Name], nil
	}

	l, err := SpawnLogicalQueue(LogicalConfig{
		MaxProduceBatchSize:  qm.conf.LogicalConfig.MaxProduceBatchSize,
		MaxLeaseBatchSize:    qm.conf.LogicalConfig.MaxLeaseBatchSize,
		MaxCompleteBatchSize: qm.conf.LogicalConfig.MaxCompleteBatchSize,
		MaxRequestsPerQueue:  qm.conf.LogicalConfig.MaxRequestsPerQueue,
		WriteTimeout:         qm.conf.LogicalConfig.WriteTimeout,
		ReadTimeout:          qm.conf.LogicalConfig.ReadTimeout,
		Clock:                qm.conf.LogicalConfig.Clock,
		Log:                  qm.conf.Log,
		StoragePartitions:    partitions,
		QueueInfo:            queue,
		Manager:              qm,
	})
	if err != nil {
		return nil, errors.Wrap(err)
	}
	qm.queues[queue.Name] = NewQueue(queue).AddLogical(l)
	return qm.queues[queue.Name], nil
}

func (qm *QueuesManager) List(ctx context.Context, items *[]types.QueueInfo, opts types.ListOptions) error {
	if qm.inShutdown.Load() {
		return ErrServiceShutdown
	}
	defer qm.mutex.RUnlock()
	qm.mutex.RLock()

	return qm.conf.StorageConfig.Queues.List(ctx, items, opts)
}

func (qm *QueuesManager) Update(ctx context.Context, update types.QueueUpdate) error {
	if qm.inShutdown.Load() {
		return ErrServiceShutdown
	}
	defer qm.mutex.Unlock()
	qm.mutex.Lock()

	var current types.QueueInfo
	if err := qm.conf.StorageConfig.Queues.Get(ctx, update.Name, &current); err != nil {
		if errors.Is(err, store.ErrQueueNotExist) {
			return err
		}
		return errors.Errorf("Queues.Get(): %w", err)
	}
	info := update.ApplyTo(current)

	if err := qm.validateDeadQueue(ctx, info); err != nil {
		return err
	}

	if err := qm.validateAsDeadQueue(ctx, info); err != nil {
		return err
	}

	info.UpdatedAt = qm.conf.LogicalConfig.Clock.Now().UTC()
	if err := qm.conf.StorageConfig.Queues.Update(ctx, info); err != nil {
		return errors.Errorf("Queues.Update(): %w", err)
	}

	q, ok := qm.queues[info.Name]
	if !ok {
		return nil
	}

	var stored types.QueueInfo
	if err := qm.conf.StorageConfig.Queues.Get(ctx, info.Name, &stored); err != nil {
		return errors.Errorf("Queues.Get(): %w", err)
	}
	q.UpdateInfo(stored)

	for _, l := range q.GetAll() {
		if err := l.UpdateInfo(ctx, stored); err != nil {
			return errors.Errorf("LogicalQueue.UpdateInfo(): %w", err)
		}
	}

	return nil
}

func (qm *QueuesManager) LifeCycle(ctx context.Context, req *types.LifeCycleRequest) error {
	queueInfo := req.PartitionInfo.Queue
	deadQueueName := queueInfo.DeadQueue

	var deadItems []*types.Item
	for _, action := range req.Actions {
		if action.Action == types.ActionItemExpired || action.Action == types.ActionItemMaxAttempts {
			deadItems = append(deadItems, &action.Item)
		}
	}

	if len(deadItems) == 0 {
		return nil
	}

	if req.PartitionStorage == nil {
		return errors.Errorf("PartitionStorage is required for lifecycle operations")
	}

	if deadQueueName == "" {
		for _, item := range deadItems {
			qm.log.Warn("item exceeded limits, deleting (no dead_queue configured)",
				"item_id", string(item.ID),
				"queue", queueInfo.Name,
				"partition", req.PartitionNum)
		}

		if err := deleteDeadItems(ctx, req, len(deadItems)); err != nil {
			return errors.Errorf("failed to delete items from source partition: %w", err)
		}
		return nil
	}

	for _, item := range deadItems {
		item.SourceID = item.ID
		item.ID = nil
		item.IsLeased = false
		item.LeaseDeadline = clock.Time{}
		item.Attempts = 0
	}

	req.HandOff = &types.ProduceRequest{Items: deadItems}
	stored, err := qm.ProduceToQueue(ctx, deadQueueName, req.HandOff)
	if errors.Is(err, ErrMoveUnanswered) {
		qm.log.Warn("dead queue has not answered the move; the source keeps the items until it does",
			"dead_queue", deadQueueName,
			"source_queue", queueInfo.Name,
			"items", len(deadItems))
		return err
	}
	return qm.settleMove(ctx, req, stored, err)
}

func (qm *QueuesManager) SettleAnsweredMove(ctx context.Context, req *types.LifeCycleRequest) error {
	err := req.HandOff.Err
	if err != nil {
		err = errors.Errorf("failed to produce to queue '%s': %w", req.PartitionInfo.Queue.DeadQueue, err)
	}
	return qm.settleMove(ctx, req, req.HandOff.Stored, err)
}

func (qm *QueuesManager) settleMove(ctx context.Context, req *types.LifeCycleRequest, stored int, err error) error {
	if err != nil {
		qm.log.Error("failed to produce to dead_queue",
			"dead_queue", req.PartitionInfo.Queue.DeadQueue,
			"source_queue", req.PartitionInfo.Queue.Name,
			"stored", stored,
			"error", err)
		if stored != 0 {
			if err := deleteDeadItems(ctx, req, stored); err != nil {
				return errors.Errorf("%w: %w", ErrMovedItemsKept, err)
			}
		}
		return err
	}

	if err := deleteDeadItems(ctx, req, len(req.HandOff.Items)); err != nil {
		return errors.Errorf("%w: %w", ErrMovedItemsKept, err)
	}

	return nil
}

func deleteDeadItems(ctx context.Context, req *types.LifeCycleRequest, limit int) error {
	actions := make([]types.Action, 0, limit)
	for _, a := range req.Actions {
		if len(actions) == limit {
			break
		}
		if a.Action == types.ActionItemExpired || a.Action == types.ActionItemMaxAttempts {
			a.Action = types.ActionDeleteItem
			actions = append(actions, a)
		}
	}
	var batch types.LifeCycleBatch
	batch.Add(&types.LifeCycleRequest{PartitionNum: req.PartitionNum, Actions: actions})
	return req.PartitionStorage.TakeAction(ctx, batch, req.PartitionState)
}

func (qm *QueuesManager) ProduceToQueue(ctx context.Context, queueName string, req *types.ProduceRequest) (int, error) {
	qm.mutex.Lock()
	q, err := qm.get(ctx, queueName)
	qm.mutex.Unlock()
	if err != nil {
		return 0, errors.Errorf("failed to get queue '%s': %w", queueName, err)
	}

	logical := q.GetNext()
	if logical == nil {
		return 0, errors.Errorf("queue '%s' has no available partitions", queueName)
	}
	stored, err := logical.ProduceDeadLetter(ctx, req)
	if err != nil {
		return stored, errors.Errorf("failed to produce to queue '%s': %w", queueName, err)
	}

	return stored, nil
}

func (qm *QueuesManager) validateDeadQueue(ctx context.Context, info types.QueueInfo) error {
	if info.DeadQueue == "" {
		return nil
	}

	const maxQueueNameLength = 500
	if len(info.DeadQueue) > maxQueueNameLength {
		return reply.NewInvalidOption("dead queue is invalid; cannot be greater than '%d' characters", maxQueueNameLength)
	}

	if strings.ContainsFunc(info.DeadQueue, unicode.IsSpace) {
		return reply.NewInvalidOption("dead queue is invalid; '%s' cannot contain whitespace", info.DeadQueue)
	}

	if strings.Contains(info.DeadQueue, "~") {
		return reply.NewInvalidOption("dead queue is invalid; '%s' cannot contain '~' character", info.DeadQueue)
	}

	if info.DeadQueue == info.Name {
		return reply.NewInvalidOption("dead_queue cannot reference itself")
	}

	dlq, err := qm.get(ctx, info.DeadQueue)
	if err != nil {
		var invalidOpt *reply.ErrInvalidOption
		if errors.Is(err, store.ErrQueueNotExist) || errors.As(err, &invalidOpt) {
			return reply.NewInvalidOption("dead_queue does not exist; '%s' was not found, create it first", info.DeadQueue)
		}
		return err
	}

	dlqInfo := dlq.Info()
	if dlqInfo.DeadQueue != "" {
		return reply.NewInvalidOption("dead_queue is invalid; '%s' already has its own dead_queue configured, "+
			"clear or change dead_queue on '%s' first", info.DeadQueue, info.DeadQueue)
	}

	if dlqInfo.RequestedPartitions > 1 {
		return reply.NewInvalidOption("dead_queue is invalid; '%s' must have a single partition, but has '%d'",
			info.DeadQueue, dlqInfo.RequestedPartitions)
	}

	return nil
}

func (qm *QueuesManager) validateAsDeadQueue(ctx context.Context, info types.QueueInfo) error {
	if info.DeadQueue == "" && info.RequestedPartitions <= 1 {
		return nil
	}

	source, err := qm.sourceQueueOf(ctx, info.Name)
	if err != nil {
		return err
	}
	if source == "" {
		return nil
	}

	if info.DeadQueue != "" {
		return reply.NewInvalidOption("dead_queue is invalid; '%s' is the dead_queue of '%s' and cannot have its own dead_queue, "+
			"clear or change dead_queue on '%s' first", info.Name, source, source)
	}

	return reply.NewInvalidOption("partitions is invalid; '%s' is the dead_queue of '%s' and must have a single partition, "+
		"clear or change dead_queue on '%s' first", info.Name, source, source)
}

func (qm *QueuesManager) sourceQueueOf(ctx context.Context, name string) (string, error) {
	const pageSize = 1000
	var pivot []byte
	for ctx.Err() == nil {
		var page []types.QueueInfo
		if err := qm.conf.StorageConfig.Queues.List(ctx, &page, types.ListOptions{Pivot: pivot, Limit: pageSize}); err != nil {
			return "", errors.Errorf("Queues.List(): %w", err)
		}

		for _, queue := range page {
			if queue.DeadQueue == name && queue.Name != name {
				return queue.Name, nil
			}
		}

		if len(page) < pageSize {
			return "", nil
		}
		pivot = []byte(page[len(page)-1].Name)
	}
	return "", ctx.Err()
}

func (qm *QueuesManager) Delete(ctx context.Context, name string) error {
	if qm.inShutdown.Load() {
		return ErrServiceShutdown
	}
	defer qm.mutex.Unlock()
	qm.mutex.Lock()

	source, err := qm.sourceQueueOf(ctx, name)
	if err != nil {
		return err
	}
	if source != "" {
		return reply.NewInvalidOption("queue '%s' cannot be deleted; it is the dead_queue of '%s', "+
			"clear or change dead_queue on '%s' first", name, source, source)
	}

	if err := qm.conf.StorageConfig.Queues.Delete(ctx, name); err != nil {
		return errors.Errorf("Queues.Delete(): %w", err)
	}

	q, ok := qm.queues[name]
	if !ok {
		return nil
	}

	for _, l := range q.GetAll() {
		if err := l.Shutdown(ctx); err != nil {
			return errors.Errorf("LogicalQueue.Shutdown(): %w", err)
		}
	}

	delete(qm.queues, name)
	return nil
}

func (qm *QueuesManager) Shutdown(ctx context.Context) error {
	if qm.inShutdown.Load() {
		return nil
	}

	qm.inShutdown.Store(true)
	qm.mutex.Lock()
	queues := make([]*Queue, 0, len(qm.queues))
	for _, name := range slices.Sorted(maps.Keys(qm.queues)) {
		queues = append(queues, qm.queues[name])
	}
	qm.mutex.Unlock()

	var closing sync.WaitGroup
	var errs []error
	done := make(chan struct{})
	closing.Go(func() {
		defer close(done)
		for _, q := range queues {
			qm.log.LogAttrs(ctx, LevelDebugAll, "shutdown logical",
				slog.Int("num_queues", len(queues)),
				slog.String("queue", q.Info().Name))
			for _, l := range q.GetAll() {
				if err := l.Shutdown(ctx); err != nil {
					errs = append(errs, err)
				}
			}
		}
		qm.log.LogAttrs(ctx, LevelDebugAll, "close queue store")
		if err := qm.conf.StorageConfig.Queues.Close(ctx); err != nil {
			errs = append(errs, err)
		}
		if qm.conf.StorageConfig.Namespaces != nil {
			qm.log.LogAttrs(ctx, LevelDebugAll, "close namespace store")
			if err := qm.conf.StorageConfig.Namespaces.Close(ctx); err != nil {
				errs = append(errs, err)
			}
		}
		if qm.conf.StorageConfig.Users != nil {
			qm.log.LogAttrs(ctx, LevelDebugAll, "close users store")
			if err := qm.conf.StorageConfig.Users.Close(ctx); err != nil {
				errs = append(errs, err)
			}
		}
		if qm.conf.StorageConfig.APIKeys != nil {
			qm.log.LogAttrs(ctx, LevelDebugAll, "close apikeys store")
			if err := qm.conf.StorageConfig.APIKeys.Close(ctx); err != nil {
				errs = append(errs, err)
			}
		}
		if qm.conf.StorageConfig.Roles != nil {
			qm.log.LogAttrs(ctx, LevelDebugAll, "close roles store")
			if err := qm.conf.StorageConfig.Roles.Close(ctx); err != nil {
				errs = append(errs, err)
			}
		}
		if qm.conf.StorageConfig.RoleBindings != nil {
			qm.log.LogAttrs(ctx, LevelDebugAll, "close rolebindings store")
			if err := qm.conf.StorageConfig.RoleBindings.Close(ctx); err != nil {
				errs = append(errs, err)
			}
		}
	})

	select {
	case <-ctx.Done():
		qm.log.Warn("ctx cancelled while waiting for shutdown")
		return errors.Wrap(ctx.Err())
	case <-done:
		closing.Wait()
		qm.log.LogAttrs(ctx, LevelDebugAll, "Shutdown complete")
		if len(errs) > 0 {
			return errors.Wrap(errs[0])
		}
		return nil
	}
}
