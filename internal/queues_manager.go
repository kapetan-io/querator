package internal

import (
	"context"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/kapetan-io/errors"
	"github.com/kapetan-io/querator/internal/placement"
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
	backends   *StorageBackends
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

	if err := placement.CheckBackends(conf.StorageConfig.Backends()); err != nil {
		return nil, err
	}

	qm := &QueuesManager{
		backends: NewStorageBackends(conf.StorageConfig.Queues, conf.StorageConfig.PartitionStorage, conf.Log),
		log:      conf.Log.With("code.namespace", "QueuesManager"),
		queues:   make(map[string]*Queue),
		conf:     conf,
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

	backends, err := qm.backends.Placeable(ctx)
	if err != nil {
		return nil, err
	}
	if !placement.CanPlace(backends) {
		return nil, reply.NewRequestFailed("no storage backend can take partitions; " +
			"at least one storage backend needs a saved affinity above 0, see '/v1/storage/backends.update'")
	}

	if err := qm.deadLetterRules().Check(ctx, info); err != nil {
		return nil, err
	}

	if info.RequestedPartitions > placement.MaxPartitions {
		return nil, reply.NewInvalidOption("partitions is invalid; cannot be greater than %d", placement.MaxPartitions)
	}
	info, err = placement.Create(info, backends)
	if err != nil {
		return nil, err
	}
	for _, p := range info.PartitionInfo {
		qm.log.LogAttrs(ctx, LevelDebugAll, "Partition Assigned",
			slog.String("queue", info.Name), slog.String("storage", p.StorageName),
			slog.Int("partition", p.PartitionNum))
	}

	info.CreatedAt = qm.conf.LogicalConfig.Clock.Now().UTC()
	info.UpdatedAt = qm.conf.LogicalConfig.Clock.Now().UTC()
	info.LinkPartitions()
	if err := qm.conf.StorageConfig.Queues.Add(ctx, info); err != nil {
		return nil, errors.With("queue", info.Name).
			Errorf("Queues.Add(): %w", err)
	}

	return qm.get(ctx, info.Name)
}

func (qm *QueuesManager) SaveFirstSeenBackends(ctx context.Context) error {
	return qm.backends.SaveFirstSeen(ctx)
}

func (qm *QueuesManager) UpdateStorageBackend(ctx context.Context, update BackendUpdate) error {
	if qm.inShutdown.Load() {
		return ErrServiceShutdown
	}
	return qm.backends.Update(ctx, update)
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

	queue.LinkPartitions()
	var partitions []store.Partition
	for _, info := range queue.PartitionInfo {
		partitions = append(partitions, qm.openPartition(info))
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
		OpenPartition:        qm.openPartition,
		QueueInfo:            queue,
		Manager:              qm,
	})
	if err != nil {
		return nil, errors.Wrap(err)
	}
	qm.queues[queue.Name] = NewQueue(queue).AddLogical(l)
	return qm.queues[queue.Name], nil
}

func (qm *QueuesManager) openPartition(info types.PartitionInfo) store.Partition {
	b := store.Find(info.StorageName, qm.conf.StorageConfig.PartitionStorage)
	if b.Name == "" {
		qm.log.Warn("partition is on a storage backend missing from the config; the partition is failing",
			"queue", info.Queue.Name, "partition", info.PartitionNum, "storage_name", info.StorageName)
		return store.NewUnconfiguredPartition(info)
	}
	return b.PartitionStore.Get(info)
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
	stored, err := qm.saveUpdate(ctx, update)
	if err != nil || stored == nil {
		return err
	}
	return qm.adoptRecord(ctx, *stored)
}

func (qm *QueuesManager) saveUpdate(ctx context.Context, update types.QueueUpdate) (*types.QueueInfo, error) {
	defer qm.mutex.Unlock()
	qm.mutex.Lock()

	var current types.QueueInfo
	if err := qm.conf.StorageConfig.Queues.Get(ctx, update.Name, &current); err != nil {
		if errors.Is(err, store.ErrQueueNotExist) {
			return nil, err
		}
		return nil, errors.Errorf("Queues.Get(): %w", err)
	}
	info := update.ApplyTo(current)

	if err := qm.deadLetterRules().Check(ctx, info); err != nil {
		return nil, err
	}

	info.UpdatedAt = qm.conf.LogicalConfig.Clock.Now().UTC()
	if err := qm.conf.StorageConfig.Queues.Update(ctx, info); err != nil {
		if errors.Is(err, store.ErrQueueChanged) {
			return nil, reply.NewRetryRequest("queue '%s' changed while it was being updated; try again", info.Name)
		}
		return nil, errors.Errorf("Queues.Update(): %w", err)
	}

	if _, ok := qm.queues[info.Name]; !ok {
		return nil, nil
	}

	var stored types.QueueInfo
	if err := qm.conf.StorageConfig.Queues.Get(ctx, info.Name, &stored); err != nil {
		return nil, errors.Errorf("Queues.Get(): %w", err)
	}
	return &stored, nil
}

func (qm *QueuesManager) adoptRecord(ctx context.Context, stored types.QueueInfo) error {
	qm.mutex.Lock()
	q, ok := qm.queues[stored.Name]
	if ok && len(q.GetAll()) == 0 && len(stored.PartitionInfo) != 0 {
		delete(qm.queues, stored.Name)
		ok = false
	}
	qm.mutex.Unlock()

	if !ok {
		return nil
	}
	held := q.Info()
	if held.CreationID != stored.CreationID {
		return nil
	}
	if held.Revision >= stored.Revision {
		return nil
	}

	stored.LinkPartitions()
	q.RoutePartitions(stored)
	saved, cancel := context.WithTimeout(context.WithoutCancel(ctx), qm.conf.LogicalConfig.WriteTimeout)
	defer cancel()
	for _, l := range q.GetAll() {
		if err := l.UpdateInfo(saved, stored); err != nil {
			return errors.With("queue", stored.Name).Errorf("while the queue adopts its saved record: %w", err)
		}
	}
	q.AdoptRevision(stored)
	return nil
}

type RebalanceCounts struct {
	ChangedQueues      int
	CreatedPartitions  int
	DrainingPartitions int
}

const maxRebalanceAttempts = 10

func (qm *QueuesManager) Rebalance(ctx context.Context) (RebalanceCounts, error) {
	var result RebalanceCounts
	if qm.inShutdown.Load() {
		return result, ErrServiceShutdown
	}

	backends, err := qm.rebalanceBackends(ctx)
	if err != nil {
		return result, err
	}

	var names []string
	if err := store.ForEachQueue(ctx, qm.conf.StorageConfig.Queues, func(queue types.QueueInfo) bool {
		names = append(names, queue.Name)
		return true
	}); err != nil {
		return result, err
	}

	for _, name := range names {
		if err := qm.rebalanceQueue(ctx, name, backends, &result); err != nil {
			return result, err
		}
	}
	return result, nil
}

func (qm *QueuesManager) rebalanceBackends(ctx context.Context) ([]types.Backend, error) {
	backends, err := qm.backends.List(ctx)
	if err != nil {
		return nil, err
	}
	for _, backend := range backends {
		if backend.Affinity > 0 && !qm.backends.Configured(backend.Name) {
			return nil, reply.NewInvalidOption("rebalance is refused; storage backend '%s' has affinity %v "+
				"and is missing from this instance's config file; add it back or save affinity 0 for it", backend.Name, backend.Affinity)
		}
	}
	if !placement.CanPlace(backends) {
		return nil, reply.NewInvalidOption("rebalance is refused; no storage backend can take partitions, " +
			"at least one storage backend needs a saved affinity above 0, see '/v1/storage/backends.update'")
	}
	return backends, nil
}

func (qm *QueuesManager) rebalanceQueue(ctx context.Context, name string, backends []types.Backend,
	result *RebalanceCounts) error {
	for range maxRebalanceAttempts {
		var queue types.QueueInfo
		if err := qm.conf.StorageConfig.Queues.Get(ctx, name, &queue); err != nil {
			if errors.Is(err, store.ErrQueueNotExist) {
				return nil
			}
			return errors.With("queue", name).Errorf("Queues.Get(): %w", err)
		}

		rebalanced, err := placement.Rebalance(queue, backends)
		if err != nil {
			return errors.With("queue", name).Errorf("placement.Rebalance(): %w", err)
		}
		if !rebalanced.Changed {
			return qm.adoptRecord(ctx, queue)
		}

		err = qm.conf.StorageConfig.Queues.SavePartitions(ctx, types.PartitionList{
			UpdatedAt:     qm.conf.LogicalConfig.Clock.Now().UTC(),
			PartitionInfo: rebalanced.Queue.PartitionInfo,
			NextPartition: rebalanced.Queue.NextPartition,
			Revision:      queue.Revision,
			CreationID:    queue.CreationID,
			QueueName:     name,
		})
		if errors.Is(err, store.ErrQueueNotExist) {
			return nil
		}
		if errors.Is(err, store.ErrQueueChanged) {
			continue
		}
		if err != nil {
			return errors.With("queue", name).Errorf("Queues.SavePartitions(): %w", err)
		}

		result.ChangedQueues++
		result.CreatedPartitions += len(rebalanced.Queue.PartitionInfo) - len(queue.PartitionInfo)
		result.DrainingPartitions += drainingCount(rebalanced.Queue) - drainingCount(queue)

		var stored types.QueueInfo
		if err := qm.conf.StorageConfig.Queues.Get(ctx, name, &stored); err != nil {
			if errors.Is(err, store.ErrQueueNotExist) {
				return nil
			}
			return errors.With("queue", name).Errorf("Queues.Get(): %w", err)
		}
		return qm.adoptRecord(ctx, stored)
	}
	return reply.NewRetryRequest("queue '%s' kept changing while it was being rebalanced; try again", name)
}

func drainingCount(queue types.QueueInfo) int {
	var count int
	for _, p := range queue.PartitionInfo {
		if p.Draining {
			count++
		}
	}
	return count
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

func (qm *QueuesManager) deadLetterRules() DeadLetterRules {
	return DeadLetterRules{
		Find: func(ctx context.Context, name string) (types.QueueInfo, error) {
			q, err := qm.get(ctx, name)
			if err != nil {
				var invalid *reply.ErrInvalidOption
				if errors.As(err, &invalid) {
					return types.QueueInfo{}, store.ErrQueueNotExist
				}
				return types.QueueInfo{}, err
			}
			return q.Info(), nil
		},
		SourceOf: qm.sourceQueueOf,
	}
}

func (qm *QueuesManager) sourceQueueOf(ctx context.Context, name string) (string, error) {
	var source string
	err := store.ForEachQueue(ctx, qm.conf.StorageConfig.Queues, func(queue types.QueueInfo) bool {
		if queue.DeadQueue == name && queue.Name != name {
			source = queue.Name
			return false
		}
		return true
	})
	return source, err
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
