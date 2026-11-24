/*
Copyright 2024 Derrick J. Wippler

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/internal"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/querator/transport/auth"
	"github.com/kapetan-io/querator/transport/reply"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/set"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	DefaultListLimit = 1_000
)

type Config struct {
	Log                   *slog.Logger
	StorageConfig         store.Config
	Auth                  auth.AuthBackend
	InstanceID            string
	WriteTimeout          clock.Duration
	ReadTimeout           clock.Duration
	MaxLeaseBatchSize     int
	MaxProduceBatchSize   int
	MaxCompleteBatchSize  int
	MaxRequestsPerQueue   int
	MaxConcurrentRequests int
	Clock                 *clock.Provider
}

type Service struct {
	queues *internal.QueuesManager
	conf   Config
}

func New(ctx context.Context, conf Config) (*Service, error) {
	set.Default(&conf.Clock, clock.NewProvider())
	set.Default(&conf.Log, slog.Default())
	set.Default(&conf.ReadTimeout, 3*time.Second)
	set.Default(&conf.WriteTimeout, 3*time.Second)

	qm, err := internal.NewQueuesManager(internal.QueuesManagerConfig{
		LogicalConfig: internal.LogicalConfig{
			MaxLeaseBatchSize:    conf.MaxLeaseBatchSize,
			MaxProduceBatchSize:  conf.MaxProduceBatchSize,
			MaxCompleteBatchSize: conf.MaxCompleteBatchSize,
			MaxRequestsPerQueue:  conf.MaxRequestsPerQueue,
			WriteTimeout:         conf.WriteTimeout,
			ReadTimeout:          conf.ReadTimeout,
			Clock:                conf.Clock,
		},
		StorageConfig: conf.StorageConfig,
		Log:           conf.Log,
	})
	if err != nil {
		return nil, err
	}

	set.Default(&conf.Auth, auth.AuthBackend(&auth.NoOpAuthBackend{}))

	s := &Service{
		queues: qm,
		conf:   conf,
	}

	if err := s.Bootstrap(ctx); err != nil {
		return nil, err
	}

	if err := qm.SaveFirstSeenBackends(ctx); err != nil {
		conf.Log.Error("cannot save storage backend records; will retry on the next queue create "+
			"or storage backend request", "error", err)
	}
	qm.StartLoadingDrainingQueues()

	return s, nil
}

type namespacePermission struct {
	Namespace  string
	Permission string
}

func (s *Service) authorize(ctx context.Context, required namespacePermission) error {
	principal := auth.PrincipalFromContext(ctx)
	hasPermission, err := s.conf.Auth.HasPermission(ctx, principal, auth.Access(required))
	if err != nil {
		s.conf.Log.Error("authorization check failed", "error", err)
		return reply.NewRequestFailed("authorization check failed")
	}
	if !hasPermission {
		return reply.NewForbidden("access denied")
	}
	return nil
}

func resolveNamespace(ns string) string {
	if ns == "" {
		return auth.SystemNamespace
	}
	return ns
}

func (s *Service) QueueProduce(ctx context.Context, req *proto.QueueProduceRequest) error {
	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	if err := s.authorize(ctx, namespacePermission{Namespace: queue.Info().Namespace, Permission: auth.QueueProduce}); err != nil {
		return err
	}

	logical := queue.GetNext()
	if logical == nil {
		return reply.NewRequestFailed("queue has no available partitions")
	}

	var r types.ProduceRequest
	if err := s.validateQueueProduceProto(req, &r); err != nil {
		return err
	}

	if err := logical.Produce(ctx, &r); err != nil {
		return err
	}

	return nil
}

func (s *Service) QueueLease(ctx context.Context, req *proto.QueueLeaseRequest,
	res *proto.QueueLeaseResponse) error {

	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	if err := s.authorize(ctx, namespacePermission{Namespace: queue.Info().Namespace, Permission: auth.QueueLease}); err != nil {
		return err
	}

	logical := queue.GetNext()
	if logical == nil {
		return reply.NewRequestFailed("queue has no available partitions")
	}

	var r types.LeaseRequest
	if err := s.validateQueueLeaseProto(req, &r); err != nil {
		return err
	}

	if err := logical.Lease(ctx, &r); err != nil {
		return err
	}

	res.Partition = int32(r.Partition)
	res.QueueName = req.QueueName

	for _, item := range r.Items {
		res.Items = append(res.Items, &proto.QueueLeaseItem{
			LeaseDeadline: timestamppb.New(item.LeaseDeadline),
			Attempts:      int32(item.Attempts),
			Id:            string(item.ID),
			Reference:     item.Reference,
			Encoding:      item.Encoding,
			Bytes:         item.Payload,
			Kind:          item.Kind,
		})
	}

	return nil
}

func (s *Service) QueueComplete(ctx context.Context, req *proto.QueueCompleteRequest) error {
	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	if err := s.authorize(ctx, namespacePermission{Namespace: queue.Info().Namespace, Permission: auth.QueueComplete}); err != nil {
		return err
	}

	logical, err := queue.GetByPartition(int(req.Partition))
	if err != nil {
		return err
	}

	var r types.CompleteRequest
	if err := s.validateQueueCompleteProto(req, &r); err != nil {
		return err
	}

	if err := logical.Complete(ctx, &r); err != nil {
		return err
	}

	return nil
}

func (s *Service) QueueRetry(ctx context.Context, req *proto.QueueRetryRequest) error {
	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	if err := s.authorize(ctx, namespacePermission{Namespace: queue.Info().Namespace, Permission: auth.QueueRetry}); err != nil {
		return err
	}

	logical, err := queue.GetByPartition(int(req.Partition))
	if err != nil {
		return err
	}

	var r types.RetryRequest
	if err := s.validateQueueRetryProto(req, &r); err != nil {
		return err
	}

	if err := logical.Retry(ctx, &r); err != nil {
		return err
	}

	return nil
}

func (s *Service) QueueReload(ctx context.Context, req *proto.QueueReloadRequest) error {
	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	if err := s.authorize(ctx, namespacePermission{Namespace: queue.Info().Namespace, Permission: auth.QueueUpdate}); err != nil {
		return err
	}

	for _, logical := range queue.GetAll() {
		r := types.ReloadRequest{
			Partitions: make([]int, 0),
		}

		if err := logical.ReloadPartitions(ctx, &r); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) QueueClear(ctx context.Context, req *proto.QueueClearRequest) error {
	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	if err := s.authorize(ctx, namespacePermission{Namespace: queue.Info().Namespace, Permission: auth.QueueClear}); err != nil {
		return err
	}

	for _, logical := range queue.GetAll() {
		r := types.ClearRequest{
			Destructive: req.Destructive,
			Scheduled:   req.Scheduled,
			Queue:       req.Queue,
		}

		if err := logical.Clear(ctx, &r); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) PauseQueue(ctx context.Context, queueName string) error {
	return s.applyPause(ctx, queueName, &types.PauseRequest{Pause: true})
}

func (s *Service) ResumeQueue(ctx context.Context, queueName string) error {
	return s.applyPause(ctx, queueName, &types.PauseRequest{Pause: false})
}

func (s *Service) applyPause(ctx context.Context, queueName string, req *types.PauseRequest) error {
	queue, err := s.queues.Get(ctx, queueName)
	if err != nil {
		return err
	}

	for _, logical := range queue.GetAll() {
		if err := logical.Pause(ctx, req); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) QueuesCreate(ctx context.Context, req *proto.QueueInfo) error {
	if err := validateQueueName(req.QueueName); err != nil {
		return err
	}

	var info types.QueueInfo

	if err := s.validateQueueOptionsProto(req, &info); err != nil {
		return err
	}

	info.Namespace = resolveNamespace(info.Namespace)
	if err := s.authorize(ctx, namespacePermission{Namespace: info.Namespace, Permission: auth.QueueCreate}); err != nil {
		return err
	}

	if s.conf.StorageConfig.Namespaces != nil {
		var namespace types.Namespace
		if err := s.conf.StorageConfig.Namespaces.Get(ctx, info.Namespace, &namespace); err != nil {
			return err
		}
	}

	_, err := s.queues.Create(ctx, info)
	if err != nil {
		return err
	}

	return nil
}

func (s *Service) QueuesList(ctx context.Context, req *proto.QueuesListRequest,
	resp *proto.QueuesListResponse) error {

	ns := resolveNamespace(req.Namespace)
	if err := s.authorize(ctx, namespacePermission{Namespace: ns, Permission: auth.QueueList}); err != nil {
		return err
	}

	if req.Limit == 0 {
		req.Limit = DefaultListLimit
	}

	items := make([]types.QueueInfo, 0, allocInt32(req.Limit))
	if err := s.queues.List(ctx, &items, types.ListOptions{
		Pivot:     types.ToItemID(req.Pivot),
		Limit:     int(req.Limit),
		Namespace: ns,
	}); err != nil {
		return err
	}

	pending, err := s.queues.PendingRebalances(ctx, items)
	if err != nil {
		return err
	}
	for i, item := range items {
		resp.Items = append(resp.Items, queueInfoWithPending(item, pending[i]))
	}
	return nil
}

func queueInfoWithPending(info types.QueueInfo, rebalance internal.QueuePendingRebalance) *proto.QueueInfo {
	out := info.ToProto(new(proto.QueueInfo))
	out.RebalanceRefused = rebalance.Refused
	if len(rebalance.Create) == 0 && len(rebalance.Drain) == 0 {
		return out
	}
	out.Pending = &proto.QueuePendingRebalance{}
	for _, p := range rebalance.Create {
		out.Pending.Create = append(out.Pending.Create, p.ToProto())
	}
	for _, p := range rebalance.Drain {
		out.Pending.Drain = append(out.Pending.Drain, p.ToProto())
	}
	return out
}

func (s *Service) QueuesUpdate(ctx context.Context, req *proto.QueuesUpdateRequest) error {
	if err := validateQueueName(req.QueueName); err != nil {
		return err
	}

	var update types.QueueUpdate
	if err := s.validateQueueUpdateProto(req, &update); err != nil {
		return err
	}

	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	if err := s.authorize(ctx, namespacePermission{Namespace: queue.Info().Namespace, Permission: auth.QueueUpdate}); err != nil {
		return err
	}

	if err := s.queues.Update(ctx, update); err != nil {
		return err
	}
	return nil
}

func (s *Service) QueuesDelete(ctx context.Context, req *proto.QueuesDeleteRequest) error {
	if err := validateQueueName(req.QueueName); err != nil {
		return err
	}

	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	if err := s.authorize(ctx, namespacePermission{Namespace: queue.Info().Namespace, Permission: auth.QueueDelete}); err != nil {
		return err
	}

	if err := s.queues.Delete(ctx, req.QueueName); err != nil {
		return err
	}
	return nil
}

func (s *Service) QueuesInfo(ctx context.Context, req *proto.QueuesInfoRequest, resp *proto.QueueInfo) error {
	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	if err := s.authorize(ctx, namespacePermission{Namespace: queue.Info().Namespace, Permission: auth.QueueStats}); err != nil {
		return err
	}

	info := queue.Info()
	pending, err := s.queues.PendingRebalance(ctx, req.QueueName)
	if err != nil {
		return err
	}
	*resp = *queueInfoWithPending(info, pending)
	return nil
}

func (s *Service) StorageItemsList(ctx context.Context, req *proto.StorageItemsListRequest,
	res *proto.StorageItemsListResponse) error {
	return s.storageItemsList(ctx, types.ListItems, req, res)
}

func (s *Service) StorageScheduledList(ctx context.Context, req *proto.StorageItemsListRequest,
	res *proto.StorageItemsListResponse) error {
	return s.storageItemsList(ctx, types.ListScheduled, req, res)
}

func (s *Service) storageItemsList(ctx context.Context, kind types.ListKind,
	req *proto.StorageItemsListRequest,
	res *proto.StorageItemsListResponse) error {
	if req.Limit == 0 {
		req.Limit = DefaultListLimit
	}

	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	if err := s.authorize(ctx, namespacePermission{Namespace: queue.Info().Namespace, Permission: auth.QueueStats}); err != nil {
		return err
	}

	logical, err := queue.GetByPartition(int(req.Partition))
	if err != nil {
		return err
	}

	items := make([]*types.Item, 0, allocInt32(req.Limit))

	if err := logical.StorageItemsList(ctx, internal.StorageRequest{
		Partition: int(req.Partition),
		Options: types.ListOptions{
			Pivot: types.ToItemID(req.Pivot),
			Limit: int(req.Limit),
		},
		Items:    &items,
		ListKind: kind,
	}); err != nil {
		return err
	}

	for _, item := range items {
		res.Items = append(res.Items, item.ToProto(new(proto.StorageItem)))
	}

	return nil
}

func (s *Service) StorageItemsImport(ctx context.Context, req *proto.StorageItemsImportRequest,
	res *proto.StorageItemsImportResponse) error {

	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	if err := s.authorize(ctx, namespacePermission{Namespace: queue.Info().Namespace, Permission: auth.QueueProduce}); err != nil {
		return err
	}

	logical, err := queue.GetByPartition(int(req.Partition))
	if err != nil {
		return err
	}

	items := make([]*types.Item, 0, len(req.Items))
	for _, item := range req.Items {
		i := new(types.Item)
		items = append(items, i.FromProto(item))
	}

	if err := logical.StorageItemsImport(ctx, int(req.Partition), &items); err != nil {
		return err
	}

	for _, item := range items {
		res.Items = append(res.Items, item.ToProto(new(proto.StorageItem)))
	}

	return nil
}

func (s *Service) StorageItemsDelete(ctx context.Context, req *proto.StorageItemsDeleteRequest) error {

	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	if err := s.authorize(ctx, namespacePermission{Namespace: queue.Info().Namespace, Permission: auth.QueueClear}); err != nil {
		return err
	}

	logical, err := queue.GetByPartition(int(req.Partition))
	if err != nil {
		return err
	}

	ids := make([]types.ItemID, 0, len(req.Ids))
	for _, id := range req.Ids {
		ids = append(ids, types.ItemID(id))
	}

	if err := logical.StorageItemsDelete(ctx, int(req.Partition), ids); err != nil {
		return err
	}
	return nil
}

func (s *Service) QueueStats(ctx context.Context, req *proto.QueueStatsRequest,
	res *proto.QueueStatsResponse) error {

	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	if err := s.authorize(ctx, namespacePermission{Namespace: queue.Info().Namespace, Permission: auth.QueueStats}); err != nil {
		return err
	}

	res.QueueName = req.QueueName
	for _, logical := range queue.GetAll() {
		var stats types.LogicalStats
		if err := logical.QueueStats(ctx, &stats); err != nil {
			return err
		}

		ls := &proto.QueueLogicalStats{
			ProduceWaiting:  int32(stats.ProduceWaiting),
			LeaseWaiting:    int32(stats.LeaseWaiting),
			CompleteWaiting: int32(stats.CompleteWaiting),
			InFlight:        int32(stats.InFlight),
			Partitions:      nil,
		}

		for _, stat := range stats.Partitions {
			ls.Partitions = append(ls.Partitions,
				&proto.QueuePartitionStats{
					AverageLeasedAge: stat.AverageLeasedAge.String(),
					Partition:        int32(stat.Partition),
					TotalLeased:      int32(stat.NumLeased),
					Failures:         int32(stat.Failures),
					AverageAge:       stat.AverageAge.String(),
					Total:            int32(stat.Total),
					Scheduled:        int32(stat.Scheduled),
					UnLeased:         int32(stat.UnLeased),
					NextLifecycleRun: stat.NextLifecycleRun.String(),
				})
		}
		res.LogicalQueues = append(res.LogicalQueues, ls)
	}
	return nil
}

func (s *Service) QueuesRebalance(ctx context.Context, _ *proto.QueuesRebalanceRequest,
	resp *proto.QueuesRebalanceResponse) error {
	if err := s.authorize(ctx, namespacePermission{Namespace: auth.SystemNamespace, Permission: auth.QueueRebalance}); err != nil {
		return err
	}

	rebalanced, err := s.queues.Rebalance(ctx)
	if err != nil {
		return err
	}
	resp.QueuesChanged = new(int32(rebalanced.ChangedQueues))
	resp.PartitionsCreated = new(int32(rebalanced.CreatedPartitions))
	resp.PartitionsDraining = new(int32(rebalanced.DrainingPartitions))
	resp.WaitingQueues = make([]*proto.WaitingQueue, 0, len(rebalanced.WaitingQueues))
	for _, waiting := range rebalanced.WaitingQueues {
		resp.WaitingQueues = append(resp.WaitingQueues, &proto.WaitingQueue{
			Namespace: waiting.Namespace,
			QueueName: waiting.QueueName,
			Backend:   waiting.Backend,
			Partition: new(int32(waiting.Partition)),
		})
	}
	return nil
}

func (s *Service) StorageBackendsUpdate(ctx context.Context, req *proto.StorageBackendsUpdateRequest) error {
	if err := s.authorize(ctx, namespacePermission{Namespace: auth.SystemNamespace, Permission: auth.BackendUpdate}); err != nil {
		return err
	}

	if strings.TrimSpace(req.Backend) == "" {
		return reply.NewInvalidOption("backend is invalid; cannot be empty")
	}

	return s.queues.UpdateStorageBackend(ctx, internal.BackendUpdate{Name: req.Backend, Affinity: req.Affinity})
}

func (s *Service) StorageBackendsList(ctx context.Context, _ *proto.StorageBackendsListRequest,
	resp *proto.StorageBackendsListResponse) error {
	if err := s.authorize(ctx, namespacePermission{Namespace: auth.SystemNamespace, Permission: auth.BackendList}); err != nil {
		return err
	}

	statuses, err := s.queues.ListStorageBackends(ctx)
	if err != nil {
		return err
	}

	for _, status := range statuses {
		backend := &proto.StorageBackend{
			Backend:    status.Backend.Name,
			Configured: new(status.Configured),
			Pending:    &proto.PendingRebalance{},
		}
		if status.Recorded {
			backend.Affinity = new(status.Backend.Affinity)
		}
		for _, p := range status.Partitions {
			partition := &proto.BackendPartition{
				QueueName: p.Queue,
				Partition: new(int32(p.Partition)),
				State:     p.State,
				Failures:  new(int32(p.Counts.Failures)),
			}
			if p.Readable {
				partition.Unleased = new(int32(p.Counts.UnLeased))
				partition.Leased = new(int32(p.Counts.NumLeased))
				partition.Scheduled = new(int32(p.Counts.Scheduled))
			}
			backend.Partitions = append(backend.Partitions, partition)
		}
		backend.Pending.Create = queuePartitionsToProto(status.Rebalance.Create)
		backend.Pending.Drain = queuePartitionsToProto(status.Rebalance.Drain)
		resp.Backends = append(resp.Backends, backend)
	}
	return nil
}

func queuePartitionsToProto(in []internal.QueuePartitions) []*proto.QueuePartitions {
	out := make([]*proto.QueuePartitions, 0, len(in))
	for _, queue := range in {
		numbers := make([]int32, 0, len(queue.Partitions))
		for _, n := range queue.Partitions {
			numbers = append(numbers, int32(n))
		}
		out = append(out, &proto.QueuePartitions{QueueName: queue.Queue, Partitions: numbers})
	}
	return out
}

func (s *Service) Health(ctx context.Context) (*transport.HealthResponse, error) {
	const healthTimeout = 5 * time.Second

	healthCtx, cancel := context.WithTimeout(ctx, healthTimeout)
	defer cancel()

	response := &transport.HealthResponse{
		Status:  transport.HealthStatusPass,
		Version: querator.Version,
		Checks:  make(map[string][]transport.Check),
	}

	var queues []types.QueueInfo
	err := s.queues.List(healthCtx, &queues, types.ListOptions{Limit: 1})

	check := transport.Check{
		ComponentType: "datastore",
		Time:          time.Now().UTC().Format(time.RFC3339),
	}

	if err != nil {
		s.conf.Log.Error("health check storage probe failed", "error", err)
		check.Status = transport.HealthStatusFail
		check.Output = "storage check failed"
		response.Status = transport.HealthStatusFail
	} else {
		check.Status = transport.HealthStatusPass
	}

	response.Checks["queues:storage"] = []transport.Check{check}

	return response, nil
}

func (s *Service) Shutdown(ctx context.Context) error {
	s.conf.Auth.Close()
	return s.queues.Shutdown(ctx)
}

func (s *Service) NamespacesCreate(ctx context.Context, req *proto.NamespaceInfo) error {
	if s.conf.StorageConfig.Namespaces == nil {
		return reply.NewRequestFailed("namespace storage not configured")
	}

	var ns types.Namespace

	if err := s.validateNamespaceProto(req, &ns); err != nil {
		return err
	}

	if err := s.authorize(ctx, namespacePermission{Namespace: auth.SystemNamespace, Permission: auth.NamespaceCreate}); err != nil {
		return err
	}

	if ns.IsReserved() {
		return types.NewErrNamespaceReserved(ns.Name)
	}

	ns.CreatedAt = s.conf.Clock.Now().UTC()
	if err := s.conf.StorageConfig.Namespaces.Add(ctx, ns); err != nil {
		return err
	}

	return nil
}

func (s *Service) NamespacesUpdate(ctx context.Context, req *proto.NamespaceInfo) error {
	if s.conf.StorageConfig.Namespaces == nil {
		return reply.NewRequestFailed("namespace storage not configured")
	}

	var ns types.Namespace

	if err := s.validateNamespaceProto(req, &ns); err != nil {
		return err
	}

	if err := s.authorize(ctx, namespacePermission{Namespace: auth.SystemNamespace, Permission: auth.NamespaceUpdate}); err != nil {
		return err
	}

	if ns.IsReserved() {
		return types.NewErrNamespaceReserved(ns.Name)
	}

	return s.conf.StorageConfig.Namespaces.Update(ctx, ns)
}

func (s *Service) NamespacesList(ctx context.Context, req *proto.NamespacesListRequest,
	resp *proto.NamespacesListResponse) error {
	if s.conf.StorageConfig.Namespaces == nil {
		return reply.NewRequestFailed("namespace storage not configured")
	}

	if req.Limit == 0 {
		req.Limit = DefaultListLimit
	}

	if err := s.authorize(ctx, namespacePermission{Namespace: auth.SystemNamespace, Permission: auth.NamespaceList}); err != nil {
		return err
	}

	namespaces := make([]types.Namespace, 0, allocInt32(req.Limit))
	if err := s.conf.StorageConfig.Namespaces.List(ctx, &namespaces, types.ListOptions{
		Pivot: types.ToItemID(req.Pivot),
		Limit: int(req.Limit),
	}); err != nil {
		return err
	}

	for _, ns := range namespaces {
		resp.Items = append(resp.Items, ns.ToProto(new(proto.NamespaceInfo)))
	}
	return nil
}

func (s *Service) NamespacesDelete(ctx context.Context, req *proto.NamespacesDeleteRequest) error {
	if s.conf.StorageConfig.Namespaces == nil {
		return reply.NewRequestFailed("namespace storage not configured")
	}
	if s.conf.StorageConfig.RoleBindings == nil {
		return reply.NewRequestFailed("role binding storage not configured")
	}
	if s.conf.StorageConfig.Roles == nil {
		return reply.NewRequestFailed("role storage not configured")
	}

	if err := validateNamespaceName(req.Name); err != nil {
		return err
	}

	if err := s.authorize(ctx, namespacePermission{Namespace: auth.SystemNamespace, Permission: auth.NamespaceDelete}); err != nil {
		return err
	}

	if strings.HasPrefix(req.Name, "_") {
		return types.NewErrNamespaceReserved(req.Name)
	}

	var bindings []types.RoleBinding
	if err := s.conf.StorageConfig.RoleBindings.List(ctx, req.Name, &bindings, types.ListOptions{Limit: 1}); err != nil {
		return err
	}
	if len(bindings) > 0 {
		return types.NewErrNamespaceHasRoleBindings(req.Name)
	}

	var roles []types.Role
	if err := s.conf.StorageConfig.Roles.List(ctx, req.Name, &roles, types.ListOptions{Limit: 1}); err != nil {
		return err
	}
	if len(roles) > 0 {
		return types.NewErrNamespaceHasRoles(req.Name)
	}

	var queues []types.QueueInfo
	if err := s.conf.StorageConfig.Queues.List(ctx, &queues, types.ListOptions{Limit: 1, Namespace: req.Name}); err != nil {
		return err
	}
	if len(queues) > 0 {
		return types.NewErrNamespaceHasQueues(req.Name)
	}

	if err := s.conf.StorageConfig.Namespaces.Delete(ctx, req.Name); err != nil {
		return err
	}
	return nil
}

func (s *Service) UsersCreate(ctx context.Context, req *proto.UserCreateRequest,
	resp *proto.UserCreateResponse) error {
	if s.conf.StorageConfig.Users == nil {
		return reply.NewRequestFailed("user storage not configured")
	}

	var user types.User

	if err := s.validateUserCreateProto(req, &user); err != nil {
		return err
	}

	if err := s.authorize(ctx, namespacePermission{Namespace: auth.SystemNamespace, Permission: auth.UserCreate}); err != nil {
		return err
	}

	user.ID = internal.NewUID()
	user.CreatedAt = s.conf.Clock.Now().UTC()
	user.UpdatedAt = user.CreatedAt

	if err := s.conf.StorageConfig.Users.Add(ctx, user); err != nil {
		return err
	}

	resp.Id = user.ID
	return nil
}

func (s *Service) UsersList(ctx context.Context, req *proto.UsersListRequest,
	resp *proto.UsersListResponse) error {
	if s.conf.StorageConfig.Users == nil {
		return reply.NewRequestFailed("user storage not configured")
	}

	if req.Limit == 0 {
		req.Limit = DefaultListLimit
	}

	if err := s.authorize(ctx, namespacePermission{Namespace: auth.SystemNamespace, Permission: auth.UserList}); err != nil {
		return err
	}

	users := make([]types.User, 0, allocInt32(req.Limit))
	if err := s.conf.StorageConfig.Users.List(ctx, &users, types.ListOptions{
		Pivot: types.ToItemID(req.Pivot),
		Limit: int(req.Limit),
	}); err != nil {
		return err
	}

	for _, user := range users {
		resp.Items = append(resp.Items, user.ToProto(new(proto.User)))
	}
	return nil
}

func (s *Service) UsersDelete(ctx context.Context, req *proto.UsersDeleteRequest) error {
	if s.conf.StorageConfig.Users == nil {
		return reply.NewRequestFailed("user storage not configured")
	}
	if s.conf.StorageConfig.RoleBindings == nil {
		return reply.NewRequestFailed("role binding storage not configured")
	}
	if s.conf.StorageConfig.APIKeys == nil {
		return reply.NewRequestFailed("api key storage not configured")
	}

	if err := s.authorize(ctx, namespacePermission{Namespace: auth.SystemNamespace, Permission: auth.UserDelete}); err != nil {
		return err
	}

	if err := s.conf.StorageConfig.Users.Delete(ctx, req.Id); err != nil {
		return err
	}
	s.conf.Auth.InvalidateUser(req.Id)

	var errs []error
	if err := s.conf.StorageConfig.RoleBindings.DeleteByUser(ctx, req.Id); err != nil {
		errs = append(errs, fmt.Errorf("cascade delete role bindings: %w", err))
	}
	if err := s.conf.StorageConfig.APIKeys.DeleteByUser(ctx, req.Id); err != nil {
		errs = append(errs, fmt.Errorf("cascade delete api keys: %w", err))
	}
	return errors.Join(errs...)
}

func (s *Service) APIKeysCreate(ctx context.Context, req *proto.APIKeyCreateRequest,
	resp *proto.APIKeyCreateResponse) error {
	if s.conf.StorageConfig.Users == nil {
		return reply.NewRequestFailed("user storage not configured")
	}
	if s.conf.StorageConfig.APIKeys == nil {
		return reply.NewRequestFailed("api key storage not configured")
	}

	var key types.APIKey

	if err := s.validateAPIKeyCreateProto(req, &key); err != nil {
		return err
	}

	if err := s.authorize(ctx, namespacePermission{Namespace: auth.SystemNamespace, Permission: auth.APIKeyCreate}); err != nil {
		return err
	}

	var user types.User
	if err := s.conf.StorageConfig.Users.Get(ctx, key.UserID, &user); err != nil {
		return err
	}

	resolvedTag := req.KeyTag
	if resolvedTag == "" && key.NamespaceScope != nil && s.conf.StorageConfig.Namespaces != nil {
		var ns types.Namespace
		if err := s.conf.StorageConfig.Namespaces.Get(ctx, *key.NamespaceScope, &ns); err != nil {
			var notFound *reply.ErrRequestFailed
			if !errors.As(err, &notFound) {
				return err
			}
		} else {
			resolvedTag = ns.APIKeyTag
		}
	}
	if resolvedTag == "" {
		resolvedTag = auth.DefaultKeyTag
	}

	generated, err := auth.GenerateAPIKey(resolvedTag)
	if err != nil {
		s.conf.Log.Error("failed to generate api key", "error", err)
		return reply.NewRequestFailed("failed to generate api key")
	}

	key.ID = internal.NewUID()
	key.KeyHash = generated.KeyHash
	key.KeyPrefix = generated.Prefix
	key.CreatedAt = s.conf.Clock.Now().UTC()

	if err := s.conf.StorageConfig.APIKeys.Add(ctx, key); err != nil {
		return err
	}

	resp.Prefix = generated.Prefix
	resp.Key = generated.Key
	resp.Id = key.ID
	return nil
}

func (s *Service) APIKeysList(ctx context.Context, req *proto.APIKeysListRequest,
	resp *proto.APIKeysListResponse) error {
	if s.conf.StorageConfig.APIKeys == nil {
		return reply.NewRequestFailed("api key storage not configured")
	}

	if req.Limit == 0 {
		req.Limit = DefaultListLimit
	}

	if err := s.authorize(ctx, namespacePermission{Namespace: auth.SystemNamespace, Permission: auth.APIKeyList}); err != nil {
		return err
	}

	keys := make([]types.APIKey, 0, allocInt32(req.Limit))
	opts := types.ListOptions{
		Pivot: types.ToItemID(req.Pivot),
		Limit: int(req.Limit),
	}

	var err error
	if req.UserId != "" {
		err = s.conf.StorageConfig.APIKeys.ListByUser(ctx, req.UserId, &keys, opts)
	} else {
		err = s.conf.StorageConfig.APIKeys.List(ctx, &keys, opts)
	}
	if err != nil {
		return err
	}

	for _, key := range keys {
		resp.Items = append(resp.Items, key.ToProto(new(proto.APIKeyMetadata)))
	}
	return nil
}

func (s *Service) APIKeysDelete(ctx context.Context, req *proto.APIKeysDeleteRequest) error {
	if s.conf.StorageConfig.APIKeys == nil {
		return reply.NewRequestFailed("api key storage not configured")
	}

	if err := s.authorize(ctx, namespacePermission{Namespace: auth.SystemNamespace, Permission: auth.APIKeyDelete}); err != nil {
		return err
	}

	var key types.APIKey
	if err := s.conf.StorageConfig.APIKeys.Get(ctx, req.Id, &key); err != nil {
		return err
	}

	if err := s.conf.StorageConfig.APIKeys.Delete(ctx, req.Id); err != nil {
		return err
	}

	s.conf.Auth.InvalidateKey(key.KeyHash)
	return nil
}

func (s *Service) RolesCreate(ctx context.Context, req *proto.RoleCreateRequest,
	resp *proto.RoleCreateResponse) error {
	if s.conf.StorageConfig.Namespaces == nil {
		return reply.NewRequestFailed("namespace storage not configured")
	}
	if s.conf.StorageConfig.Roles == nil {
		return reply.NewRequestFailed("role storage not configured")
	}

	var role types.Role

	if err := s.validateRoleCreateProto(req, &role); err != nil {
		return err
	}

	if err := s.authorize(ctx, namespacePermission{Namespace: role.Namespace, Permission: auth.RoleCreate}); err != nil {
		return err
	}

	if auth.IsStandardRole(role.Name) {
		return types.NewErrRoleIsStandard(role.Name)
	}

	var ns types.Namespace
	if err := s.conf.StorageConfig.Namespaces.Get(ctx, role.Namespace, &ns); err != nil {
		return err
	}

	role.ID = internal.NewUID()
	role.CreatedAt = s.conf.Clock.Now().UTC()

	if err := s.conf.StorageConfig.Roles.Add(ctx, role); err != nil {
		return err
	}

	resp.Id = role.ID
	return nil
}

func (s *Service) RolesList(ctx context.Context, req *proto.RolesListRequest,
	resp *proto.RolesListResponse) error {
	if s.conf.StorageConfig.Roles == nil {
		return reply.NewRequestFailed("role storage not configured")
	}

	if req.Limit == 0 {
		req.Limit = DefaultListLimit
	}

	ns := resolveNamespace(req.Namespace)
	if err := s.authorize(ctx, namespacePermission{Namespace: ns, Permission: auth.RoleList}); err != nil {
		return err
	}

	roles := make([]types.Role, 0, allocInt32(req.Limit))
	if err := s.conf.StorageConfig.Roles.List(ctx, ns, &roles, types.ListOptions{
		Pivot: types.ToItemID(req.Pivot),
		Limit: int(req.Limit),
	}); err != nil {
		return err
	}

	for _, role := range roles {
		resp.Items = append(resp.Items, role.ToProto(new(proto.Role)))
	}
	return nil
}

func (s *Service) RolesUpdate(ctx context.Context, req *proto.RoleUpdateRequest) error {
	if s.conf.StorageConfig.Roles == nil {
		return reply.NewRequestFailed("role storage not configured")
	}

	var role types.Role

	if err := s.validateRoleUpdateProto(req, &role); err != nil {
		return err
	}

	if err := s.authorize(ctx, namespacePermission{Namespace: role.Namespace, Permission: auth.RoleUpdate}); err != nil {
		return err
	}

	var existing types.Role
	if err := s.conf.StorageConfig.Roles.Get(ctx, store.RoleName{Namespace: role.Namespace, Name: role.Name}, &existing); err != nil {
		return err
	}

	if auth.IsStandardRole(existing.Name) {
		return types.NewErrRoleIsStandard(existing.Name)
	}

	role.ID = existing.ID
	if err := s.conf.StorageConfig.Roles.Update(ctx, role); err != nil {
		return err
	}
	return nil
}

func (s *Service) RolesDelete(ctx context.Context, req *proto.RolesDeleteRequest) error {
	if s.conf.StorageConfig.Roles == nil {
		return reply.NewRequestFailed("role storage not configured")
	}
	if s.conf.StorageConfig.RoleBindings == nil {
		return reply.NewRequestFailed("role binding storage not configured")
	}

	ns := resolveNamespace(req.Namespace)
	if err := s.authorize(ctx, namespacePermission{Namespace: ns, Permission: auth.RoleDelete}); err != nil {
		return err
	}

	var role types.Role
	if err := s.conf.StorageConfig.Roles.Get(ctx, store.RoleName{Namespace: ns, Name: req.Name}, &role); err != nil {
		return err
	}

	if auth.IsStandardRole(role.Name) {
		return types.NewErrRoleIsStandard(role.Name)
	}

	var bindings []types.RoleBinding
	if err := s.conf.StorageConfig.RoleBindings.ListByRole(ctx, role.ID, &bindings, 1); err != nil {
		return err
	}
	if len(bindings) > 0 {
		return types.NewErrRoleHasBindings(role.Name)
	}

	if err := s.conf.StorageConfig.Roles.Delete(ctx, role.ID); err != nil {
		return err
	}
	return nil
}

func (s *Service) RoleBindingsCreate(ctx context.Context, req *proto.RoleBindingCreateRequest,
	resp *proto.RoleBindingCreateResponse) error {
	if s.conf.StorageConfig.Roles == nil {
		return reply.NewRequestFailed("role storage not configured")
	}
	if s.conf.StorageConfig.Users == nil {
		return reply.NewRequestFailed("user storage not configured")
	}
	if s.conf.StorageConfig.RoleBindings == nil {
		return reply.NewRequestFailed("role binding storage not configured")
	}

	var binding types.RoleBinding

	if err := s.validateRoleBindingCreateProto(req, &binding); err != nil {
		return err
	}

	if err := s.authorize(ctx, namespacePermission{Namespace: binding.Namespace, Permission: auth.RoleBindingCreate}); err != nil {
		return err
	}

	var role types.Role
	if err := s.conf.StorageConfig.Roles.Get(ctx, store.RoleName{Namespace: req.Namespace, Name: req.RoleName}, &role); err != nil {
		return err
	}

	var user types.User
	if err := s.conf.StorageConfig.Users.Get(ctx, binding.UserID, &user); err != nil {
		return err
	}

	binding.ID = internal.NewUID()
	binding.RoleID = role.ID
	binding.CreatedAt = s.conf.Clock.Now().UTC()

	if err := s.conf.StorageConfig.RoleBindings.Add(ctx, binding); err != nil {
		return err
	}

	resp.Id = binding.ID
	return nil
}

func (s *Service) RoleBindingsList(ctx context.Context, req *proto.RoleBindingsListRequest,
	resp *proto.RoleBindingsListResponse) error {
	if s.conf.StorageConfig.RoleBindings == nil {
		return reply.NewRequestFailed("role binding storage not configured")
	}

	if req.Limit == 0 {
		req.Limit = DefaultListLimit
	}

	ns := resolveNamespace(req.Namespace)
	if err := s.authorize(ctx, namespacePermission{Namespace: ns, Permission: auth.RoleBindingList}); err != nil {
		return err
	}

	bindings := make([]types.RoleBinding, 0, allocInt32(req.Limit))
	if err := s.conf.StorageConfig.RoleBindings.List(ctx, ns, &bindings, types.ListOptions{
		Pivot: types.ToItemID(req.Pivot),
		Limit: int(req.Limit),
	}); err != nil {
		return err
	}

	for _, binding := range bindings {
		resp.Items = append(resp.Items, binding.ToProto(new(proto.RoleBinding)))
	}
	return nil
}

func (s *Service) RoleBindingsDelete(ctx context.Context, req *proto.RoleBindingDeleteRequest) error {
	if s.conf.StorageConfig.Roles == nil {
		return reply.NewRequestFailed("role storage not configured")
	}
	if s.conf.StorageConfig.RoleBindings == nil {
		return reply.NewRequestFailed("role binding storage not configured")
	}

	if strings.Contains(req.UserId, ":") {
		return reply.NewInvalidOption("user_id is invalid; '%s' cannot contain ':' character", req.UserId)
	}

	ns := resolveNamespace(req.Namespace)
	if err := s.authorize(ctx, namespacePermission{Namespace: ns, Permission: auth.RoleBindingDelete}); err != nil {
		return err
	}

	var role types.Role
	if err := s.conf.StorageConfig.Roles.Get(ctx, store.RoleName{Namespace: ns, Name: req.RoleName}, &role); err != nil {
		return err
	}

	if err := s.conf.StorageConfig.RoleBindings.DeleteByUserAndRole(ctx, store.RoleForUser{Namespace: ns, UserID: req.UserId, RoleID: role.ID}); err != nil {
		return err
	}
	return nil
}

func (s *Service) Bootstrap(ctx context.Context) error {
	if s.conf.StorageConfig.Namespaces != nil {
		if err := s.bootstrapSystemNamespace(ctx); err != nil {
			return err
		}
	}

	if _, ok := s.conf.Auth.(*auth.NoOpAuthBackend); ok {
		return nil
	}
	if s.conf.StorageConfig.Users == nil || s.conf.StorageConfig.Roles == nil ||
		s.conf.StorageConfig.RoleBindings == nil {
		return nil
	}

	if err := s.bootstrapAnonymousUser(ctx); err != nil {
		return err
	}
	if err := s.bootstrapStandardRoles(ctx); err != nil {
		return err
	}
	if err := s.bootstrapAnonymousAdminBinding(ctx); err != nil {
		return err
	}
	return nil
}

func (s *Service) bootstrapSystemNamespace(ctx context.Context) error {
	err := s.conf.StorageConfig.Namespaces.Add(ctx, types.Namespace{
		Name:      auth.SystemNamespace,
		CreatedAt: s.conf.Clock.Now().UTC(),
	})
	if err != nil && !errors.Is(err, types.ErrNamespaceAlreadyExists) {
		return err
	}
	return nil
}

func (s *Service) bootstrapAnonymousUser(ctx context.Context) error {
	now := s.conf.Clock.Now().UTC()
	err := s.conf.StorageConfig.Users.Add(ctx, types.User{
		ID:        auth.AnonymousUserID,
		Username:  auth.AnonymousUsername,
		CreatedAt: now,
		UpdatedAt: now,
	})
	if err != nil && !errors.Is(err, types.ErrUserAlreadyExists) && !errors.Is(err, types.ErrUsernameAlreadyTaken) {
		return err
	}
	return nil
}

func (s *Service) bootstrapStandardRoles(ctx context.Context) error {
	now := s.conf.Clock.Now().UTC()
	roles := []types.Role{
		{
			Name:        auth.RoleAdmin,
			Namespace:   auth.SystemNamespace,
			Permissions: auth.AdminPermissions(),
			CreatedAt:   now,
		},
		{
			Name:        auth.RoleNamespaceOwner,
			Namespace:   auth.SystemNamespace,
			Permissions: auth.NamespaceOwnerPermissions(),
			CreatedAt:   now,
		},
		{
			Name:        auth.RolePublicViewer,
			Namespace:   auth.SystemNamespace,
			Permissions: auth.PublicViewerPermissions(),
			CreatedAt:   now,
		},
	}
	for _, role := range roles {
		var existing types.Role
		if err := s.conf.StorageConfig.Roles.Get(ctx, store.RoleName{Namespace: role.Namespace, Name: role.Name}, &existing); err != nil {
			role.ID = internal.NewUID()
			if err = s.conf.StorageConfig.Roles.Add(ctx, role); err != nil {
				return err
			}
			continue
		}
		role.ID = existing.ID
		if err := s.conf.StorageConfig.Roles.Update(ctx, role); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) bootstrapAnonymousAdminBinding(ctx context.Context) error {
	var adminRole types.Role
	if err := s.conf.StorageConfig.Roles.Get(ctx, store.RoleName{Namespace: auth.SystemNamespace, Name: auth.RoleAdmin}, &adminRole); err != nil {
		return err
	}

	err := s.conf.StorageConfig.RoleBindings.Add(ctx, types.RoleBinding{
		ID:        internal.NewUID(),
		UserID:    auth.AnonymousUserID,
		RoleID:    adminRole.ID,
		Namespace: auth.SystemNamespace,
		CreatedAt: s.conf.Clock.Now().UTC(),
	})
	if errors.Is(err, types.ErrRoleBindingLimitReached) {
		s.conf.Log.Warn("Open Door binding not granted; the anonymous user already holds the most role bindings "+
			"one user can hold in _system", "error", err)
		return nil
	}
	if err != nil && !errors.Is(err, types.ErrRoleBindingAlreadyExists) {
		return err
	}
	if err == nil {
		s.conf.Log.Warn("SYSTEM RUNNING IN OPEN DOOR MODE - anonymous user has admin privileges. " +
			"Remove the anonymous admin binding to secure the system.")
	}
	return nil
}
