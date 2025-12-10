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
	"log/slog"
	"time"

	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/internal"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/set"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	DefaultListLimit = 1_000
)

type Config struct {
	// Log is the logging implementation used by this Querator instance
	Log *slog.Logger
	// StorageConfig is the configured storage backends
	StorageConfig store.Config
	// InstanceID is a unique id for this instance of Querator
	InstanceID string
	// WriteTimeout The time it should take for a single batched write to complete
	WriteTimeout clock.Duration
	// ReadTimeout The time it should take for a single batched read to complete
	ReadTimeout clock.Duration
	// MaxLeaseBatchSize is the maximum number of items a client can request in a single lease request
	MaxLeaseBatchSize int
	// MaxProduceBatchSize is the maximum number of items a client can produce in a single produce request
	MaxProduceBatchSize int
	// MaxCompleteBatchSize is the maximum number of ids a client can mark complete in a single complete request
	MaxCompleteBatchSize int
	// MaxRequestsPerQueue is the maximum number of client requests a queue can handle before it returns an
	// queue overloaded message
	MaxRequestsPerQueue int
	// MaxConcurrentConnections is the maximum number of connections allowed. Default is 1,000
	MaxConcurrentRequests int
	// Clock is a time provider used to preform time related calculations. It is configurable so that it can
	// be overridden for testing.
	Clock *clock.Provider
}

type Service struct {
	queues *internal.QueuesManager
	conf   Config
}

func New(conf Config) (*Service, error) {
	set.Default(&conf.Log, slog.Default())
	set.Default(&conf.ReadTimeout, 3*time.Second)
	set.Default(&conf.WriteTimeout, 3*time.Second)

	// TODO: Validate the Storage Config (somewhere) ensure that we have at least one storage backend with an affinity
	//  greater than 0.0 else QueuesManager will be unable to assign partitions.

	// TODO: The queue store should also know if the config provided is valid, IE: does every partition
	//  storage name exist in the config? If not, then it's a bad config and Querator should not start.

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

	return &Service{
		conf:   conf,
		queues: qm,
	}, nil
}

func (s *Service) QueueProduce(ctx context.Context, req *proto.QueueProduceRequest) error {
	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	proxy, logical := queue.GetNext()
	if proxy != nil {
		return proxy.QueueProduce(ctx, req)
	}

	var r types.ProduceRequest
	if err := s.validateQueueProduceProto(req, &r); err != nil {
		return err
	}

	// Produce will block until success, context cancel or timeout
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

	proxy, logical := queue.GetNext()
	if proxy != nil {
		return proxy.QueueLease(ctx, req, res)
	}

	var r types.LeaseRequest
	if err := s.validateQueueLeaseProto(req, &r); err != nil {
		return err
	}

	// Lease will block until success, context cancel or timeout
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

	proxy, logical, err := queue.GetByPartition(int(req.Partition))
	if err != nil {
		return err
	}

	if proxy != nil {
		return proxy.QueueComplete(ctx, req)
	}

	var r types.CompleteRequest
	if err := s.validateQueueCompleteProto(req, &r); err != nil {
		return err
	}

	// Complete will block until success, context cancel or timeout
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

	proxy, logical, err := queue.GetByPartition(int(req.Partition))
	if err != nil {
		return err
	}

	if proxy != nil {
		return proxy.QueueRetry(ctx, req)
	}

	var r types.RetryRequest
	if err := s.validateQueueRetryProto(req, &r); err != nil {
		return err
	}

	// Retry will block until success, context cancel or timeout
	if err := logical.Retry(ctx, &r); err != nil {
		return err
	}

	return nil
}

// TODO(reload)
func (s *Service) QueueReload(ctx context.Context, req *proto.QueueClearRequest) error {
	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	// Clear all the logical queues on this instance
	for _, logical := range queue.GetAll() {
		r := types.ReloadRequest{
			Partitions: make([]int, 0), // TODO
		}

		if err := logical.ReloadPartitions(ctx, &r); err != nil {
			// If a logical queue goes away while we are clearing
			// return the error to the client, so they know the clear request didn't complete.
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

	// Clear all the logical queues on this instance
	for _, logical := range queue.GetAll() {
		r := types.ClearRequest{
			Destructive: req.Destructive,
			Scheduled:   req.Scheduled,
			Queue:       req.Queue,
			Retry:       req.Retry,
		}

		if err := logical.Clear(ctx, &r); err != nil {
			// If a logical queue goes away while we are clearing
			// return the error to the client, so they know the clear request didn't complete.
			return err
		}
	}

	return nil
}

// PauseQueue is used to temporarily pause processing of queue requests to simulate various high contention scenarios
// in testing; it is not exposed to the users via API calls.
func (s *Service) PauseQueue(ctx context.Context, queueName string, pause bool) error {
	queue, err := s.queues.Get(ctx, queueName)
	if err != nil {
		return err
	}

	// Pause all the logical queues on this instance
	for _, logical := range queue.GetAll() {
		if err := logical.Pause(ctx, &types.PauseRequest{Pause: pause}); err != nil {
			return err
		}
	}

	return nil
}

// -------------------------------------------------
// API to manage lists of queues
// -------------------------------------------------

func (s *Service) QueuesCreate(ctx context.Context, req *proto.QueueInfo) error {
	var info types.QueueInfo

	if err := s.validateQueueOptionsProto(req, &info); err != nil {
		return err
	}

	_, err := s.queues.Create(ctx, info)
	if err != nil {
		return err
	}

	return nil
}

func (s *Service) QueuesList(ctx context.Context, req *proto.QueuesListRequest,
	resp *proto.QueuesListResponse) error {

	if req.Limit == 0 {
		req.Limit = DefaultListLimit
	}

	items := make([]types.QueueInfo, 0, allocInt32(req.Limit))
	if err := s.queues.List(ctx, &items, types.ListOptions{
		Pivot: types.ToItemID(req.Pivot),
		Limit: int(req.Limit),
	}); err != nil {
		return err
	}

	for _, item := range items {
		resp.Items = append(resp.Items, item.ToProto(new(proto.QueueInfo)))
	}
	return nil
}

func (s *Service) QueuesUpdate(ctx context.Context, req *proto.QueueInfo) error {
	var info types.QueueInfo

	if err := s.validateQueueOptionsProto(req, &info); err != nil {
		return err
	}

	if err := s.queues.Update(ctx, info); err != nil {
		return err
	}
	return nil
}

func (s *Service) QueuesDelete(ctx context.Context, req *proto.QueuesDeleteRequest) error {

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

	info := queue.Info()
	*resp = *info.ToProto(resp)
	return nil
}

// -------------------------------------------------
// API to inspect queue storage
// -------------------------------------------------

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

	proxy, logical, err := queue.GetByPartition(int(req.Partition))
	if err != nil {
		return err
	}

	if proxy != nil {
		return proxy.StorageItemsList(ctx, req, res)
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

	proxy, logical, err := queue.GetByPartition(int(req.Partition))
	if err != nil {
		return err
	}

	if proxy != nil {
		return proxy.StorageItemsImport(ctx, req, res)
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

	proxy, logical, err := queue.GetByPartition(int(req.Partition))
	if err != nil {
		return err
	}

	if proxy != nil {
		return proxy.StorageItemsDelete(ctx, req)
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
				})
		}
		res.LogicalQueues = append(res.LogicalQueues, ls)
	}
	return nil
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
		check.Status = transport.HealthStatusFail
		check.Output = err.Error()
		response.Status = transport.HealthStatusFail
	} else {
		check.Status = transport.HealthStatusPass
	}

	response.Checks["queues:storage"] = []transport.Check{check}

	return response, nil
}

func (s *Service) Shutdown(ctx context.Context) error {
	// See 0015-shutdown-errors.md for a discussion of shutdown operation
	return s.queues.Shutdown(ctx)
}
