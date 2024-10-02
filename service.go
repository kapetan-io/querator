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

package querator

import (
	"context"
	"github.com/kapetan-io/querator/internal"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/set"
	"google.golang.org/protobuf/types/known/timestamppb"
	"log/slog"
)

const (
	DefaultListLimit = 1_000
)

type ServiceConfig struct {
	// Log is the logging implementation used by this Querator instance
	Log *slog.Logger
	// StorageConfig is the configured storage backends
	StorageConfig store.StorageConfig
	// InstanceID is a unique id for this instance of Querator
	InstanceID string
	// WriteTimeout The time it should take for a single batched write to complete
	WriteTimeout clock.Duration
	// ReadTimeout The time it should take for a single batched read to complete
	ReadTimeout clock.Duration
	// MaxReserveBatchSize is the maximum number of items a client can request in a single reserve request
	MaxReserveBatchSize int
	// MaxProduceBatchSize is the maximum number of items a client can produce in a single produce request
	MaxProduceBatchSize int
	// MaxCompleteBatchSize is the maximum number of ids a client can mark complete in a single complete request
	MaxCompleteBatchSize int
	// MaxRequestsPerQueue is the maximum number of client requests a queue can handle before it returns an
	// queue overloaded message
	MaxRequestsPerQueue int
	// Clock is a time provider used to preform time related calculations. It is configurable so that it can
	// be overridden for testing.
	Clock *clock.Provider
}

type Service struct {
	queues *internal.QueuesManager
	conf   ServiceConfig
}

func NewService(conf ServiceConfig) (*Service, error) {
	set.Default(&conf.Log, slog.Default())

	// TODO: Validate the Storage Config (somewhere) ensure that we have at least one storage backend with an affinity
	//  greater than 0.0 else QueuesManager will be unable to assign partitions.

	// TODO: The queue store should also know if the config provided is valid, IE: does every partition
	//  storage name exist in the config? If not, then it's a bad config and Querator should not start.

	qm, err := internal.NewQueuesManager(internal.QueuesManagerConfig{
		LogicalConfig: internal.LogicalConfig{
			MaxReserveBatchSize:  conf.MaxReserveBatchSize,
			MaxProduceBatchSize:  conf.MaxProduceBatchSize,
			MaxCompleteBatchSize: conf.MaxCompleteBatchSize,
			MaxRequestsPerQueue:  conf.MaxRequestsPerQueue,
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

func (s *Service) QueueReserve(ctx context.Context, req *proto.QueueReserveRequest,
	res *proto.QueueReserveResponse) error {

	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	proxy, logical := queue.GetNext()
	if proxy != nil {
		return proxy.QueueReserve(ctx, req, res)
	}

	var r types.ReserveRequest
	if err := s.validateQueueReserveProto(req, &r); err != nil {
		return err
	}

	// Reserve will block until success, context cancel or timeout
	if err := logical.Reserve(ctx, &r); err != nil {
		return err
	}

	res.Partition = int32(r.Partition)
	res.QueueName = req.QueueName

	for _, item := range r.Items {
		res.Items = append(res.Items, &proto.QueueReserveItem{
			ReserveDeadline: timestamppb.New(item.ReserveDeadline),
			Attempts:        int32(item.Attempts),
			Id:              string(item.ID),
			Reference:       item.Reference,
			Encoding:        item.Encoding,
			Bytes:           item.Payload,
			Kind:            item.Kind,
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
			Defer:       req.Defer,
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

// -------------------------------------------------
// API to inspect queue storage
// -------------------------------------------------

func (s *Service) StorageItemsList(ctx context.Context, req *proto.StorageItemsListRequest,
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
	if err := logical.StorageItemsList(ctx, int(req.Partition), &items, types.ListOptions{
		Pivot: types.ToItemID(req.Pivot),
		Limit: int(req.Limit),
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
			ReserveWaiting:  int32(stats.ReserveWaiting),
			CompleteWaiting: int32(stats.CompleteWaiting),
			InFlight:        int32(stats.InFlight),
			Partitions:      nil,
		}

		for _, stat := range stats.Partitions {
			ls.Partitions = append(ls.Partitions,
				&proto.QueuePartitionStats{
					AverageReservedAge: stat.AverageReservedAge.String(),
					Partition:          int32(stat.Partition),
					TotalReserved:      int32(stat.TotalReserved),
					AverageAge:         stat.AverageAge.String(),
					Total:              int32(stat.Total),
				})
		}
		res.LogicalQueues = append(res.LogicalQueues, ls)
	}
	return nil
}

func (s *Service) Shutdown(ctx context.Context) error {
	// See 0015-shutdown-errors.md for a discussion of shutdown operation
	return s.queues.Shutdown(ctx)
}
