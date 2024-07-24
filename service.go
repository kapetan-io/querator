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
	"errors"
	"github.com/duh-rpc/duh-go"
	"github.com/kapetan-io/querator/internal"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/tackle/set"
	"google.golang.org/protobuf/types/known/timestamppb"
	"log/slog"
)

const DefaultListLimit = 1_000

var ErrQueueNameEmpty = transport.NewInvalidOption("invalid queue; queue name cannot be empty")

// TODO: Document this and make it configurable via the daemon
type ServiceOptions struct {
	Logger              duh.StandardLogger
	Storage             store.Storage
	MaxReserveBatchSize int
	MaxProduceBatchSize int
}

type Service struct {
	queues *internal.QueuesManager
	opts   ServiceOptions
}

func NewService(opts ServiceOptions) (*Service, error) {
	set.Default(&opts.Logger, slog.Default())

	if opts.Storage == nil {
		return nil, errors.New("storage is required")
	}

	qm, err := internal.NewQueuesManager(internal.QueuesManagerOptions{
		QueueOptions: internal.QueueOptions{
			MaxReserveBatchSize: opts.MaxReserveBatchSize,
			MaxProduceBatchSize: opts.MaxProduceBatchSize,
		},
		Storage: opts.Storage,
		Logger:  opts.Logger,
	})
	if err != nil {
		return nil, err
	}

	return &Service{
		opts:   opts,
		queues: qm,
	}, nil
}

func (s *Service) QueueProduce(ctx context.Context, req *proto.QueueProduceRequest) error {
	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	var r types.ProduceRequest
	if err := s.validateQueueProduceProto(req, &r); err != nil {
		return err
	}

	// Produce will block until success, context cancel or timeout
	if err := queue.Produce(ctx, &r); err != nil {
		return err
	}

	return nil
}

func (s *Service) QueueReserve(ctx context.Context, req *proto.QueueReserveRequest, res *proto.QueueReserveResponse) error {
	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	var r types.ReserveRequest
	if err := s.validateQueueReserveProto(req, &r); err != nil {
		return err
	}

	// Reserve will block until success, context cancel or timeout
	if err := queue.Reserve(ctx, &r); err != nil {
		return err
	}

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

	var r types.CompleteRequest
	if err := s.validateQueueCompleteProto(req, &r); err != nil {
		return err
	}

	// Complete will block until success, context cancel or timeout
	if err := queue.Complete(ctx, &r); err != nil {
		return err
	}

	return nil
}

func (s *Service) QueueClear(ctx context.Context, req *proto.QueueClearRequest) error {
	//if strings.TrimSpace(req.QueueName) == "" {
	//	return ErrQueueNameEmpty
	//}

	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	r := types.ClearRequest{
		Destructive: req.Destructive,
		Scheduled:   req.Scheduled,
		Queue:       req.Queue,
		Defer:       req.Defer,
	}

	if err := queue.Clear(ctx, &r); err != nil {
		return err
	}

	return nil
}

func (s *Service) QueuePause(ctx context.Context, req *proto.QueuePauseRequest) error {
	var r types.PauseRequest

	//if strings.TrimSpace(req.QueueName) == "" {
	//	return ErrQueueNameEmpty
	//}

	if err := s.validateQueuePauseRequestProto(req, &r); err != nil {
		return err
	}

	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	if err := queue.Pause(ctx, &r); err != nil {
		return err
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

	items := make([]types.QueueInfo, 0, req.Limit)
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

	// TODO: Should be the same validation as the Create method
	if err := s.validateQueueOptionsProto(req, &info); err != nil {
		return err
	}

	if err := s.queues.Update(ctx, info); err != nil {
		return err
	}
	return nil
}

func (s *Service) QueuesDelete(ctx context.Context, req *proto.QueuesDeleteRequest) error {
	// TODO: Validate protobuf

	if err := s.queues.Delete(ctx, req.QueueName); err != nil {
		return err
	}
	return nil
}

// -------------------------------------------------
// API to inspect queue storage
// -------------------------------------------------

func (s *Service) StorageQueueList(ctx context.Context, req *proto.StorageQueueListRequest,
	res *proto.StorageQueueListResponse) error {

	//if strings.TrimSpace(req.QueueName) == "" {
	//	return ErrQueueNameEmpty
	//}

	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	if req.Limit == 0 {
		req.Limit = DefaultListLimit
	}

	items := make([]*types.Item, 0, req.Limit)
	if err := queue.StorageQueueList(ctx, &items, types.ListOptions{
		Pivot: types.ToItemID(req.Pivot),
		Limit: int(req.Limit),
	}); err != nil {
		return err
	}

	for _, item := range items {
		res.Items = append(res.Items, item.ToProto(new(proto.StorageQueueItem)))
	}

	return nil
}

func (s *Service) StorageQueueAdd(ctx context.Context, req *proto.StorageQueueAddRequest,
	res *proto.StorageQueueAddResponse) error {

	//if strings.TrimSpace(req.QueueName) == "" {
	//	return ErrQueueNameEmpty
	//}

	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	items := make([]*types.Item, 0, len(req.Items))
	for _, item := range req.Items {
		i := new(types.Item)
		items = append(items, i.FromProto(item))
	}

	if err := queue.StorageQueueAdd(ctx, &items); err != nil {
		return err
	}

	for _, item := range items {
		res.Items = append(res.Items, item.ToProto(new(proto.StorageQueueItem)))
	}

	return nil
}

func (s *Service) StorageQueueDelete(ctx context.Context, req *proto.StorageQueueDeleteRequest) error {

	//if strings.TrimSpace(req.QueueName) == "" {
	//	return ErrQueueNameEmpty
	//}

	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	ids := make([]types.ItemID, 0, len(req.Ids))
	for _, id := range req.Ids {
		ids = append(ids, types.ItemID(id))
	}

	if err := queue.StorageQueueDelete(ctx, ids); err != nil {
		return err
	}
	return nil
}

func (s *Service) QueueStats(ctx context.Context, req *proto.QueueStatsRequest,
	res *proto.QueueStatsResponse) error {

	//if strings.TrimSpace(req.QueueName) == "" {
	//	return ErrQueueNameEmpty
	//}

	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	var stats types.QueueStats
	if err := queue.QueueStats(ctx, &stats); err != nil {
		return err
	}

	res.AverageReservedAge = stats.AverageReservedAge.String()
	res.TotalReserved = int32(stats.TotalReserved)
	res.AverageAge = stats.AverageAge.String()
	res.Total = int32(stats.Total)
	res.ProduceWaiting = int32(stats.ProduceWaiting)
	res.ReserveWaiting = int32(stats.ReserveWaiting)
	res.CompleteWaiting = int32(stats.CompleteWaiting)
	res.ReserveBlocked = int32(stats.ReserveBlocked)
	return nil
}

func (s *Service) Shutdown(ctx context.Context) error {
	return s.queues.Shutdown(ctx)
}
