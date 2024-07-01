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
	"github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/querator/store"
	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/tackle/set"
	"google.golang.org/protobuf/types/known/timestamppb"
	"log/slog"
	"strings"
)

// TODO: Document this and make it configurable via the daemon
type ServiceOptions struct {
	Logger              duh.StandardLogger
	Storage             store.Storage
	MaxReserveBatchSize int
	MaxProduceBatchSize int
}

type Service struct {
	manager *internal.QueueManager
	opts    ServiceOptions
}

func NewService(opts ServiceOptions) (*Service, error) {
	set.Default(&opts.Logger, slog.Default())

	if opts.Storage == nil {
		return nil, errors.New("storage is required")
	}

	qm := internal.NewQueueManager(internal.QueueManagerOptions{
		QueueOptions: internal.QueueOptions{
			MaxReserveBatchSize: opts.MaxReserveBatchSize,
			MaxProduceBatchSize: opts.MaxProduceBatchSize,
		},
		Storage: opts.Storage,
	})

	return &Service{
		opts:    opts,
		manager: qm,
	}, nil
}

func (s *Service) QueueProduce(ctx context.Context, req *proto.QueueProduceRequest) error {
	queue, err := s.manager.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	var r internal.ProduceRequest
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
	queue, err := s.manager.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	var r internal.ReserveRequest
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
			Reference:       item.Reference,
			Encoding:        item.Encoding,
			Kind:            item.Kind,
			Bytes:           item.Payload,
			Id:              item.ID,
		})
	}

	return nil
}

func (s *Service) QueueComplete(ctx context.Context, req *proto.QueueCompleteRequest) error {
	queue, err := s.manager.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	var r internal.CompleteRequest
	if err := s.validateQueueCompleteProto(req, &r); err != nil {
		return err
	}

	// Complete will block until success, context cancel or timeout
	if err := queue.Complete(ctx, &r); err != nil {
		return err
	}

	return nil
}

// TODO: Manage Queue Methods

func (s *Service) QueueCreate(ctx context.Context, req *proto.QueueOptions) error {
	var opts internal.QueueOptions

	if err := s.validateQueueOptionsProto(req, &opts); err != nil {
		return err
	}

	_, err := s.manager.Create(ctx, opts)
	if err != nil {
		return err
	}
	return nil
}

func (s *Service) StorageList(ctx context.Context, req *proto.StorageListRequest, res *proto.StorageListResponse) error {
	if req.Limit > 1_000 {
		return transport.NewInvalidRequest("limit exceeds maximum limit;"+
			" max limit is 1,000 requested '%d'", req.Limit)
	}

	if strings.TrimSpace(req.QueueName) == "" {
		return transport.NewInvalidRequest("queue name cannot be empty")
	}

	queue, err := s.manager.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	r := internal.StorageRequest{Pivot: req.Pivot, Limit: int(req.Limit)}
	if err := queue.Storage(ctx, &r); err != nil {
		return transport.NewRequestFailed("list request failed; %s", err)
	}

	for _, item := range r.Items {
		res.Items = append(res.Items, item.ToStorageItemProto(new(proto.StorageItem)))
	}

	return nil
}

func (s *Service) StorageInspect(ctx context.Context, req *proto.StorageInspectRequest, res *proto.StorageItem) error {

	var sid store.StorageID
	if err := s.opts.Storage.ParseID(req.Id, &sid); err != nil {
		return transport.NewInvalidRequest("invalid storage id; %s", err)
	}

	queue, err := s.manager.Get(ctx, sid.Queue)
	if err != nil {
		return err
	}

	r := internal.StorageRequest{ID: req.Id}
	if err := queue.Storage(ctx, &r); err != nil {
		return transport.NewRequestFailed("inspect request failed; %s", err)
	}

	r.Items[0].ToStorageItemProto(res)
	return nil
}

func (s *Service) Shutdown(ctx context.Context) error {
	return s.manager.Shutdown(ctx)
}
