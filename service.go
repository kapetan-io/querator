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
	"github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/querator/store"
	"github.com/kapetan-io/tackle/set"
)

type ServiceOptions struct {
	NewQueueStorage     func(name string) store.QueueStorage
	MaxReserveBatchSize int32 `json:"max_reserve_batch_size"`
	MaxProduceBatchSize int   `json:"max_produce_batch_size"`
}

type Service struct {
	manager *internal.QueueManager
	opts    ServiceOptions
}

func NewService(opts ServiceOptions) (*Service, error) {
	// TODO: Document this
	set.Default(opts.MaxReserveBatchSize, 1_000)

	qm := internal.NewQueueManager(internal.QueueManagerOptions{
		NewQueueStorage: opts.NewQueueStorage,
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

func (s *Service) Shutdown(ctx context.Context) error {
	return s.manager.Shutdown(ctx)
}
