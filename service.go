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
)

type ServiceOptions struct {
	// QueueManager manages queues
	QueueManager internal.QueueManager
}

type Service struct {
	opts ServiceOptions
	//log  slog.Logger
}

func NewService(opts ServiceOptions) *Service {
	return &Service{
		opts: opts,
	}
}

func (s *Service) QueueProduce(ctx context.Context, req *proto.QueueProduceRequest, res *proto.QueueProduceResponse) error {
	queue, err := s.opts.QueueManager.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	var r internal.ProduceRequest
	if err := validateQueueProduceProto(req, &r); err != nil {
		return err
	}

	// TODO: Forward the request to a different instance of querator if needed

	// Produce will block until success, context cancel or timeout
	if err := queue.Produce(ctx, &r); err != nil {
		return err
	}

	return nil
}

func (s *Service) QueueReserve(ctx context.Context, req *proto.QueueReserveRequest, res *proto.QueueReserveResponse) error {
	// TODO: Lookup the queue
	// TODO: Forward the request
	// TODO: Handle the response
	return nil
}

// TODO: Manage Queue Methods
//func (s *Service) QueueCreate(ctx context.Context, req *proto.Queue, res *proto.QueueReserveResponse) error {
