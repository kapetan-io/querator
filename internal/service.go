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

package internal

import (
	"context"
	"github.com/kapetan-io/querator/proto"
	"log/slog"
)

type Service struct {
	opts ServiceOptions
	log  slog.Logger
}

func NewService(opts ...ServiceOptions) *Service {
	return &Service{}
}

func (s *Service) QueueProduce(ctx context.Context, req *proto.QueueProduceRequest, res *proto.QueueProduceResponse) error {
	res.MessageId = "queue-p-12048123098"
	return nil
}
