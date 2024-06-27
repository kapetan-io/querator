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

package transport

import (
	"context"
	"fmt"
	"github.com/duh-rpc/duh-go"
	v1 "github.com/duh-rpc/duh-go/proto/v1"
	"github.com/kapetan-io/querator/proto"
	"github.com/prometheus/client_golang/prometheus"
	"net/http"
)

const (
	RPCQueueProduce  = "/v1/queue.produce"
	RPCQueueReserve  = "/v1/queue.reserve"
	RPCQueueDefer    = "/v1/queue.defer"
	RPCQueueComplete = "/v1/queue.complete"
	RPCListQueue     = "/v1/queue.list"
	RPCCreateQueue   = "/v1/queue.create"
	RPCDeleteQueue   = "/v1/queue.delete"
	RPCInspectQueue  = "/v1/queue.inspect"
)

type Service interface {
	QueueProduce(context.Context, *proto.QueueProduceRequest, *proto.QueueProduceResponse) error
	QueueCreate(context.Context, *proto.QueueOptions) error
}

type HTTPHandler struct {
	duration *prometheus.SummaryVec
	metrics  http.Handler
	service  Service
}

func NewHTTPHandler(s Service, metrics http.Handler) *HTTPHandler {
	return &HTTPHandler{
		duration: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Name: "http_handler_duration",
			Help: "The timings of http requests handled by the service",
			Objectives: map[float64]float64{
				0.5:  0.05,
				0.99: 0.001,
			},
		}, []string{"path"}),
		metrics: metrics,
		service: s,
	}
}

func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer prometheus.NewTimer(h.duration.WithLabelValues(r.URL.Path)).ObserveDuration()
	ctx := r.Context()

	switch r.URL.Path {
	case RPCQueueProduce:
		h.QueueProduce(ctx, w, r)
		return
	case "/metrics":
		h.metrics.ServeHTTP(w, r)
		return
	case RPCQueueReserve:
	case RPCQueueDefer:
	case RPCQueueComplete:
	case RPCListQueue:
	case RPCCreateQueue:
		h.QueueCreate(ctx, w, r)
		return
	case RPCDeleteQueue:
	case RPCInspectQueue:
	}
	duh.ReplyWithCode(w, r, duh.CodeNotImplemented, nil, "no such method; "+r.URL.Path)
}

func (h *HTTPHandler) QueueProduce(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		duh.ReplyWithCode(w, r, duh.CodeBadRequest, nil,
			fmt.Sprintf("http method '%s' not allowed; only POST", r.Method))
		return
	}

	var req proto.QueueProduceRequest
	if err := duh.ReadRequest(r, &req); err != nil {
		duh.ReplyError(w, r, err)
		return
	}
	var resp proto.QueueProduceResponse
	if err := h.service.QueueProduce(ctx, &req, &resp); err != nil {
		duh.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &resp)
}

func (h *HTTPHandler) QueueCreate(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		duh.ReplyWithCode(w, r, duh.CodeBadRequest, nil,
			fmt.Sprintf("http method '%s' not allowed; only POST", r.Method))
		return
	}

	var req proto.QueueOptions
	if err := duh.ReadRequest(r, &req); err != nil {
		duh.ReplyError(w, r, err)
		return
	}
	if err := h.service.QueueCreate(ctx, &req); err != nil {
		duh.ReplyError(w, r, err)
		return
	}
	resp := v1.Reply{
		Message: "queue created",
		Code:    duh.CodeOK,
	}
	duh.Reply(w, r, duh.CodeOK, &resp)
}

// Describe fetches prometheus metrics to be registered
func (h *HTTPHandler) Describe(ch chan<- *prometheus.Desc) {
	h.duration.Describe(ch)
}

// Collect fetches metrics from the server for use by prometheus
func (h *HTTPHandler) Collect(ch chan<- prometheus.Metric) {
	h.duration.Collect(ch)
}
