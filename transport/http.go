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
	RPCQueueProduce   = "/v1/queue.produce"
	RPCQueueReserve   = "/v1/queue.reserve"
	RPCQueueDefer     = "/v1/queue.defer"
	RPCQueueComplete  = "/v1/queue.complete"
	RPCQueueList      = "/v1/queue.list"
	RPCQueueCreate    = "/v1/queue.create"
	RPCQueueDelete    = "/v1/queue.delete"
	RPCQueueInspect   = "/v1/queue.inspect"
	RPCStorageInspect = "/v1/storage.inspect"
	RPCStorageList    = "/v1/storage.list"
)

type Service interface {
	QueueProduce(context.Context, *proto.QueueProduceRequest) error
	QueueCreate(context.Context, *proto.QueueOptions) error
	QueueReserve(context.Context, *proto.QueueReserveRequest, *proto.QueueReserveResponse) error
	QueueComplete(context.Context, *proto.QueueCompleteRequest) error
	StorageList(context.Context, *proto.StorageListRequest, *proto.StorageListResponse) error
	StorageInspect(context.Context, *proto.StorageInspectRequest, *proto.StorageItem) error
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

	if r.Method != http.MethodPost {
		duh.ReplyWithCode(w, r, duh.CodeBadRequest, nil,
			fmt.Sprintf("http method '%s' not allowed; only POST", r.Method))
		return
	}

	switch r.URL.Path {
	case RPCQueueProduce:
		h.QueueProduce(ctx, w, r)
		return
	case RPCQueueReserve:
		h.QueueReserve(ctx, w, r)
		return
	case RPCQueueDefer:
	case RPCQueueComplete:
		h.QueueComplete(ctx, w, r)
		return
	case RPCQueueList:
	case RPCQueueCreate:
		h.QueueCreate(ctx, w, r)
		return
	case RPCQueueDelete:
	case RPCQueueInspect:
	case RPCStorageInspect:
		h.StorageInspect(ctx, w, r)
		return
	case RPCStorageList:
		h.StorageList(ctx, w, r)
		return
	case "/metrics":
		h.metrics.ServeHTTP(w, r)
		return
	}
	duh.ReplyWithCode(w, r, duh.CodeNotImplemented, nil, "no such method; "+r.URL.Path)
}

func (h *HTTPHandler) QueueProduce(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req proto.QueueProduceRequest
	if err := duh.ReadRequest(r, &req); err != nil {
		duh.ReplyError(w, r, err)
		return
	}

	if err := h.service.QueueProduce(ctx, &req); err != nil {
		duh.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &v1.Reply{Code: duh.CodeOK})
}

func (h *HTTPHandler) QueueReserve(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req proto.QueueReserveRequest
	if err := duh.ReadRequest(r, &req); err != nil {
		duh.ReplyError(w, r, err)
		return
	}

	var resp proto.QueueReserveResponse
	if err := h.service.QueueReserve(ctx, &req, &resp); err != nil {
		duh.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &resp)
}

func (h *HTTPHandler) QueueComplete(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req proto.QueueCompleteRequest
	if err := duh.ReadRequest(r, &req); err != nil {
		duh.ReplyError(w, r, err)
		return
	}

	if err := h.service.QueueComplete(ctx, &req); err != nil {
		duh.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &v1.Reply{Code: duh.CodeOK})
}

func (h *HTTPHandler) QueueCreate(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req proto.QueueOptions
	if err := duh.ReadRequest(r, &req); err != nil {
		duh.ReplyError(w, r, err)
		return
	}

	if err := h.service.QueueCreate(ctx, &req); err != nil {
		duh.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &v1.Reply{Code: duh.CodeOK})
}

func (h *HTTPHandler) StorageList(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req proto.StorageListRequest
	if err := duh.ReadRequest(r, &req); err != nil {
		duh.ReplyError(w, r, err)
		return
	}

	var resp proto.StorageListResponse
	if err := h.service.StorageList(ctx, &req, &resp); err != nil {
		duh.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &resp)
}

func (h *HTTPHandler) StorageInspect(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req proto.StorageInspectRequest
	if err := duh.ReadRequest(r, &req); err != nil {
		duh.ReplyError(w, r, err)
		return
	}

	var resp proto.StorageItem
	if err := h.service.StorageInspect(ctx, &req, &resp); err != nil {
		duh.ReplyError(w, r, err)
		return
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
