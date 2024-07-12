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

	RPCQueueList   = "/v1/queue.list"
	RPCQueueCreate = "/v1/queue.create"
	RPCQueueDelete = "/v1/queue.delete"
	RPCQueueUpdate = "/v1/queue.update"

	RPCStorageQueueList   = "/v1/storage/queue.list"
	RPCStorageQueueAdd    = "/v1/storage/queue.add"
	RPCStorageQueueDelete = "/v1/storage/queue.delete"
	RPCStorageQueueStats  = "/v1/storage/queue.stats"

	RPCStorageScheduleList     = "/v1/storage/schedule.list"
	RPCStorageScheduleQueueAdd = "/v1/storage/schedule.add"
	RPCStorageScheduleDelete   = "/v1/storage/schedule.delete"
	RPCStorageScheduleStats    = "/v1/storage/schedule.stats"
)

// Service exists to provide an abstraction from other public capabilities.
//
// Abstraction rules dictate that the `transport` package should NOT access any other public interfaces other
// than `Service`. To expose other public interface capabilities via the HTTP interface, we must first add that
// capability to the `Service` first.
//
// NOTE: Golang circular dependency rules work with us to help remind developers that we should not break our
// public HTTP abstraction rule. The `Service` interface avoids a circular dependency on any other part of the
// code because `transport` package is imported by most packages in the code base.
type Service interface {
	QueueProduce(context.Context, *proto.QueueProduceRequest) error
	QueueCreate(context.Context, *proto.QueueOptions) error
	QueueReserve(context.Context, *proto.QueueReserveRequest, *proto.QueueReserveResponse) error
	QueueComplete(context.Context, *proto.QueueCompleteRequest) error

	StorageQueueList(context.Context, *proto.StorageQueueListRequest, *proto.StorageQueueListResponse) error
	StorageQueueAdd(context.Context, *proto.StorageQueueAddRequest, *proto.StorageQueueAddResponse) error
	StorageQueueDelete(context.Context, *proto.StorageQueueDeleteRequest) error
	StorageQueueStats(context.Context, *proto.StorageQueueStatsRequest, *proto.StorageQueueStatsResponse) error
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

	// TODO: Implement a custom duh.Reply method to capture internal errors and log them
	//  instead of returning them to the caller.

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
	case RPCQueueUpdate:
	case RPCStorageQueueList:
		h.StorageQueueList(ctx, w, r)
		return
	case RPCStorageQueueAdd:
		h.StorageQueueAdd(ctx, w, r)
		return
	case RPCStorageQueueDelete:
		h.StorageQueueDelete(ctx, w, r)
		return
	case RPCStorageQueueStats:
		h.StorageQueueStats(ctx, w, r)
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

func (h *HTTPHandler) StorageQueueList(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req proto.StorageQueueListRequest
	if err := duh.ReadRequest(r, &req); err != nil {
		duh.ReplyError(w, r, err)
		return
	}

	var resp proto.StorageQueueListResponse
	if err := h.service.StorageQueueList(ctx, &req, &resp); err != nil {
		duh.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &resp)
}

func (h *HTTPHandler) StorageQueueAdd(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req proto.StorageQueueAddRequest
	if err := duh.ReadRequest(r, &req); err != nil {
		duh.ReplyError(w, r, err)
		return
	}

	var resp proto.StorageQueueAddResponse
	if err := h.service.StorageQueueAdd(ctx, &req, &resp); err != nil {
		duh.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &resp)
}

func (h *HTTPHandler) StorageQueueDelete(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req proto.StorageQueueDeleteRequest
	if err := duh.ReadRequest(r, &req); err != nil {
		duh.ReplyError(w, r, err)
		return
	}

	if err := h.service.StorageQueueDelete(ctx, &req); err != nil {
		duh.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &v1.Reply{Code: duh.CodeOK})
}

func (h *HTTPHandler) StorageQueueStats(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req proto.StorageQueueStatsRequest
	if err := duh.ReadRequest(r, &req); err != nil {
		duh.ReplyError(w, r, err)
		return
	}

	var resp proto.StorageQueueStatsResponse
	if err := h.service.StorageQueueStats(ctx, &req, &resp); err != nil {
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
