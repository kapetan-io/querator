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

// TODO: Document pause in OpenAPI, "Pauses queue processing such that requests to produce, reserve,
//  defer and complete are all paused. While a queue is on pause, Querator will queue those requests
//  until the pause is lifted". /v1/queue API requests can still timeout
//  NOTE: This does not effect /v1/storage/ or /v1/queue.list,create,delete,update API requests.

const (
	RPCQueueProduce  = "/v1/queue.produce"
	RPCQueueReserve  = "/v1/queue.reserve"
	RPCQueueDefer    = "/v1/queue.defer"
	RPCQueueComplete = "/v1/queue.complete"
	RPCQueueStats    = "/v1/queue.stats"
	RPCQueueClear    = "/v1/queue.clear"
	RPCQueuePause    = "/v1/queue.pause"

	RPCQueuesList   = "/v1/queues.list"
	RPCQueuesCreate = "/v1/queues.create"
	RPCQueuesDelete = "/v1/queues.delete"
	RPCQueuesUpdate = "/v1/queues.update"

	// TODO: Document the /storage/queue.list endpoint. The results include the pivot intentionally. Clients who
	//  wish to iterate through all the items page by page should account for this. Also clients must check if the
	//  pivot is the first item, because if the pivot is missing from the data store the API will return the next
	//  item after the pivot. This is an extremely efficient way to iterate through a SQL RDBMS `primary_key > pivot`

	// TODO: We should define a maximum payload size and now allow clients to send or receive larger than expected
	//   payloads. I think DUH should do this for us when via duh.ReadRequest(r, &req, maxSize)

	RPCStorageQueueList   = "/v1/storage/queue.list"
	RPCStorageQueueAdd    = "/v1/storage/queue.add"
	RPCStorageQueueDelete = "/v1/storage/queue.delete"

	RPCStorageScheduleList     = "/v1/storage/schedule.list"
	RPCStorageScheduleQueueAdd = "/v1/storage/schedule.add"
	RPCStorageScheduleDelete   = "/v1/storage/schedule.delete"
	RPCStorageScheduleStats    = "/v1/storage/schedule.stats"
)

// Service is an abstraction separating the public protocol from the underlying implementation.
//
// Abstraction rules dictate that the `transport` package should NOT access any other public interfaces or types.
// To expose new public interface capabilities via the HTTP interface, we must first add that capability to the
// `Service` first.
//
// Golang circular dependency rules work with us to help remind developers that we should not break our
// abstraction of HTTP code from the implementation. Any attempt to add a non-public facing method or types
// from other Querator packages will result in a circular dependency warning.
type Service interface {
	QueueProduce(context.Context, *proto.QueueProduceRequest) error
	QueueCreate(context.Context, *proto.QueueOptions) error
	QueueReserve(context.Context, *proto.QueueReserveRequest, *proto.QueueReserveResponse) error
	QueueComplete(context.Context, *proto.QueueCompleteRequest) error
	QueueStats(context.Context, *proto.QueueStatsRequest, *proto.QueueStatsResponse) error
	QueuePause(context.Context, *proto.QueuePauseRequest) error
	QueueClear(context.Context, *proto.QueueClearRequest) error

	StorageQueueList(context.Context, *proto.StorageQueueListRequest, *proto.StorageQueueListResponse) error
	StorageQueueAdd(context.Context, *proto.StorageQueueAddRequest, *proto.StorageQueueAddResponse) error
	StorageQueueDelete(context.Context, *proto.StorageQueueDeleteRequest) error
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
	case RPCQueueStats:
		h.QueueStats(ctx, w, r)
		return
	case RPCQueueClear:
		h.QueueStats(ctx, w, r)
		return
	case RPCQueuePause:
		h.QueuePause(ctx, w, r)
		return
	case RPCQueuesList:
	case RPCQueuesCreate:
		h.QueuesCreate(ctx, w, r)
		return
	case RPCQueuesDelete:
	case RPCQueuesUpdate:
	case RPCStorageQueueList:
		h.StorageQueueList(ctx, w, r)
		return
	case RPCStorageQueueAdd:
		h.StorageQueueAdd(ctx, w, r)
		return
	case RPCStorageQueueDelete:
		h.StorageQueueDelete(ctx, w, r)
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

func (h *HTTPHandler) QueuesCreate(ctx context.Context, w http.ResponseWriter, r *http.Request) {
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

func (h *HTTPHandler) QueueStats(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req proto.QueueStatsRequest
	if err := duh.ReadRequest(r, &req); err != nil {
		duh.ReplyError(w, r, err)
		return
	}

	var resp proto.QueueStatsResponse
	if err := h.service.QueueStats(ctx, &req, &resp); err != nil {
		duh.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &resp)
}

func (h *HTTPHandler) QueueClear(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req proto.QueueClearRequest
	if err := duh.ReadRequest(r, &req); err != nil {
		duh.ReplyError(w, r, err)
		return
	}

	if err := h.service.QueueClear(ctx, &req); err != nil {
		duh.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &v1.Reply{Code: duh.CodeOK})
}

func (h *HTTPHandler) QueuePause(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req proto.QueuePauseRequest
	if err := duh.ReadRequest(r, &req); err != nil {
		duh.ReplyError(w, r, err)
		return
	}

	if err := h.service.QueuePause(ctx, &req); err != nil {
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

// Describe fetches prometheus metrics to be registered
func (h *HTTPHandler) Describe(ch chan<- *prometheus.Desc) {
	h.duration.Describe(ch)
}

// Collect fetches metrics from the server for use by prometheus
func (h *HTTPHandler) Collect(ch chan<- prometheus.Metric) {
	h.duration.Collect(ch)
}
