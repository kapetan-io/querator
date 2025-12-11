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
	"encoding/json"
	"errors"
	"fmt"
	"github.com/duh-rpc/duh-go"
	v1 "github.com/duh-rpc/duh-go/proto/v1"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/tackle/set"
	"github.com/prometheus/client_golang/prometheus"
	"log/slog"
	"net/http"
)

// TODO: Document pause in OpenAPI, "Pauses queue processing such that requests to produce, lease,
//  retry and complete are all paused. While a queue is on pause, Querator will queue those requests
//  until the pause is lifted". /v1/queue API requests can still timeout
//  NOTE: This does not effect /v1/storage/ or /v1/queue.list,create,delete,update API requests.

const (
	RPCQueueProduce  = "/v1/queue.produce"
	RPCQueueLease    = "/v1/queue.lease"
	RPCQueueComplete = "/v1/queue.complete"
	RPCQueueRetry    = "/v1/queue.retry"
	RPCQueueStats    = "/v1/queue.stats"
	RPCQueueClear    = "/v1/queue.clear"
	RPCQueueReload   = "/v1/queue.reload"

	RPCQueuesInfo      = "/v1/queues.info"
	RPCQueuesRebalance = "/v1/queues.rebalance"
	RPCQueuesList      = "/v1/queues.list"
	RPCQueuesCreate    = "/v1/queues.create"
	RPCQueuesDelete    = "/v1/queues.delete"
	RPCQueuesUpdate    = "/v1/queues.update"

	// TODO: Document the /storage/queue.list endpoint. The results include the pivot intentionally. Clients who
	//  wish to iterate through all the items page by page should account for this. Also clients must check if the
	//  pivot is the first item, because if the pivot is missing from the data store the API will return the next
	//  item after the pivot. This is an extremely efficient way to iterate through a SQL RDBMS `primary_key > pivot`
	//
	// DOC: If the pivot does not exist when calling StoreItemsList(), the endpoint will return
	// the nearest next item in the list to the pivot provided. It is up to the caller to verify the list of
	// items returned begins with the id specified in the pivot. This allows users to iterate through a constantly
	// moving list without constantly running into "pivot not found" errors.

	// TODO: We should define a maximum payload size and now allow clients to send or receive larger than expected
	//   payloads. I think DUH should do this for us when via duh.ReadRequest(r, &req, maxSize)

	RPCStorageItemsList   = "/v1/storage/items.list"
	RPCStorageItemsImport = "/v1/storage/items.import"
	RPCStorageItemsDelete = "/v1/storage/items.delete"

	// TODO: Add this endpoint to openapi.yaml

	RPCStorageScheduledList     = "/v1/storage/scheduled.list"
	RPCStorageScheduledQueueAdd = "/v1/storage/scheduled.add"
	RPCStorageScheduledDelete   = "/v1/storage/scheduled.delete"
)

type HTTPHandler struct {
	duration       *prometheus.SummaryVec
	log            *slog.Logger
	metrics        http.Handler
	service        Service
	maxProduceSize int64
}

func NewHTTPHandler(s Service, metrics http.Handler, maxProduceSize int64, log *slog.Logger) *HTTPHandler {
	set.Default(&maxProduceSize, int64(5*duh.MegaByte))

	return &HTTPHandler{
		duration: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Name: "http_handler_duration",
			Help: "The timings of http requests handled by the service",
			Objectives: map[float64]float64{
				0.5:  0.05,
				0.99: 0.001,
			},
		}, []string{"path"}),
		maxProduceSize: maxProduceSize,
		metrics:        metrics,
		log:            log,
		service:        s,
	}
}

func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer prometheus.NewTimer(h.duration.WithLabelValues(r.URL.Path)).ObserveDuration()
	ctx := r.Context()

	if r.URL.Path == "/metrics" && r.Method == http.MethodGet {
		h.metrics.ServeHTTP(w, r)
		return
	}

	if r.URL.Path == "/health" && r.Method == http.MethodGet {
		h.Health(ctx, w, r)
		return
	}

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
	case RPCQueueLease:
		h.QueueLease(ctx, w, r)
		return
	case RPCQueueRetry:
		h.QueueRetry(ctx, w, r)
		return
	case RPCQueueComplete:
		h.QueueComplete(ctx, w, r)
		return
	case RPCQueueStats:
		h.QueueStats(ctx, w, r)
		return
	case RPCQueueClear:
		h.QueueClear(ctx, w, r)
		return
	case RPCQueueReload:
		h.QueueReload(ctx, w, r)
		return
	case RPCQueuesCreate:
		h.QueuesCreate(ctx, w, r)
		return
	case RPCQueuesList:
		h.QueuesList(ctx, w, r)
		return
	case RPCQueuesUpdate:
		h.QueuesUpdate(ctx, w, r)
		return
	case RPCQueuesDelete:
		h.QueuesDelete(ctx, w, r)
		return
	case RPCQueuesInfo:
		h.QueuesInfo(ctx, w, r)
		return
	case RPCStorageItemsList:
		h.StorageItemsList(ctx, w, r)
		return
	case RPCStorageItemsImport:
		h.StorageItemsImport(ctx, w, r)
		return
	case RPCStorageItemsDelete:
		h.StorageItemsDelete(ctx, w, r)
		return
	case RPCStorageScheduledList:
		h.StorageScheduledList(ctx, w, r)
		return
	}
	duh.ReplyWithCode(w, r, duh.CodeNotImplemented, nil, "no such method; "+r.URL.Path)
}

func (h *HTTPHandler) QueueProduce(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req pb.QueueProduceRequest
	if err := duh.ReadRequest(r, &req, h.maxProduceSize); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	if err := h.service.QueueProduce(ctx, &req); err != nil {
		h.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &v1.Reply{Code: duh.CodeOK})
}

func (h *HTTPHandler) QueueLease(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req pb.QueueLeaseRequest
	if err := duh.ReadRequest(r, &req, 512*duh.Bytes); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	var resp pb.QueueLeaseResponse
	if err := h.service.QueueLease(ctx, &req, &resp); err != nil {
		h.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &resp)
}

func (h *HTTPHandler) QueueComplete(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req pb.QueueCompleteRequest
	if err := duh.ReadRequest(r, &req, 256*duh.Kilobyte); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	if err := h.service.QueueComplete(ctx, &req); err != nil {
		h.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &v1.Reply{Code: duh.CodeOK})
}

func (h *HTTPHandler) QueueRetry(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req pb.QueueRetryRequest
	if err := duh.ReadRequest(r, &req, 256*duh.Kilobyte); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	if err := h.service.QueueRetry(ctx, &req); err != nil {
		h.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &v1.Reply{Code: duh.CodeOK})
}

// -------------------------------------------------
// API to manage lists of queues
// -------------------------------------------------

func (h *HTTPHandler) QueuesCreate(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req pb.QueueInfo
	if err := duh.ReadRequest(r, &req, 256*duh.Kilobyte); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	if err := h.service.QueuesCreate(ctx, &req); err != nil {
		h.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &v1.Reply{Code: duh.CodeOK})
}

func (h *HTTPHandler) QueuesList(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req pb.QueuesListRequest
	if err := duh.ReadRequest(r, &req, 256*duh.Kilobyte); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	var resp pb.QueuesListResponse
	if err := h.service.QueuesList(ctx, &req, &resp); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	duh.Reply(w, r, duh.CodeOK, &resp)
}

func (h *HTTPHandler) QueuesUpdate(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req pb.QueueInfo
	if err := duh.ReadRequest(r, &req, 512*duh.Kilobyte); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	if err := h.service.QueuesUpdate(ctx, &req); err != nil {
		h.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &v1.Reply{Code: duh.CodeOK})
}

func (h *HTTPHandler) QueuesDelete(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req pb.QueuesDeleteRequest
	if err := duh.ReadRequest(r, &req, 256*duh.Kilobyte); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	if err := h.service.QueuesDelete(ctx, &req); err != nil {
		h.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &v1.Reply{Code: duh.CodeOK})
}

func (h *HTTPHandler) QueuesInfo(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req pb.QueuesInfoRequest
	if err := duh.ReadRequest(r, &req, 256*duh.Kilobyte); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	var resp pb.QueueInfo
	if err := h.service.QueuesInfo(ctx, &req, &resp); err != nil {
		h.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &resp)
}

func (h *HTTPHandler) QueueStats(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req pb.QueueStatsRequest
	if err := duh.ReadRequest(r, &req, 512*duh.Bytes); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	var resp pb.QueueStatsResponse
	if err := h.service.QueueStats(ctx, &req, &resp); err != nil {
		h.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &resp)
}

func (h *HTTPHandler) QueueClear(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req pb.QueueClearRequest
	if err := duh.ReadRequest(r, &req, 512*duh.Bytes); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	if err := h.service.QueueClear(ctx, &req); err != nil {
		h.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &v1.Reply{Code: duh.CodeOK})
}

func (h *HTTPHandler) QueueReload(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req pb.QueueReloadRequest
	if err := duh.ReadRequest(r, &req, 512*duh.Bytes); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	if err := h.service.QueueReload(ctx, &req); err != nil {
		h.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &v1.Reply{Code: duh.CodeOK})
}

func (h *HTTPHandler) StorageItemsList(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req pb.StorageItemsListRequest
	if err := duh.ReadRequest(r, &req, 256*duh.Kilobyte); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	var resp pb.StorageItemsListResponse
	if err := h.service.StorageItemsList(ctx, &req, &resp); err != nil {
		h.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &resp)
}

func (h *HTTPHandler) StorageScheduledList(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req pb.StorageItemsListRequest
	if err := duh.ReadRequest(r, &req, 256*duh.Kilobyte); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	var resp pb.StorageItemsListResponse
	if err := h.service.StorageScheduledList(ctx, &req, &resp); err != nil {
		h.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &resp)
}

func (h *HTTPHandler) StorageItemsImport(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req pb.StorageItemsImportRequest
	if err := duh.ReadRequest(r, &req, 64*duh.MegaByte); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	var resp pb.StorageItemsImportResponse
	if err := h.service.StorageItemsImport(ctx, &req, &resp); err != nil {
		h.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &resp)
}

func (h *HTTPHandler) StorageItemsDelete(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req pb.StorageItemsDeleteRequest
	if err := duh.ReadRequest(r, &req, 256*duh.Kilobyte); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	if err := h.service.StorageItemsDelete(ctx, &req); err != nil {
		h.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &v1.Reply{Code: duh.CodeOK})
}

func (h *HTTPHandler) Health(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	health, err := h.service.Health(ctx)
	if err != nil {
		h.ReplyError(w, r, err)
		return
	}

	w.Header().Set("Content-Type", "application/health+json")

	statusCode := http.StatusOK
	if health.Status == HealthStatusFail {
		statusCode = http.StatusServiceUnavailable
	}

	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(health); err != nil {
		h.log.Error("failed to encode health response", "error", err)
	}
}

// Describe fetches prometheus metrics to be registered
func (h *HTTPHandler) Describe(ch chan<- *prometheus.Desc) {
	h.duration.Describe(ch)
}

// Collect fetches metrics from the server for use by prometheus
func (h *HTTPHandler) Collect(ch chan<- prometheus.Metric) {
	h.duration.Collect(ch)
}

func (h *HTTPHandler) ReplyError(w http.ResponseWriter, r *http.Request, err error) {
	var re duh.Error
	if errors.As(err, &re) {
		duh.Reply(w, r, re.Code(), re.ProtoMessage())
		return
	}
	// TODO: Extract error.Fields and add them to the log fields

	h.log.Error(err.Error(),
		"location", "HTTPHandler",
		"http.request.status", duh.CodeInternalError,
		"http.request.url", r.URL.String(),
		"http.request.headers", r.Header,
		"http.request.useragent", r.Header.Get("user-agent"),
	)
	duh.ReplyWithCode(w, r, duh.CodeInternalError, nil, "Internal Error")
}
