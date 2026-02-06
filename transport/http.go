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
	"log/slog"
	"net/http"
	"strings"

	"github.com/duh-rpc/duh-go"
	v1 "github.com/duh-rpc/duh-go/proto/v1"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/tackle/set"
	"github.com/prometheus/client_golang/prometheus"
)

// Principal represents an authenticated entity making a request
type Principal struct {
	NamespaceScope *string
	UserID         string
	Username       string
}

// AnonymousPrincipal represents an unauthenticated user
var AnonymousPrincipal = Principal{Username: "anonymous"}

// AuthBackend defines the interface for authentication and authorization in the HTTP layer.
// This interface is defined locally to avoid import cycles with internal/auth.
type AuthBackend interface {
	// Authenticate validates a token and returns the principal
	Authenticate(ctx context.Context, token string) (Principal, error)
	// HasPermission checks if a principal has a permission in a namespace
	HasPermission(ctx context.Context, principal Principal, targetNS string, perm string) (bool, error)
}

// NoOpAuthBackend is an AuthBackend that always allows access (default open)
type NoOpAuthBackend struct{}

// Authenticate returns the anonymous principal
func (n *NoOpAuthBackend) Authenticate(_ context.Context, _ string) (Principal, error) {
	return AnonymousPrincipal, nil
}

// HasPermission always returns true
func (n *NoOpAuthBackend) HasPermission(_ context.Context, _ Principal, _ string, _ string) (bool, error) {
	return true, nil
}

type contextKey string

const principalKey contextKey = "principal"

// PrincipalFromContext extracts the Principal from the context
func PrincipalFromContext(ctx context.Context) Principal {
	p, ok := ctx.Value(principalKey).(Principal)
	if !ok {
		return AnonymousPrincipal
	}
	return p
}

// ContextWithPrincipal returns a new context with the Principal stored in it
func ContextWithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey, p)
}

// Permission constants for authorization checks
const (
	PermNamespaceCreate = "namespace.create"
	PermNamespaceDelete = "namespace.delete"
	PermNamespaceList   = "namespace.list"

	PermQueueCreate   = "queue.create"
	PermQueueDelete   = "queue.delete"
	PermQueueUpdate   = "queue.update"
	PermQueueList     = "queue.list"
	PermQueueProduce  = "queue.produce"
	PermQueueLease    = "queue.lease"
	PermQueueComplete = "queue.complete"
	PermQueueRetry    = "queue.retry"
	PermQueueStats    = "queue.stats"
	PermQueueClear    = "queue.clear"

	PermUserCreate = "user.create"
	PermUserDelete = "user.delete"
	PermUserList   = "user.list"

	PermAPIKeyCreate = "apikey.create"
	PermAPIKeyDelete = "apikey.delete"
	PermAPIKeyList   = "apikey.list"

	PermRoleCreate        = "role.create"
	PermRoleUpdate        = "role.update"
	PermRoleDelete        = "role.delete"
	PermRoleList          = "role.list"
	PermRoleBindingCreate = "rolebinding.create"
	PermRoleBindingDelete = "rolebinding.delete"
	PermRoleBindingList   = "rolebinding.list"

	// SystemNamespace is the reserved namespace for system-level resources
	SystemNamespace = "_system"
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

	RPCNamespacesCreate = "/v1/namespaces.create"
	RPCNamespacesList   = "/v1/namespaces.list"
	RPCNamespacesDelete = "/v1/namespaces.delete"

	RPCUsersCreate = "/v1/users.create"
	RPCUsersList   = "/v1/users.list"
	RPCUsersDelete = "/v1/users.delete"

	RPCAPIKeysCreate = "/v1/apikeys.create"
	RPCAPIKeysList   = "/v1/apikeys.list"
	RPCAPIKeysDelete = "/v1/apikeys.delete"

	RPCRolesCreate = "/v1/roles.create"
	RPCRolesList   = "/v1/roles.list"
	RPCRolesUpdate = "/v1/roles.update"
	RPCRolesDelete = "/v1/roles.delete"

	RPCRoleBindingsCreate = "/v1/rolebindings.create"
	RPCRoleBindingsList   = "/v1/rolebindings.list"
	RPCRoleBindingsDelete = "/v1/rolebindings.delete"
)

type HTTPHandler struct {
	duration       *prometheus.SummaryVec
	maxProduceSize int64
	metrics        http.Handler
	service        Service
	auth           AuthBackend
	log            *slog.Logger
}

// HTTPHandlerConfig configures the HTTPHandler
type HTTPHandlerConfig struct {
	MaxProduceSize int64
	AuthBackend    AuthBackend
	Metrics        http.Handler
	Service        Service
	Log            *slog.Logger
}

func NewHTTPHandler(conf HTTPHandlerConfig) *HTTPHandler {
	set.Default(&conf.MaxProduceSize, int64(5*duh.MegaByte))
	set.Default(&conf.AuthBackend, &NoOpAuthBackend{})

	return &HTTPHandler{
		duration: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Name: "http_handler_duration",
			Help: "The timings of http requests handled by the service",
			Objectives: map[float64]float64{
				0.5:  0.05,
				0.99: 0.001,
			},
		}, []string{"path"}),
		maxProduceSize: conf.MaxProduceSize,
		metrics:        conf.Metrics,
		service:        conf.Service,
		auth:           conf.AuthBackend,
		log:            conf.Log,
	}
}

// authenticate extracts and validates credentials from the request
func (h *HTTPHandler) authenticate(ctx context.Context, r *http.Request) (context.Context, error) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		principal, err := h.auth.Authenticate(ctx, "")
		if err != nil {
			return ctx, NewUnauthorized("authentication required")
		}
		return ContextWithPrincipal(ctx, principal), nil
	}

	const bearerPrefix = "Bearer "
	if !strings.HasPrefix(authHeader, bearerPrefix) {
		return ctx, NewUnauthorized("invalid authorization header format")
	}
	token := strings.TrimPrefix(authHeader, bearerPrefix)

	principal, err := h.auth.Authenticate(ctx, token)
	if err != nil {
		return ctx, err
	}
	return ContextWithPrincipal(ctx, principal), nil
}

// authorize checks if the principal has the required permission in the namespace
func (h *HTTPHandler) authorize(ctx context.Context, namespace, permission string) error {
	principal := PrincipalFromContext(ctx)
	hasPermission, err := h.auth.HasPermission(ctx, principal, namespace, permission)
	if err != nil {
		return NewRequestFailed("authorization check failed: %s", err.Error())
	}
	if !hasPermission {
		return NewForbidden("access denied")
	}
	return nil
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

	// Authenticate the request
	ctx, err := h.authenticate(ctx, r)
	if err != nil {
		h.ReplyError(w, r, err)
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
	case RPCNamespacesCreate:
		h.NamespacesCreate(ctx, w, r)
		return
	case RPCNamespacesList:
		h.NamespacesList(ctx, w, r)
		return
	case RPCNamespacesDelete:
		h.NamespacesDelete(ctx, w, r)
		return
	case RPCUsersCreate:
		h.UsersCreate(ctx, w, r)
		return
	case RPCUsersList:
		h.UsersList(ctx, w, r)
		return
	case RPCUsersDelete:
		h.UsersDelete(ctx, w, r)
		return
	case RPCAPIKeysCreate:
		h.APIKeysCreate(ctx, w, r)
		return
	case RPCAPIKeysList:
		h.APIKeysList(ctx, w, r)
		return
	case RPCAPIKeysDelete:
		h.APIKeysDelete(ctx, w, r)
		return
	case RPCRolesCreate:
		h.RolesCreate(ctx, w, r)
		return
	case RPCRolesList:
		h.RolesList(ctx, w, r)
		return
	case RPCRolesUpdate:
		h.RolesUpdate(ctx, w, r)
		return
	case RPCRolesDelete:
		h.RolesDelete(ctx, w, r)
		return
	case RPCRoleBindingsCreate:
		h.RoleBindingsCreate(ctx, w, r)
		return
	case RPCRoleBindingsList:
		h.RoleBindingsList(ctx, w, r)
		return
	case RPCRoleBindingsDelete:
		h.RoleBindingsDelete(ctx, w, r)
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

	// Get queue namespace for authorization
	ns, err := h.service.GetQueueNamespace(ctx, req.QueueName)
	if err != nil {
		h.ReplyError(w, r, err)
		return
	}
	if err := h.authorize(ctx, ns, PermQueueProduce); err != nil {
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

	// Get queue namespace for authorization
	ns, err := h.service.GetQueueNamespace(ctx, req.QueueName)
	if err != nil {
		h.ReplyError(w, r, err)
		return
	}
	if err := h.authorize(ctx, ns, PermQueueLease); err != nil {
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

	// Get queue namespace for authorization
	ns, err := h.service.GetQueueNamespace(ctx, req.QueueName)
	if err != nil {
		h.ReplyError(w, r, err)
		return
	}
	if err := h.authorize(ctx, ns, PermQueueComplete); err != nil {
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

	// Get queue namespace for authorization
	ns, err := h.service.GetQueueNamespace(ctx, req.QueueName)
	if err != nil {
		h.ReplyError(w, r, err)
		return
	}
	if err := h.authorize(ctx, ns, PermQueueRetry); err != nil {
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

	// Authorize against the namespace from the request
	ns := req.Namespace
	if ns == "" {
		ns = SystemNamespace
	}
	if err := h.authorize(ctx, ns, PermQueueCreate); err != nil {
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

	// QueuesList requires system-level permission
	if err := h.authorize(ctx, SystemNamespace, PermQueueList); err != nil {
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

	// Get queue namespace for authorization
	ns, err := h.service.GetQueueNamespace(ctx, req.QueueName)
	if err != nil {
		h.ReplyError(w, r, err)
		return
	}
	if err := h.authorize(ctx, ns, PermQueueStats); err != nil {
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

	// Get queue namespace for authorization
	ns, err := h.service.GetQueueNamespace(ctx, req.QueueName)
	if err != nil {
		h.ReplyError(w, r, err)
		return
	}
	if err := h.authorize(ctx, ns, PermQueueStats); err != nil {
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

	// Get queue namespace for authorization
	ns, err := h.service.GetQueueNamespace(ctx, req.QueueName)
	if err != nil {
		h.ReplyError(w, r, err)
		return
	}
	if err := h.authorize(ctx, ns, PermQueueClear); err != nil {
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

	// Get queue namespace for authorization
	ns, err := h.service.GetQueueNamespace(ctx, req.QueueName)
	if err != nil {
		h.ReplyError(w, r, err)
		return
	}
	if err := h.authorize(ctx, ns, PermQueueUpdate); err != nil {
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

	// Get queue namespace for authorization
	ns, err := h.service.GetQueueNamespace(ctx, req.QueueName)
	if err != nil {
		h.ReplyError(w, r, err)
		return
	}
	if err := h.authorize(ctx, ns, PermQueueStats); err != nil {
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

	// Get queue namespace for authorization
	ns, err := h.service.GetQueueNamespace(ctx, req.QueueName)
	if err != nil {
		h.ReplyError(w, r, err)
		return
	}
	if err := h.authorize(ctx, ns, PermQueueStats); err != nil {
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

	// Get queue namespace for authorization
	ns, err := h.service.GetQueueNamespace(ctx, req.QueueName)
	if err != nil {
		h.ReplyError(w, r, err)
		return
	}
	if err := h.authorize(ctx, ns, PermQueueProduce); err != nil {
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

	// Get queue namespace for authorization
	ns, err := h.service.GetQueueNamespace(ctx, req.QueueName)
	if err != nil {
		h.ReplyError(w, r, err)
		return
	}
	if err := h.authorize(ctx, ns, PermQueueComplete); err != nil {
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

// -------------------------------------------------
// Namespace Management API
// -------------------------------------------------

func (h *HTTPHandler) NamespacesCreate(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req pb.NamespaceInfo
	if err := duh.ReadRequest(r, &req, 256*duh.Kilobyte); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	// Namespace operations require system-level permission
	if err := h.authorize(ctx, SystemNamespace, PermNamespaceCreate); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	if err := h.service.NamespacesCreate(ctx, &req); err != nil {
		h.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &v1.Reply{Code: duh.CodeOK})
}

func (h *HTTPHandler) NamespacesList(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req pb.NamespacesListRequest
	if err := duh.ReadRequest(r, &req, 256*duh.Kilobyte); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	// Namespace operations require system-level permission
	if err := h.authorize(ctx, SystemNamespace, PermNamespaceList); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	var resp pb.NamespacesListResponse
	if err := h.service.NamespacesList(ctx, &req, &resp); err != nil {
		h.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &resp)
}

func (h *HTTPHandler) NamespacesDelete(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req pb.NamespacesDeleteRequest
	if err := duh.ReadRequest(r, &req, 256*duh.Kilobyte); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	// Namespace operations require system-level permission
	if err := h.authorize(ctx, SystemNamespace, PermNamespaceDelete); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	if err := h.service.NamespacesDelete(ctx, &req); err != nil {
		h.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &v1.Reply{Code: duh.CodeOK})
}

// -------------------------------------------------
// User Management API
// -------------------------------------------------

func (h *HTTPHandler) UsersCreate(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req pb.UserCreateRequest
	if err := duh.ReadRequest(r, &req, 256*duh.Kilobyte); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	// User operations require system-level permission
	if err := h.authorize(ctx, SystemNamespace, PermUserCreate); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	var resp pb.UserCreateResponse
	if err := h.service.UsersCreate(ctx, &req, &resp); err != nil {
		h.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &resp)
}

func (h *HTTPHandler) UsersList(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req pb.UsersListRequest
	if err := duh.ReadRequest(r, &req, 256*duh.Kilobyte); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	// User operations require system-level permission
	if err := h.authorize(ctx, SystemNamespace, PermUserList); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	var resp pb.UsersListResponse
	if err := h.service.UsersList(ctx, &req, &resp); err != nil {
		h.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &resp)
}

func (h *HTTPHandler) UsersDelete(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req pb.UsersDeleteRequest
	if err := duh.ReadRequest(r, &req, 256*duh.Kilobyte); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	// User operations require system-level permission
	if err := h.authorize(ctx, SystemNamespace, PermUserDelete); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	if err := h.service.UsersDelete(ctx, &req); err != nil {
		h.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &v1.Reply{Code: duh.CodeOK})
}

// -------------------------------------------------
// API Key Management API
// -------------------------------------------------

func (h *HTTPHandler) APIKeysCreate(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req pb.APIKeyCreateRequest
	if err := duh.ReadRequest(r, &req, 256*duh.Kilobyte); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	// API key operations require system-level permission
	if err := h.authorize(ctx, SystemNamespace, PermAPIKeyCreate); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	var resp pb.APIKeyCreateResponse
	if err := h.service.APIKeysCreate(ctx, &req, &resp); err != nil {
		h.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &resp)
}

func (h *HTTPHandler) APIKeysList(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req pb.APIKeysListRequest
	if err := duh.ReadRequest(r, &req, 256*duh.Kilobyte); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	// API key operations require system-level permission
	if err := h.authorize(ctx, SystemNamespace, PermAPIKeyList); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	var resp pb.APIKeysListResponse
	if err := h.service.APIKeysList(ctx, &req, &resp); err != nil {
		h.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &resp)
}

func (h *HTTPHandler) APIKeysDelete(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req pb.APIKeysDeleteRequest
	if err := duh.ReadRequest(r, &req, 256*duh.Kilobyte); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	// API key operations require system-level permission
	if err := h.authorize(ctx, SystemNamespace, PermAPIKeyDelete); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	if err := h.service.APIKeysDelete(ctx, &req); err != nil {
		h.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &v1.Reply{Code: duh.CodeOK})
}

// -------------------------------------------------
// Role Management API
// -------------------------------------------------

func (h *HTTPHandler) RolesCreate(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req pb.RoleCreateRequest
	if err := duh.ReadRequest(r, &req, 256*duh.Kilobyte); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	// Role operations are scoped to the namespace in the request
	if err := h.authorize(ctx, req.Namespace, PermRoleCreate); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	var resp pb.RoleCreateResponse
	if err := h.service.RolesCreate(ctx, &req, &resp); err != nil {
		h.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &resp)
}

func (h *HTTPHandler) RolesList(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req pb.RolesListRequest
	if err := duh.ReadRequest(r, &req, 256*duh.Kilobyte); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	// Role operations are scoped to the namespace in the request
	if err := h.authorize(ctx, req.Namespace, PermRoleList); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	var resp pb.RolesListResponse
	if err := h.service.RolesList(ctx, &req, &resp); err != nil {
		h.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &resp)
}

func (h *HTTPHandler) RolesUpdate(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req pb.RoleUpdateRequest
	if err := duh.ReadRequest(r, &req, 256*duh.Kilobyte); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	// Role operations are scoped to the namespace in the request
	if err := h.authorize(ctx, req.Namespace, PermRoleUpdate); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	if err := h.service.RolesUpdate(ctx, &req); err != nil {
		h.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &v1.Reply{Code: duh.CodeOK})
}

func (h *HTTPHandler) RolesDelete(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req pb.RolesDeleteRequest
	if err := duh.ReadRequest(r, &req, 256*duh.Kilobyte); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	// Role operations are scoped to the namespace in the request
	if err := h.authorize(ctx, req.Namespace, PermRoleDelete); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	if err := h.service.RolesDelete(ctx, &req); err != nil {
		h.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &v1.Reply{Code: duh.CodeOK})
}

// -------------------------------------------------
// Role Binding Management API
// -------------------------------------------------

func (h *HTTPHandler) RoleBindingsCreate(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req pb.RoleBindingCreateRequest
	if err := duh.ReadRequest(r, &req, 256*duh.Kilobyte); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	// Role binding operations are scoped to the namespace in the request
	if err := h.authorize(ctx, req.Namespace, PermRoleBindingCreate); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	var resp pb.RoleBindingCreateResponse
	if err := h.service.RoleBindingsCreate(ctx, &req, &resp); err != nil {
		h.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &resp)
}

func (h *HTTPHandler) RoleBindingsList(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req pb.RoleBindingsListRequest
	if err := duh.ReadRequest(r, &req, 256*duh.Kilobyte); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	// Role binding operations are scoped to the namespace in the request
	if err := h.authorize(ctx, req.Namespace, PermRoleBindingList); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	var resp pb.RoleBindingsListResponse
	if err := h.service.RoleBindingsList(ctx, &req, &resp); err != nil {
		h.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &resp)
}

func (h *HTTPHandler) RoleBindingsDelete(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req pb.RoleBindingDeleteRequest
	if err := duh.ReadRequest(r, &req, 256*duh.Kilobyte); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	// Role binding operations are scoped to the namespace in the request
	if err := h.authorize(ctx, req.Namespace, PermRoleBindingDelete); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	if err := h.service.RoleBindingsDelete(ctx, &req); err != nil {
		h.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &v1.Reply{Code: duh.CodeOK})
}
