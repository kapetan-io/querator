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

package service

import (
	"context"
	"log/slog"
	"time"

	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/internal"
	"github.com/kapetan-io/querator/internal/auth"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/querator/transport"
	tauth "github.com/kapetan-io/querator/transport/auth"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/set"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	DefaultListLimit = 1_000
)

type Config struct {
	// Log is the logging implementation used by this Querator instance
	Log *slog.Logger
	// StorageConfig is the configured storage backends
	StorageConfig store.Config
	// Auth is the authentication/authorization backend (optional, defaults to NoOp)
	Auth auth.AuthBackend
	// InstanceID is a unique id for this instance of Querator
	InstanceID string
	// WriteTimeout The time it should take for a single batched write to complete
	WriteTimeout clock.Duration
	// ReadTimeout The time it should take for a single batched read to complete
	ReadTimeout clock.Duration
	// MaxLeaseBatchSize is the maximum number of items a client can request in a single lease request
	MaxLeaseBatchSize int
	// MaxProduceBatchSize is the maximum number of items a client can produce in a single produce request
	MaxProduceBatchSize int
	// MaxCompleteBatchSize is the maximum number of ids a client can mark complete in a single complete request
	MaxCompleteBatchSize int
	// MaxRequestsPerQueue is the maximum number of client requests a queue can handle before it returns an
	// queue overloaded message
	MaxRequestsPerQueue int
	// MaxConcurrentConnections is the maximum number of connections allowed. Default is 1,000
	MaxConcurrentRequests int
	// Clock is a time provider used to preform time related calculations. It is configurable so that it can
	// be overridden for testing.
	Clock *clock.Provider
}

type Service struct {
	queues *internal.QueuesManager
	conf   Config
	auth   auth.AuthBackend
}

func New(conf Config) (*Service, error) {
	set.Default(&conf.Clock, clock.NewProvider())
	set.Default(&conf.Log, slog.Default())
	set.Default(&conf.ReadTimeout, 3*time.Second)
	set.Default(&conf.WriteTimeout, 3*time.Second)

	// TODO: Validate the Storage Config (somewhere) ensure that we have at least one storage backend with an affinity
	//  greater than 0.0 else QueuesManager will be unable to assign partitions.

	// TODO: The queue store should also know if the config provided is valid, IE: does every partition
	//  storage name exist in the config? If not, then it's a bad config and Querator should not start.

	qm, err := internal.NewQueuesManager(internal.QueuesManagerConfig{
		LogicalConfig: internal.LogicalConfig{
			MaxLeaseBatchSize:    conf.MaxLeaseBatchSize,
			MaxProduceBatchSize:  conf.MaxProduceBatchSize,
			MaxCompleteBatchSize: conf.MaxCompleteBatchSize,
			MaxRequestsPerQueue:  conf.MaxRequestsPerQueue,
			WriteTimeout:         conf.WriteTimeout,
			ReadTimeout:          conf.ReadTimeout,
			Clock:                conf.Clock,
		},
		StorageConfig: conf.StorageConfig,
		Log:           conf.Log,
	})
	if err != nil {
		return nil, err
	}

	// Default to NoOp auth if not provided
	if conf.Auth == nil {
		conf.Auth = &auth.NoOpAuthBackend{}
	}

	return &Service{
		queues: qm,
		conf:   conf,
		auth:   conf.Auth,
	}, nil
}

// authorize checks if the principal in context has the required permission in the namespace
func (s *Service) authorize(ctx context.Context, namespace, permission string) error {
	principal := internal.PrincipalFromContext(ctx)
	hasPermission, err := s.auth.HasPermission(ctx, principal, namespace, permission)
	if err != nil {
		return transport.NewRequestFailed("authorization check failed: %s", err.Error())
	}
	if !hasPermission {
		return transport.NewForbidden("access denied")
	}
	return nil
}

func (s *Service) QueueProduce(ctx context.Context, req *proto.QueueProduceRequest) error {
	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	proxy, logical := queue.GetNext()
	if proxy != nil {
		return proxy.QueueProduce(ctx, req)
	}

	var r types.ProduceRequest
	if err := s.validateQueueProduceProto(req, &r); err != nil {
		return err
	}

	// Produce will block until success, context cancel or timeout
	if err := logical.Produce(ctx, &r); err != nil {
		return err
	}

	return nil
}

func (s *Service) QueueLease(ctx context.Context, req *proto.QueueLeaseRequest,
	res *proto.QueueLeaseResponse) error {

	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	proxy, logical := queue.GetNext()
	if proxy != nil {
		return proxy.QueueLease(ctx, req, res)
	}

	var r types.LeaseRequest
	if err := s.validateQueueLeaseProto(req, &r); err != nil {
		return err
	}

	// Lease will block until success, context cancel or timeout
	if err := logical.Lease(ctx, &r); err != nil {
		return err
	}

	res.Partition = int32(r.Partition)
	res.QueueName = req.QueueName

	for _, item := range r.Items {
		res.Items = append(res.Items, &proto.QueueLeaseItem{
			LeaseDeadline: timestamppb.New(item.LeaseDeadline),
			Attempts:      int32(item.Attempts),
			Id:            string(item.ID),
			Reference:     item.Reference,
			Encoding:      item.Encoding,
			Bytes:         item.Payload,
			Kind:          item.Kind,
		})
	}

	return nil
}

func (s *Service) QueueComplete(ctx context.Context, req *proto.QueueCompleteRequest) error {
	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	proxy, logical, err := queue.GetByPartition(int(req.Partition))
	if err != nil {
		return err
	}

	if proxy != nil {
		return proxy.QueueComplete(ctx, req)
	}

	var r types.CompleteRequest
	if err := s.validateQueueCompleteProto(req, &r); err != nil {
		return err
	}

	// Complete will block until success, context cancel or timeout
	if err := logical.Complete(ctx, &r); err != nil {
		return err
	}

	return nil
}

func (s *Service) QueueRetry(ctx context.Context, req *proto.QueueRetryRequest) error {
	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	proxy, logical, err := queue.GetByPartition(int(req.Partition))
	if err != nil {
		return err
	}

	if proxy != nil {
		return proxy.QueueRetry(ctx, req)
	}

	var r types.RetryRequest
	if err := s.validateQueueRetryProto(req, &r); err != nil {
		return err
	}

	// Retry will block until success, context cancel or timeout
	if err := logical.Retry(ctx, &r); err != nil {
		return err
	}

	return nil
}

func (s *Service) QueueReload(ctx context.Context, req *proto.QueueReloadRequest) error {
	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	// Clear all the logical queues on this instance
	for _, logical := range queue.GetAll() {
		r := types.ReloadRequest{
			Partitions: make([]int, 0),
		}

		if err := logical.ReloadPartitions(ctx, &r); err != nil {
			// If a logical queue goes away while we are clearing
			// return the error to the client, so they know the clear request didn't complete.
			return err
		}
	}

	return nil
}

func (s *Service) QueueClear(ctx context.Context, req *proto.QueueClearRequest) error {
	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	// Clear all the logical queues on this instance
	for _, logical := range queue.GetAll() {
		r := types.ClearRequest{
			Destructive: req.Destructive,
			Scheduled:   req.Scheduled,
			Queue:       req.Queue,
		}

		if err := logical.Clear(ctx, &r); err != nil {
			// If a logical queue goes away while we are clearing
			// return the error to the client, so they know the clear request didn't complete.
			return err
		}
	}

	return nil
}

// PauseQueue is used to temporarily pause processing of queue requests to simulate various high contention scenarios
// in testing; it is not exposed to the users via API calls.
func (s *Service) PauseQueue(ctx context.Context, queueName string, pause bool) error {
	queue, err := s.queues.Get(ctx, queueName)
	if err != nil {
		return err
	}

	// Pause all the logical queues on this instance
	for _, logical := range queue.GetAll() {
		if err := logical.Pause(ctx, &types.PauseRequest{Pause: pause}); err != nil {
			return err
		}
	}

	return nil
}

// -------------------------------------------------
// API to manage lists of queues
// -------------------------------------------------

func (s *Service) QueuesCreate(ctx context.Context, req *proto.QueueInfo) error {
	var info types.QueueInfo

	if err := s.validateQueueOptionsProto(req, &info); err != nil {
		return err
	}

	_, err := s.queues.Create(ctx, info)
	if err != nil {
		return err
	}

	return nil
}

func (s *Service) QueuesList(ctx context.Context, req *proto.QueuesListRequest,
	resp *proto.QueuesListResponse) error {

	if req.Limit == 0 {
		req.Limit = DefaultListLimit
	}

	items := make([]types.QueueInfo, 0, allocInt32(req.Limit))
	if err := s.queues.List(ctx, &items, types.ListOptions{
		Pivot: types.ToItemID(req.Pivot),
		Limit: int(req.Limit),
	}); err != nil {
		return err
	}

	for _, item := range items {
		resp.Items = append(resp.Items, item.ToProto(new(proto.QueueInfo)))
	}
	return nil
}

func (s *Service) QueuesUpdate(ctx context.Context, req *proto.QueueInfo) error {
	// Validate queue name format first
	if err := validateQueueName(req.QueueName); err != nil {
		return err
	}

	var info types.QueueInfo
	if err := s.validateQueueOptionsProto(req, &info); err != nil {
		return err
	}

	// Get namespace for authorization
	ns, err := s.GetQueueNamespace(ctx, req.QueueName)
	if err != nil {
		return err
	}

	// Authorize
	if err := s.authorize(ctx, ns, tauth.QueueUpdate); err != nil {
		return err
	}

	if err := s.queues.Update(ctx, info); err != nil {
		return err
	}
	return nil
}

func (s *Service) QueuesDelete(ctx context.Context, req *proto.QueuesDeleteRequest) error {
	// Validate queue name format first
	if err := validateQueueName(req.QueueName); err != nil {
		return err
	}

	// Get namespace for authorization
	ns, err := s.GetQueueNamespace(ctx, req.QueueName)
	if err != nil {
		return err
	}

	// Authorize
	if err := s.authorize(ctx, ns, tauth.QueueDelete); err != nil {
		return err
	}

	if err := s.queues.Delete(ctx, req.QueueName); err != nil {
		return err
	}
	return nil
}

func (s *Service) QueuesInfo(ctx context.Context, req *proto.QueuesInfoRequest, resp *proto.QueueInfo) error {
	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	info := queue.Info()
	*resp = *info.ToProto(resp)
	return nil
}

// -------------------------------------------------
// API to inspect queue storage
// -------------------------------------------------

func (s *Service) StorageItemsList(ctx context.Context, req *proto.StorageItemsListRequest,
	res *proto.StorageItemsListResponse) error {
	return s.storageItemsList(ctx, types.ListItems, req, res)
}

func (s *Service) StorageScheduledList(ctx context.Context, req *proto.StorageItemsListRequest,
	res *proto.StorageItemsListResponse) error {
	return s.storageItemsList(ctx, types.ListScheduled, req, res)
}

func (s *Service) storageItemsList(ctx context.Context, kind types.ListKind,
	req *proto.StorageItemsListRequest,
	res *proto.StorageItemsListResponse) error {
	if req.Limit == 0 {
		req.Limit = DefaultListLimit
	}

	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	proxy, logical, err := queue.GetByPartition(int(req.Partition))
	if err != nil {
		return err
	}

	if proxy != nil {
		return proxy.StorageItemsList(ctx, req, res)
	}

	items := make([]*types.Item, 0, allocInt32(req.Limit))

	if err := logical.StorageItemsList(ctx, internal.StorageRequest{
		Partition: int(req.Partition),
		Options: types.ListOptions{
			Pivot: types.ToItemID(req.Pivot),
			Limit: int(req.Limit),
		},
		Items:    &items,
		ListKind: kind,
	}); err != nil {
		return err
	}

	for _, item := range items {
		res.Items = append(res.Items, item.ToProto(new(proto.StorageItem)))
	}

	return nil
}

func (s *Service) StorageItemsImport(ctx context.Context, req *proto.StorageItemsImportRequest,
	res *proto.StorageItemsImportResponse) error {

	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	proxy, logical, err := queue.GetByPartition(int(req.Partition))
	if err != nil {
		return err
	}

	if proxy != nil {
		return proxy.StorageItemsImport(ctx, req, res)
	}

	items := make([]*types.Item, 0, len(req.Items))
	for _, item := range req.Items {
		i := new(types.Item)
		items = append(items, i.FromProto(item))
	}

	if err := logical.StorageItemsImport(ctx, int(req.Partition), &items); err != nil {
		return err
	}

	for _, item := range items {
		res.Items = append(res.Items, item.ToProto(new(proto.StorageItem)))
	}

	return nil
}

func (s *Service) StorageItemsDelete(ctx context.Context, req *proto.StorageItemsDeleteRequest) error {

	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	proxy, logical, err := queue.GetByPartition(int(req.Partition))
	if err != nil {
		return err
	}

	if proxy != nil {
		return proxy.StorageItemsDelete(ctx, req)
	}

	ids := make([]types.ItemID, 0, len(req.Ids))
	for _, id := range req.Ids {
		ids = append(ids, types.ItemID(id))
	}

	if err := logical.StorageItemsDelete(ctx, int(req.Partition), ids); err != nil {
		return err
	}
	return nil
}

func (s *Service) QueueStats(ctx context.Context, req *proto.QueueStatsRequest,
	res *proto.QueueStatsResponse) error {

	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	res.QueueName = req.QueueName
	for _, logical := range queue.GetAll() {
		var stats types.LogicalStats
		if err := logical.QueueStats(ctx, &stats); err != nil {
			return err
		}

		ls := &proto.QueueLogicalStats{
			ProduceWaiting:  int32(stats.ProduceWaiting),
			LeaseWaiting:    int32(stats.LeaseWaiting),
			CompleteWaiting: int32(stats.CompleteWaiting),
			InFlight:        int32(stats.InFlight),
			Partitions:      nil,
		}

		for _, stat := range stats.Partitions {
			ls.Partitions = append(ls.Partitions,
				&proto.QueuePartitionStats{
					AverageLeasedAge: stat.AverageLeasedAge.String(),
					Partition:        int32(stat.Partition),
					TotalLeased:      int32(stat.NumLeased),
					Failures:         int32(stat.Failures),
					AverageAge:       stat.AverageAge.String(),
					Total:            int32(stat.Total),
					Scheduled:        int32(stat.Scheduled),
				})
		}
		res.LogicalQueues = append(res.LogicalQueues, ls)
	}
	return nil
}

func (s *Service) Health(ctx context.Context) (*transport.HealthResponse, error) {
	const healthTimeout = 5 * time.Second

	healthCtx, cancel := context.WithTimeout(ctx, healthTimeout)
	defer cancel()

	response := &transport.HealthResponse{
		Status:  transport.HealthStatusPass,
		Version: querator.Version,
		Checks:  make(map[string][]transport.Check),
	}

	var queues []types.QueueInfo
	err := s.queues.List(healthCtx, &queues, types.ListOptions{Limit: 1})

	check := transport.Check{
		ComponentType: "datastore",
		Time:          time.Now().UTC().Format(time.RFC3339),
	}

	if err != nil {
		check.Status = transport.HealthStatusFail
		check.Output = err.Error()
		response.Status = transport.HealthStatusFail
	} else {
		check.Status = transport.HealthStatusPass
	}

	response.Checks["queues:storage"] = []transport.Check{check}

	return response, nil
}

func (s *Service) Shutdown(ctx context.Context) error {
	// See 0015-shutdown-errors.md for a discussion of shutdown operation
	return s.queues.Shutdown(ctx)
}

// GetQueueNamespace returns the namespace for a given queue name.
// Used by the HTTP layer for authorization checks.
func (s *Service) GetQueueNamespace(ctx context.Context, queueName string) (string, error) {
	queue, err := s.queues.Get(ctx, queueName)
	if err != nil {
		return "", err
	}
	return queue.Info().Namespace, nil
}

// -------------------------------------------------
// Namespace Management API
// -------------------------------------------------

func (s *Service) NamespacesCreate(ctx context.Context, req *proto.NamespaceInfo) error {
	var ns types.Namespace

	if err := s.validateNamespaceProto(req, &ns); err != nil {
		return err
	}

	// Check for reserved namespace prefix
	if ns.IsReserved() {
		return types.ErrNamespaceReserved
	}

	ns.CreatedAt = s.conf.Clock.Now().UTC()
	if err := s.conf.StorageConfig.Namespaces.Add(ctx, ns); err != nil {
		return err
	}

	return nil
}

func (s *Service) NamespacesList(ctx context.Context, req *proto.NamespacesListRequest,
	resp *proto.NamespacesListResponse) error {

	if req.Limit == 0 {
		req.Limit = DefaultListLimit
	}

	namespaces := make([]types.Namespace, 0, allocInt32(req.Limit))
	if err := s.conf.StorageConfig.Namespaces.List(ctx, &namespaces, types.ListOptions{
		Pivot: types.ToItemID(req.Pivot),
		Limit: int(req.Limit),
	}); err != nil {
		return err
	}

	for _, ns := range namespaces {
		resp.Items = append(resp.Items, ns.ToProto(new(proto.NamespaceInfo)))
	}
	return nil
}

func (s *Service) NamespacesDelete(ctx context.Context, req *proto.NamespacesDeleteRequest) error {
	// TODO: Check if namespace has queues and return ErrNamespaceHasQueues if so
	if err := s.conf.StorageConfig.Namespaces.Delete(ctx, req.Name); err != nil {
		return err
	}
	return nil
}

// -------------------------------------------------
// User Management API
// -------------------------------------------------

func (s *Service) UsersCreate(ctx context.Context, req *proto.UserCreateRequest,
	resp *proto.UserCreateResponse) error {
	var user types.User

	if err := s.validateUserCreateProto(req, &user); err != nil {
		return err
	}

	user.ID = internal.NewUID()
	user.CreatedAt = s.conf.Clock.Now().UTC()
	user.UpdatedAt = user.CreatedAt

	if err := s.conf.StorageConfig.Users.Add(ctx, user); err != nil {
		return err
	}

	resp.Id = user.ID
	return nil
}

func (s *Service) UsersList(ctx context.Context, req *proto.UsersListRequest,
	resp *proto.UsersListResponse) error {

	if req.Limit == 0 {
		req.Limit = DefaultListLimit
	}

	users := make([]types.User, 0, allocInt32(req.Limit))
	if err := s.conf.StorageConfig.Users.List(ctx, &users, types.ListOptions{
		Pivot: types.ToItemID(req.Pivot),
		Limit: int(req.Limit),
	}); err != nil {
		return err
	}

	for _, user := range users {
		resp.Items = append(resp.Items, user.ToProto(new(proto.User)))
	}
	return nil
}

func (s *Service) UsersDelete(ctx context.Context, req *proto.UsersDeleteRequest) error {
	// Delete all role bindings for this user first (CASCADE delete)
	if err := s.conf.StorageConfig.RoleBindings.DeleteByUser(ctx, req.Id); err != nil {
		return err
	}

	// Delete all API keys for this user (CASCADE delete)
	if err := s.conf.StorageConfig.APIKeys.DeleteByUser(ctx, req.Id); err != nil {
		return err
	}

	if err := s.conf.StorageConfig.Users.Delete(ctx, req.Id); err != nil {
		return err
	}
	return nil
}

// -------------------------------------------------
// API Key Management API
// -------------------------------------------------

func (s *Service) APIKeysCreate(ctx context.Context, req *proto.APIKeyCreateRequest,
	resp *proto.APIKeyCreateResponse) error {
	var key types.APIKey

	if err := s.validateAPIKeyCreateProto(req, &key); err != nil {
		return err
	}

	// Verify the user exists
	var user types.User
	if err := s.conf.StorageConfig.Users.Get(ctx, key.UserID, &user); err != nil {
		return err
	}

	// Generate the API key
	envTag := req.EnvTag
	if envTag == "" {
		envTag = "qtr"
	}

	generated, err := auth.GenerateAPIKey(envTag)
	if err != nil {
		return transport.NewRequestFailed("failed to generate api key: %s", err.Error())
	}

	key.ID = internal.NewUID()
	key.KeyHash = generated.KeyHash
	key.KeyPrefix = generated.Prefix
	key.CreatedAt = s.conf.Clock.Now().UTC()

	if err := s.conf.StorageConfig.APIKeys.Add(ctx, key); err != nil {
		return err
	}

	resp.Prefix = generated.Prefix
	resp.Key = generated.Key
	resp.Id = key.ID
	return nil
}

func (s *Service) APIKeysList(ctx context.Context, req *proto.APIKeysListRequest,
	resp *proto.APIKeysListResponse) error {

	if req.Limit == 0 {
		req.Limit = DefaultListLimit
	}

	keys := make([]types.APIKey, 0, allocInt32(req.Limit))
	opts := types.ListOptions{
		Pivot: types.ToItemID(req.Pivot),
		Limit: int(req.Limit),
	}

	var err error
	if req.UserId != "" {
		err = s.conf.StorageConfig.APIKeys.ListByUser(ctx, req.UserId, &keys, opts)
	} else {
		err = s.conf.StorageConfig.APIKeys.List(ctx, &keys, opts)
	}
	if err != nil {
		return err
	}

	for _, key := range keys {
		resp.Items = append(resp.Items, key.ToProto(new(proto.APIKeyMetadata)))
	}
	return nil
}

func (s *Service) APIKeysDelete(ctx context.Context, req *proto.APIKeysDeleteRequest) error {
	if err := s.conf.StorageConfig.APIKeys.Delete(ctx, req.Id); err != nil {
		return err
	}
	return nil
}

// -------------------------------------------------
// Role Management API
// -------------------------------------------------

func (s *Service) RolesCreate(ctx context.Context, req *proto.RoleCreateRequest,
	resp *proto.RoleCreateResponse) error {
	var role types.Role

	if err := s.validateRoleCreateProto(req, &role); err != nil {
		return err
	}

	// Check if this is a standard role name that cannot be created
	if tauth.IsStandardRole(role.Name) {
		return types.ErrRoleIsStandard
	}

	// Validate all permissions
	for _, perm := range role.Permissions {
		if !tauth.IsValidPermission(perm) {
			return transport.NewInvalidOption("permission '%s' is invalid", perm)
		}
	}

	// Verify the namespace exists
	var ns types.Namespace
	if err := s.conf.StorageConfig.Namespaces.Get(ctx, role.Namespace, &ns); err != nil {
		return err
	}

	role.ID = internal.NewUID()
	role.CreatedAt = s.conf.Clock.Now().UTC()

	if err := s.conf.StorageConfig.Roles.Add(ctx, role); err != nil {
		return err
	}

	resp.Id = role.ID
	return nil
}

func (s *Service) RolesList(ctx context.Context, req *proto.RolesListRequest,
	resp *proto.RolesListResponse) error {

	if req.Limit == 0 {
		req.Limit = DefaultListLimit
	}

	roles := make([]types.Role, 0, allocInt32(req.Limit))
	if err := s.conf.StorageConfig.Roles.List(ctx, req.Namespace, &roles, types.ListOptions{
		Pivot: types.ToItemID(req.Pivot),
		Limit: int(req.Limit),
	}); err != nil {
		return err
	}

	for _, role := range roles {
		resp.Items = append(resp.Items, role.ToProto(new(proto.Role)))
	}
	return nil
}

func (s *Service) RolesUpdate(ctx context.Context, req *proto.RoleUpdateRequest) error {
	var role types.Role

	if err := s.validateRoleUpdateProto(req, &role); err != nil {
		return err
	}

	// Get the existing role
	var existing types.Role
	if err := s.conf.StorageConfig.Roles.Get(ctx, role.Namespace, role.Name, &existing); err != nil {
		return err
	}

	// Check if this is a standard role that cannot be updated
	if tauth.IsStandardRole(existing.Name) {
		return types.ErrRoleIsStandard
	}

	// Validate all permissions
	for _, perm := range role.Permissions {
		if !tauth.IsValidPermission(perm) {
			return transport.NewInvalidOption("permission '%s' is invalid", perm)
		}
	}

	role.ID = existing.ID
	if err := s.conf.StorageConfig.Roles.Update(ctx, role); err != nil {
		return err
	}
	return nil
}

func (s *Service) RolesDelete(ctx context.Context, req *proto.RolesDeleteRequest) error {
	// Get the role first to check standard role and get ID
	var role types.Role
	if err := s.conf.StorageConfig.Roles.Get(ctx, req.Namespace, req.Name, &role); err != nil {
		return err
	}

	// Check if this is a standard role that cannot be deleted
	if tauth.IsStandardRole(role.Name) {
		return types.ErrRoleIsStandard
	}

	// Check if role has bindings
	var bindings []types.RoleBinding
	if err := s.conf.StorageConfig.RoleBindings.ListByRole(ctx, role.ID, &bindings); err != nil {
		return err
	}
	if len(bindings) > 0 {
		return types.ErrRoleHasBindings
	}

	if err := s.conf.StorageConfig.Roles.Delete(ctx, role.ID); err != nil {
		return err
	}
	return nil
}

// -------------------------------------------------
// Role Binding Management API
// -------------------------------------------------

func (s *Service) RoleBindingsCreate(ctx context.Context, req *proto.RoleBindingCreateRequest,
	resp *proto.RoleBindingCreateResponse) error {
	var binding types.RoleBinding

	if err := s.validateRoleBindingCreateProto(req, &binding); err != nil {
		return err
	}

	// Verify the role exists in the namespace
	var role types.Role
	if err := s.conf.StorageConfig.Roles.Get(ctx, req.Namespace, req.RoleName, &role); err != nil {
		return err
	}

	// Verify the user exists
	var user types.User
	if err := s.conf.StorageConfig.Users.Get(ctx, binding.UserID, &user); err != nil {
		return err
	}

	binding.ID = internal.NewUID()
	binding.RoleID = role.ID
	binding.CreatedAt = s.conf.Clock.Now().UTC()

	if err := s.conf.StorageConfig.RoleBindings.Add(ctx, binding); err != nil {
		return err
	}

	resp.Id = binding.ID
	return nil
}

func (s *Service) RoleBindingsList(ctx context.Context, req *proto.RoleBindingsListRequest,
	resp *proto.RoleBindingsListResponse) error {

	if req.Limit == 0 {
		req.Limit = DefaultListLimit
	}

	bindings := make([]types.RoleBinding, 0, allocInt32(req.Limit))
	if err := s.conf.StorageConfig.RoleBindings.List(ctx, req.Namespace, &bindings, types.ListOptions{
		Pivot: types.ToItemID(req.Pivot),
		Limit: int(req.Limit),
	}); err != nil {
		return err
	}

	for _, binding := range bindings {
		resp.Items = append(resp.Items, binding.ToProto(new(proto.RoleBinding)))
	}
	return nil
}

func (s *Service) RoleBindingsDelete(ctx context.Context, req *proto.RoleBindingDeleteRequest) error {
	// Get the role first to find the binding
	var role types.Role
	if err := s.conf.StorageConfig.Roles.Get(ctx, req.Namespace, req.RoleName, &role); err != nil {
		return err
	}

	// Find the binding for this user and role
	var bindings []types.RoleBinding
	if err := s.conf.StorageConfig.RoleBindings.ListByRole(ctx, role.ID, &bindings); err != nil {
		return err
	}

	var bindingID string
	for _, b := range bindings {
		if b.UserID == req.UserId && b.Namespace == req.Namespace {
			bindingID = b.ID
			break
		}
	}

	if bindingID == "" {
		return types.ErrRoleBindingNotExist
	}

	if err := s.conf.StorageConfig.RoleBindings.Delete(ctx, bindingID); err != nil {
		return err
	}
	return nil
}
