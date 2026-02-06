package transport

import (
	"context"

	pb "github.com/kapetan-io/querator/proto"
)

// QueueOps handles high-frequency queue operations
type QueueOps interface {
	QueueProduce(context.Context, *pb.QueueProduceRequest) error
	QueueLease(context.Context, *pb.QueueLeaseRequest, *pb.QueueLeaseResponse) error
	QueueComplete(context.Context, *pb.QueueCompleteRequest) error
	QueueRetry(context.Context, *pb.QueueRetryRequest) error
	QueueStats(context.Context, *pb.QueueStatsRequest, *pb.QueueStatsResponse) error
	QueueClear(context.Context, *pb.QueueClearRequest) error
	QueueReload(context.Context, *pb.QueueReloadRequest) error
}

// QueueAdmin handles queue lifecycle management
type QueueAdmin interface {
	QueuesCreate(context.Context, *pb.QueueInfo) error
	QueuesList(context.Context, *pb.QueuesListRequest, *pb.QueuesListResponse) error
	QueuesUpdate(context.Context, *pb.QueueInfo) error
	QueuesDelete(context.Context, *pb.QueuesDeleteRequest) error
	QueuesInfo(context.Context, *pb.QueuesInfoRequest, *pb.QueueInfo) error
}

// StorageInspector handles direct storage access
type StorageInspector interface {
	StorageItemsList(context.Context, *pb.StorageItemsListRequest, *pb.StorageItemsListResponse) error
	StorageItemsImport(context.Context, *pb.StorageItemsImportRequest, *pb.StorageItemsImportResponse) error
	StorageItemsDelete(context.Context, *pb.StorageItemsDeleteRequest) error
	StorageScheduledList(context.Context, *pb.StorageItemsListRequest, *pb.StorageItemsListResponse) error
}

// NamespaceAdmin handles namespace lifecycle management
type NamespaceAdmin interface {
	NamespacesCreate(context.Context, *pb.NamespaceInfo) error
	NamespacesList(context.Context, *pb.NamespacesListRequest, *pb.NamespacesListResponse) error
	NamespacesDelete(context.Context, *pb.NamespacesDeleteRequest) error
}

// UsersAdmin handles user lifecycle management
type UsersAdmin interface {
	UsersCreate(context.Context, *pb.UserCreateRequest, *pb.UserCreateResponse) error
	UsersList(context.Context, *pb.UsersListRequest, *pb.UsersListResponse) error
	UsersDelete(context.Context, *pb.UsersDeleteRequest) error
}

// APIKeysAdmin handles API key lifecycle management
type APIKeysAdmin interface {
	APIKeysCreate(context.Context, *pb.APIKeyCreateRequest, *pb.APIKeyCreateResponse) error
	APIKeysList(context.Context, *pb.APIKeysListRequest, *pb.APIKeysListResponse) error
	APIKeysDelete(context.Context, *pb.APIKeysDeleteRequest) error
}

// RolesAdmin handles role lifecycle management
type RolesAdmin interface {
	RolesCreate(context.Context, *pb.RoleCreateRequest, *pb.RoleCreateResponse) error
	RolesList(context.Context, *pb.RolesListRequest, *pb.RolesListResponse) error
	RolesUpdate(context.Context, *pb.RoleUpdateRequest) error
	RolesDelete(context.Context, *pb.RolesDeleteRequest) error
}

// RoleBindingsAdmin handles role binding lifecycle management
type RoleBindingsAdmin interface {
	RoleBindingsCreate(context.Context, *pb.RoleBindingCreateRequest, *pb.RoleBindingCreateResponse) error
	RoleBindingsList(context.Context, *pb.RoleBindingsListRequest, *pb.RoleBindingsListResponse) error
	RoleBindingsDelete(context.Context, *pb.RoleBindingDeleteRequest) error
}

// Service combines all interfaces for backward compatibility
type Service interface {
	QueueOps
	QueueAdmin
	StorageInspector
	NamespaceAdmin
	UsersAdmin
	APIKeysAdmin
	RolesAdmin
	RoleBindingsAdmin
	Health(context.Context) (*HealthResponse, error)
	// GetQueueNamespace returns the namespace for a given queue name.
	// Used by the HTTP layer for authorization checks.
	GetQueueNamespace(ctx context.Context, queueName string) (string, error)
}
