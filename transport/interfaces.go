package transport

import (
	"context"

	pb "github.com/kapetan-io/querator/proto"
)

type QueueOps interface {
	QueueProduce(context.Context, *pb.QueueProduceRequest) error
	QueueLease(context.Context, *pb.QueueLeaseRequest, *pb.QueueLeaseResponse) error
	QueueComplete(context.Context, *pb.QueueCompleteRequest) error
	QueueRetry(context.Context, *pb.QueueRetryRequest) error
	QueueStats(context.Context, *pb.QueueStatsRequest, *pb.QueueStatsResponse) error
	QueueClear(context.Context, *pb.QueueClearRequest) error
	QueueReload(context.Context, *pb.QueueReloadRequest) error
}

type QueueAdmin interface {
	QueuesCreate(context.Context, *pb.QueueInfo) error
	QueuesList(context.Context, *pb.QueuesListRequest, *pb.QueuesListResponse) error
	QueuesUpdate(context.Context, *pb.QueuesUpdateRequest) error
	QueuesDelete(context.Context, *pb.QueuesDeleteRequest) error
	QueuesInfo(context.Context, *pb.QueuesInfoRequest, *pb.QueueInfo) error
}

type StorageInspector interface {
	StorageItemsList(context.Context, *pb.StorageItemsListRequest, *pb.StorageItemsListResponse) error
	StorageItemsImport(context.Context, *pb.StorageItemsImportRequest, *pb.StorageItemsImportResponse) error
	StorageItemsDelete(context.Context, *pb.StorageItemsDeleteRequest) error
	StorageScheduledList(context.Context, *pb.StorageItemsListRequest, *pb.StorageItemsListResponse) error
}

type StorageBackendsAdmin interface {
	StorageBackendsUpdate(context.Context, *pb.StorageBackendsUpdateRequest) error
	StorageBackendsList(context.Context, *pb.StorageBackendsListRequest, *pb.StorageBackendsListResponse) error
}

type NamespaceAdmin interface {
	NamespacesCreate(context.Context, *pb.NamespaceInfo) error
	NamespacesUpdate(context.Context, *pb.NamespaceInfo) error
	NamespacesList(context.Context, *pb.NamespacesListRequest, *pb.NamespacesListResponse) error
	NamespacesDelete(context.Context, *pb.NamespacesDeleteRequest) error
}

type UsersAdmin interface {
	UsersCreate(context.Context, *pb.UserCreateRequest, *pb.UserCreateResponse) error
	UsersList(context.Context, *pb.UsersListRequest, *pb.UsersListResponse) error
	UsersDelete(context.Context, *pb.UsersDeleteRequest) error
}

type APIKeysAdmin interface {
	APIKeysCreate(context.Context, *pb.APIKeyCreateRequest, *pb.APIKeyCreateResponse) error
	APIKeysList(context.Context, *pb.APIKeysListRequest, *pb.APIKeysListResponse) error
	APIKeysDelete(context.Context, *pb.APIKeysDeleteRequest) error
}

type RolesAdmin interface {
	RolesCreate(context.Context, *pb.RoleCreateRequest, *pb.RoleCreateResponse) error
	RolesList(context.Context, *pb.RolesListRequest, *pb.RolesListResponse) error
	RolesUpdate(context.Context, *pb.RoleUpdateRequest) error
	RolesDelete(context.Context, *pb.RolesDeleteRequest) error
}

type RoleBindingsAdmin interface {
	RoleBindingsCreate(context.Context, *pb.RoleBindingCreateRequest, *pb.RoleBindingCreateResponse) error
	RoleBindingsList(context.Context, *pb.RoleBindingsListRequest, *pb.RoleBindingsListResponse) error
	RoleBindingsDelete(context.Context, *pb.RoleBindingDeleteRequest) error
}

type Service interface {
	QueueOps
	QueueAdmin
	StorageInspector
	StorageBackendsAdmin
	NamespaceAdmin
	UsersAdmin
	APIKeysAdmin
	RolesAdmin
	RoleBindingsAdmin
	Health(context.Context) (*HealthResponse, error)
}
