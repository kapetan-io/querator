package auth

import "slices"

const (
	NamespaceCreate = "namespace.create"
	NamespaceDelete = "namespace.delete"
	NamespaceList   = "namespace.list"
	NamespaceUpdate = "namespace.update"

	QueueCreate   = "queue.create"
	QueueDelete   = "queue.delete"
	QueueUpdate   = "queue.update"
	QueueList     = "queue.list"
	QueueProduce  = "queue.produce"
	QueueLease    = "queue.lease"
	QueueComplete = "queue.complete"
	QueueRetry    = "queue.retry"
	QueueStats    = "queue.stats"
	QueueClear    = "queue.clear"

	UserCreate = "user.create"
	UserDelete = "user.delete"
	UserList   = "user.list"

	APIKeyCreate = "apikey.create"
	APIKeyDelete = "apikey.delete"
	APIKeyList   = "apikey.list"

	RoleCreate        = "role.create"
	RoleUpdate        = "role.update"
	RoleDelete        = "role.delete"
	RoleList          = "role.list"
	RoleBindingCreate = "rolebinding.create"
	RoleBindingDelete = "rolebinding.delete"
	RoleBindingList   = "rolebinding.list"

	BackendUpdate = "backend.update"
	BackendList   = "backend.list"

	SystemHealth  = "system.health"
	SystemMetrics = "system.metrics"
)

const SystemNamespace = "_system"

const (
	RoleAdmin          = "Admin"
	RoleNamespaceOwner = "NamespaceOwner"
	RolePublicViewer   = "PublicViewer"
)

var allPermissions = []string{
	NamespaceCreate,
	NamespaceDelete,
	NamespaceList,
	NamespaceUpdate,
	QueueCreate,
	QueueDelete,
	QueueUpdate,
	QueueList,
	QueueProduce,
	QueueLease,
	QueueComplete,
	QueueRetry,
	QueueStats,
	QueueClear,
	UserCreate,
	UserDelete,
	UserList,
	APIKeyCreate,
	APIKeyDelete,
	APIKeyList,
	RoleCreate,
	RoleUpdate,
	RoleDelete,
	RoleList,
	RoleBindingCreate,
	RoleBindingDelete,
	RoleBindingList,
	BackendUpdate,
	BackendList,
	SystemHealth,
	SystemMetrics,
}

var permissionSet map[string]struct{}

func init() {
	permissionSet = make(map[string]struct{}, len(allPermissions))
	for _, p := range allPermissions {
		permissionSet[p] = struct{}{}
	}
}

func AllPermissions() []string {
	return slices.Clone(allPermissions)
}

func AdminPermissions() []string { return AllPermissions() }

func NamespaceOwnerPermissions() []string {
	return []string{
		QueueCreate,
		QueueDelete,
		QueueUpdate,
		QueueList,
		QueueProduce,
		QueueLease,
		QueueComplete,
		QueueRetry,
		QueueStats,
		QueueClear,
		RoleCreate,
		RoleUpdate,
		RoleDelete,
		RoleList,
		RoleBindingCreate,
		RoleBindingDelete,
		RoleBindingList,
		SystemHealth,
		SystemMetrics,
	}
}

func PublicViewerPermissions() []string {
	return []string{
		SystemHealth,
		SystemMetrics,
	}
}

func IsValidPermission(perm string) bool {
	_, ok := permissionSet[perm]
	return ok
}

func IsStandardRole(name string) bool {
	return name == RoleAdmin || name == RoleNamespaceOwner || name == RolePublicViewer
}
