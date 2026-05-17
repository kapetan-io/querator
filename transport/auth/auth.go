package auth

import "slices"

// Permission constants define all available permissions in the system
const (
	// Namespace permissions
	NamespaceCreate = "namespace.create"
	NamespaceDelete = "namespace.delete"
	NamespaceList   = "namespace.list"
	NamespaceUpdate = "namespace.update"

	// Queue permissions
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

	// User permissions
	UserCreate = "user.create"
	UserDelete = "user.delete"
	UserList   = "user.list"

	// API Key permissions
	APIKeyCreate = "apikey.create"
	APIKeyDelete = "apikey.delete"
	APIKeyList   = "apikey.list"

	// Role permissions
	RoleCreate        = "role.create"
	RoleUpdate        = "role.update"
	RoleDelete        = "role.delete"
	RoleList          = "role.list"
	RoleBindingCreate = "rolebinding.create"
	RoleBindingDelete = "rolebinding.delete"
	RoleBindingList   = "rolebinding.list"

	// System permissions
	SystemHealth  = "system.health"
	SystemMetrics = "system.metrics"
)

// SystemNamespace is the reserved namespace for system-level resources
const SystemNamespace = "_system"

// Standard role names that cannot be modified or deleted
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

// AllPermissions returns the complete list of all available permissions.
// A new slice is returned on each call to prevent callers from mutating the canonical list.
func AllPermissions() []string {
	return slices.Clone(allPermissions)
}

// AdminPermissions returns all permissions granted to the Admin role.
// A new slice is returned on each call to prevent callers from mutating the canonical list.
func AdminPermissions() []string { return AllPermissions() }

// NamespaceOwnerPermissions returns permissions for managing a namespace's resources.
// A new slice is returned on each call to prevent callers from mutating the canonical list.
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

// PublicViewerPermissions returns minimal permissions for public access.
// A new slice is returned on each call to prevent callers from mutating the canonical list.
func PublicViewerPermissions() []string {
	return []string{
		SystemHealth,
		SystemMetrics,
	}
}

// IsValidPermission checks if a permission string is valid
func IsValidPermission(perm string) bool {
	_, ok := permissionSet[perm]
	return ok
}

// IsStandardRole checks if a role name is a standard immutable role
func IsStandardRole(name string) bool {
	return name == RoleAdmin || name == RoleNamespaceOwner || name == RolePublicViewer
}
