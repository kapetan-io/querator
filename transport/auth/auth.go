package auth

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

// AllPermissions is the complete list of all available permissions
var AllPermissions = []string{
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

// AdminPermissions are all permissions granted to the Admin role
var AdminPermissions = append([]string(nil), AllPermissions...)

// NamespaceOwnerPermissions are permissions for managing a namespace's resources
var NamespaceOwnerPermissions = []string{
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

// PublicViewerPermissions are minimal permissions for public access
var PublicViewerPermissions = []string{
	SystemHealth,
	SystemMetrics,
}

// IsValidPermission checks if a permission string is valid
func IsValidPermission(perm string) bool {
	for _, p := range AllPermissions {
		if p == perm {
			return true
		}
	}
	return false
}

// IsStandardRole checks if a role name is a standard immutable role
func IsStandardRole(name string) bool {
	return name == RoleAdmin || name == RoleNamespaceOwner || name == RolePublicViewer
}
