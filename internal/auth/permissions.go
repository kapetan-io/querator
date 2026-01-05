package auth

// Permission constants define all available permissions in the system
const (
	// Namespace permissions
	PermNamespaceCreate = "namespace.create"
	PermNamespaceDelete = "namespace.delete"
	PermNamespaceList   = "namespace.list"

	// Queue permissions
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

	// User permissions
	PermUserCreate = "user.create"
	PermUserDelete = "user.delete"
	PermUserList   = "user.list"

	// API Key permissions
	PermAPIKeyCreate = "apikey.create"
	PermAPIKeyDelete = "apikey.delete"
	PermAPIKeyList   = "apikey.list"

	// Role permissions
	PermRoleCreate        = "role.create"
	PermRoleUpdate        = "role.update"
	PermRoleDelete        = "role.delete"
	PermRoleList          = "role.list"
	PermRoleBindingCreate = "rolebinding.create"
	PermRoleBindingDelete = "rolebinding.delete"
	PermRoleBindingList   = "rolebinding.list"

	// System permissions
	PermSystemHealth  = "system.health"
	PermSystemMetrics = "system.metrics"
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
	PermNamespaceCreate,
	PermNamespaceDelete,
	PermNamespaceList,
	PermQueueCreate,
	PermQueueDelete,
	PermQueueUpdate,
	PermQueueList,
	PermQueueProduce,
	PermQueueLease,
	PermQueueComplete,
	PermQueueRetry,
	PermQueueStats,
	PermQueueClear,
	PermUserCreate,
	PermUserDelete,
	PermUserList,
	PermAPIKeyCreate,
	PermAPIKeyDelete,
	PermAPIKeyList,
	PermRoleCreate,
	PermRoleUpdate,
	PermRoleDelete,
	PermRoleList,
	PermRoleBindingCreate,
	PermRoleBindingDelete,
	PermRoleBindingList,
	PermSystemHealth,
	PermSystemMetrics,
}

// AdminPermissions are all permissions granted to the Admin role
var AdminPermissions = AllPermissions

// NamespaceOwnerPermissions are permissions for managing a namespace's resources
var NamespaceOwnerPermissions = []string{
	PermQueueCreate,
	PermQueueDelete,
	PermQueueUpdate,
	PermQueueList,
	PermQueueProduce,
	PermQueueLease,
	PermQueueComplete,
	PermQueueRetry,
	PermQueueStats,
	PermQueueClear,
	PermRoleCreate,
	PermRoleUpdate,
	PermRoleDelete,
	PermRoleList,
	PermRoleBindingCreate,
	PermRoleBindingDelete,
	PermRoleBindingList,
	PermSystemHealth,
	PermSystemMetrics,
}

// PublicViewerPermissions are minimal permissions for public access
var PublicViewerPermissions = []string{
	PermSystemHealth,
	PermSystemMetrics,
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
