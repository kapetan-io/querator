package store

import (
	"context"

	"github.com/kapetan-io/querator/internal/types"
)

// Roles is storage for listing and storing information about roles.
// Implementations should employ lazy storage initialization such that it makes contact
// or creates underlying tables only upon first invocation.
type Roles interface {
	// Get fetches a Role from storage by namespace and name. Returns ErrRoleNotExist if the
	// role requested does not exist.
	Get(ctx context.Context, namespace, name string, role *types.Role) error

	// GetByID fetches a Role from storage by ID. Returns ErrRoleNotExist if the
	// role requested does not exist.
	GetByID(ctx context.Context, id string, role *types.Role) error

	// Add a Role to the store. If the role already exists returns ErrRoleAlreadyExists.
	Add(ctx context.Context, role types.Role) error

	// Update a role in the store. Returns ErrRoleNotExist if the role does not exist.
	Update(ctx context.Context, role types.Role) error

	// List returns a list of roles for a namespace with pagination support.
	List(ctx context.Context, namespace string, roles *[]types.Role, opts types.ListOptions) error

	// Delete deletes a role by ID. Returns ErrRoleNotExist if the role does not exist.
	Delete(ctx context.Context, id string) error

	// Close the all open database connections or files.
	Close(ctx context.Context) error
}

// RoleBindings is storage for listing and storing role bindings.
// Implementations should employ lazy storage initialization such that it makes contact
// or creates underlying tables only upon first invocation.
type RoleBindings interface {
	// Get fetches a RoleBinding from storage by ID. Returns ErrRoleBindingNotExist if the
	// binding requested does not exist.
	Get(ctx context.Context, id string, binding *types.RoleBinding) error

	// Add a RoleBinding to the store. If the binding already exists returns ErrRoleBindingAlreadyExists.
	Add(ctx context.Context, binding types.RoleBinding) error

	// List returns a list of role bindings for a namespace with pagination support.
	List(ctx context.Context, namespace string, bindings *[]types.RoleBinding, opts types.ListOptions) error

	// ListByUser returns all role bindings for a user in a specific namespace.
	ListByUser(ctx context.Context, userID, namespace string, bindings *[]types.RoleBinding) error

	// ListByRole returns all role bindings for a specific role.
	ListByRole(ctx context.Context, roleID string, bindings *[]types.RoleBinding) error

	// DeleteByUserAndRole deletes the role binding identified by (namespace, userID, roleID).
	// Returns ErrRoleBindingNotExist if no matching binding exists.
	DeleteByUserAndRole(ctx context.Context, namespace, userID, roleID string) error

	// Delete deletes a role binding by ID. Returns ErrRoleBindingNotExist if the binding does not exist.
	Delete(ctx context.Context, id string) error

	// DeleteByUser deletes all role bindings for a user (used in CASCADE delete).
	DeleteByUser(ctx context.Context, userID string) error

	// Close the all open database connections or files.
	Close(ctx context.Context) error
}
