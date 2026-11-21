package store

import (
	"context"

	"github.com/kapetan-io/querator/internal/types"
)

type RoleName struct {
	Namespace string
	Name      string
}

type UserInNamespace struct {
	Namespace string
	UserID    string
}

type RoleForUser struct {
	Namespace string
	UserID    string
	RoleID    string
}

type Roles interface {
	Get(ctx context.Context, roleName RoleName, role *types.Role) error

	GetByID(ctx context.Context, id string, role *types.Role) error

	Add(ctx context.Context, role types.Role) error

	Update(ctx context.Context, role types.Role) error

	List(ctx context.Context, namespace string, roles *[]types.Role, opts types.ListOptions) error

	Delete(ctx context.Context, id string) error

	Close(ctx context.Context) error
}

type RoleBindings interface {
	Get(ctx context.Context, id string, binding *types.RoleBinding) error

	Add(ctx context.Context, binding types.RoleBinding) error

	List(ctx context.Context, namespace string, bindings *[]types.RoleBinding, opts types.ListOptions) error

	ListByUser(ctx context.Context, user UserInNamespace, bindings *[]types.RoleBinding, limit int) error

	ListByRole(ctx context.Context, roleID string, bindings *[]types.RoleBinding, limit int) error

	DeleteByUserAndRole(ctx context.Context, binding RoleForUser) error

	Delete(ctx context.Context, id string) error

	DeleteByUser(ctx context.Context, userID string) error

	Close(ctx context.Context) error
}
