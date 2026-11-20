package internal

import (
	"context"
	"errors"
	"log/slog"

	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport/auth"
	"github.com/kapetan-io/querator/transport/reply"
	"github.com/kapetan-io/tackle/set"
)

type AuthBackendConfig struct {
	RoleBindings store.RoleBindings
	APIKeys      store.APIKeys
	Users        store.Users
	Roles        store.Roles
	Log          *slog.Logger
}

type AuthBackend struct {
	roleBindings store.RoleBindings
	roles        store.Roles
	cache        *AuthCache
	log          *slog.Logger
}

var _ auth.AuthBackend = &AuthBackend{}

func NewAuthBackend(conf AuthBackendConfig) *AuthBackend {
	set.Default(&conf.Log, slog.Default())

	return &AuthBackend{
		roleBindings: conf.RoleBindings,
		roles:        conf.Roles,
		log:          conf.Log,
		cache: NewAuthCache(AuthCacheConfig{
			APIKeys: conf.APIKeys,
			Users:   conf.Users,
			Log:     conf.Log,
		}),
	}
}

func (a *AuthBackend) Authenticate(ctx context.Context, token string) (auth.Principal, error) {
	if token == "" {
		return auth.AnonymousPrincipal(), nil
	}
	return a.cache.Authenticate(ctx, token)
}

func (a *AuthBackend) HasPermission(ctx context.Context, principal auth.Principal, access auth.Access) (bool, error) {
	targetNS, perm := access.Namespace, access.Permission
	if principal.NamespaceScope != nil && *principal.NamespaceScope != targetNS {
		if targetNS != auth.SystemNamespace {
			return false, nil
		}
		return a.checkPermissionInNamespace(ctx, permissionCheckArgs{UserID: principal.UserID, Namespace: auth.SystemNamespace, Permission: perm})
	}

	hasPermission, err := a.checkPermissionInNamespace(ctx, permissionCheckArgs{UserID: principal.UserID, Namespace: targetNS, Permission: perm})
	if err != nil {
		return false, err
	}
	if hasPermission {
		return true, nil
	}

	if targetNS != auth.SystemNamespace {
		hasPermission, err = a.checkPermissionInNamespace(ctx, permissionCheckArgs{UserID: principal.UserID, Namespace: auth.SystemNamespace, Permission: perm})
		if err != nil {
			return false, err
		}
		if hasPermission {
			return true, nil
		}
	}

	return false, nil
}

type permissionCheckArgs struct {
	UserID     string
	Namespace  string
	Permission string
}

func (a *AuthBackend) checkPermissionInNamespace(ctx context.Context, check permissionCheckArgs) (bool, error) {
	var bindings []types.RoleBinding
	if err := a.roleBindings.ListByUser(ctx, store.UserInNamespace{Namespace: check.Namespace, UserID: check.UserID}, &bindings, types.MaxRoleBindingsPerUserInNamespace); err != nil {
		return false, err
	}

	for _, binding := range bindings {

		var role types.Role
		if err := a.roles.GetByID(ctx, binding.RoleID, &role); err != nil {
			var notFound *reply.ErrRequestFailed
			if !errors.As(err, &notFound) {
				return false, err
			}
			a.log.Warn("role binding references non-existent role",
				"binding_id", binding.ID,
				"role_id", binding.RoleID)
			continue
		}

		for _, p := range role.Permissions {
			if p == check.Permission {
				return true, nil
			}
		}
	}

	return false, nil
}

func (a *AuthBackend) InvalidateUser(userID string) {
	a.cache.InvalidateUser(userID)
}

func (a *AuthBackend) InvalidateKey(keyHash string) {
	a.cache.Invalidate(keyHash)
}

func (a *AuthBackend) Close() {
	a.cache.Close()
}
