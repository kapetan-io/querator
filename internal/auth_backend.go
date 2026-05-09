package internal

import (
	"context"
	"log/slog"

	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport/auth"
	"github.com/kapetan-io/tackle/set"
)

// AuthBackendConfig configures the AuthBackend
type AuthBackendConfig struct {
	RoleBindings store.RoleBindings
	APIKeys      store.APIKeys
	Users        store.Users
	Roles        store.Roles
	Log          *slog.Logger
}

// AuthBackend implements auth.AuthBackend using storage backends
type AuthBackend struct {
	roleBindings store.RoleBindings
	roles        store.Roles
	cache        *AuthCache
	log          *slog.Logger
}

var _ auth.AuthBackend = &AuthBackend{}

// NewAuthBackend creates a new AuthBackend
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

// Authenticate validates a token and returns the principal
func (a *AuthBackend) Authenticate(ctx context.Context, token string) (auth.Principal, error) {
	if token == "" {
		return auth.AnonymousPrincipal(), nil
	}
	return a.cache.Authenticate(ctx, token)
}

// HasPermission implements the cascading permission check:
// 1. Check API key scope: If scoped and scope != target namespace, return FALSE immediately
// 2. Check target namespace for permission (User has role in target NS)
// 3. If not found and target != _system, check _system namespace (User has role in _system)
func (a *AuthBackend) HasPermission(ctx context.Context, principal auth.Principal, targetNS string, perm string) (bool, error) {
	if principal.NamespaceScope != nil && *principal.NamespaceScope != targetNS {
		return false, nil
	}

	hasPermission, err := a.checkPermissionInNamespace(ctx, principal.UserID, targetNS, perm)
	if err != nil {
		return false, err
	}
	if hasPermission {
		return true, nil
	}

	if targetNS != auth.SystemNamespace {
		hasPermission, err = a.checkPermissionInNamespace(ctx, principal.UserID, auth.SystemNamespace, perm)
		if err != nil {
			return false, err
		}
		if hasPermission {
			return true, nil
		}
	}

	return false, nil
}

// checkPermissionInNamespace checks if a user has a specific permission in a namespace
func (a *AuthBackend) checkPermissionInNamespace(ctx context.Context, userID, namespace, perm string) (bool, error) {
	var bindings []types.RoleBinding
	if err := a.roleBindings.ListByUser(ctx, userID, &bindings); err != nil {
		return false, err
	}

	for _, binding := range bindings {
		if binding.Namespace != namespace {
			continue
		}

		var role types.Role
		if err := a.roles.GetByID(ctx, binding.RoleID, &role); err != nil {
			a.log.Warn("role binding references non-existent role",
				"binding_id", binding.ID,
				"role_id", binding.RoleID)
			continue
		}

		for _, p := range role.Permissions {
			if p == perm {
				return true, nil
			}
		}
	}

	return false, nil
}

// InvalidateUser removes all cached entries for a user
func (a *AuthBackend) InvalidateUser(userID string) {
	a.cache.InvalidateUser(userID)
}

// InvalidateKey removes a cached entry by key hash
func (a *AuthBackend) InvalidateKey(keyHash string) {
	a.cache.Invalidate(keyHash)
}

// Close releases resources
func (a *AuthBackend) Close() {
	a.cache.Close()
}
