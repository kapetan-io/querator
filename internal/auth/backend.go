package auth

import (
	"context"
	"log/slog"

	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	tauth "github.com/kapetan-io/querator/transport/auth"
	"github.com/kapetan-io/tackle/set"
)

// AuthBackend defines the interface for authentication and authorization
type AuthBackend interface {
	// Authenticate validates a token and returns the principal
	Authenticate(ctx context.Context, token string) (types.Principal, error)
	// HasPermission checks if a principal has a permission in a namespace
	HasPermission(ctx context.Context, principal types.Principal, targetNS string, perm string) (bool, error)
	// InvalidateUser removes all cached entries for a user
	InvalidateUser(userID string)
	// InvalidateKey removes a cached entry by key hash
	InvalidateKey(keyHash string)
	// Close releases any resources
	Close()
}

// DefaultAuthBackendConfig configures the DefaultAuthBackend
type DefaultAuthBackendConfig struct {
	RoleBindings store.RoleBindings
	APIKeys      store.APIKeys
	Users        store.Users
	Roles        store.Roles
	Log          *slog.Logger
}

// DefaultAuthBackend implements AuthBackend using storage backends
type DefaultAuthBackend struct {
	roleBindings store.RoleBindings
	apiKeys      store.APIKeys
	users        store.Users
	roles        store.Roles
	cache        *Cache
	log          *slog.Logger
}

var _ AuthBackend = &DefaultAuthBackend{}

// NewDefaultAuthBackend creates a new DefaultAuthBackend
func NewDefaultAuthBackend(conf DefaultAuthBackendConfig) *DefaultAuthBackend {
	set.Default(&conf.Log, slog.Default())

	return &DefaultAuthBackend{
		roleBindings: conf.RoleBindings,
		apiKeys:      conf.APIKeys,
		users:        conf.Users,
		roles:        conf.Roles,
		log:          conf.Log,
		cache: NewCache(CacheConfig{
			APIKeys: conf.APIKeys,
			Users:   conf.Users,
		}),
	}
}

// Authenticate validates a token and returns the principal
func (a *DefaultAuthBackend) Authenticate(ctx context.Context, token string) (types.Principal, error) {
	if token == "" {
		return types.AnonymousPrincipal, nil
	}
	return a.cache.Authenticate(ctx, token)
}

// HasPermission implements the cascading permission check:
// 1. Check API key scope: If scoped and scope != target namespace, return FALSE immediately
// 2. Check target namespace for permission (User has role in target NS)
// 3. If not found and target != _system, check _system namespace (User has role in _system)
func (a *DefaultAuthBackend) HasPermission(ctx context.Context, principal types.Principal, targetNS string, perm string) (bool, error) {
	// Step 1: Check API key scope
	// If the principal's API key is scoped to a specific namespace and it doesn't match the target,
	// deny access immediately without checking roles
	if principal.NamespaceScope != nil && *principal.NamespaceScope != targetNS {
		return false, nil
	}

	// Step 2: Check permission in target namespace
	hasPermission, err := a.checkPermissionInNamespace(ctx, principal.User.ID, targetNS, perm)
	if err != nil {
		return false, err
	}
	if hasPermission {
		return true, nil
	}

	// Step 3: If target is not _system, check permission in _system namespace (admin fallback)
	if targetNS != tauth.SystemNamespace {
		hasPermission, err = a.checkPermissionInNamespace(ctx, principal.User.ID, tauth.SystemNamespace, perm)
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
func (a *DefaultAuthBackend) checkPermissionInNamespace(ctx context.Context, userID, namespace, perm string) (bool, error) {
	// Get all role bindings for the user
	var bindings []types.RoleBinding
	if err := a.roleBindings.ListByUser(ctx, userID, &bindings); err != nil {
		return false, err
	}

	// Check each binding in the target namespace
	for _, binding := range bindings {
		if binding.Namespace != namespace {
			continue
		}

		// Get the role for this binding
		var role types.Role
		if err := a.roles.GetByID(ctx, binding.RoleID, &role); err != nil {
			a.log.Warn("role binding references non-existent role",
				"binding_id", binding.ID,
				"role_id", binding.RoleID)
			continue
		}

		// Check if the role has the required permission
		for _, p := range role.Permissions {
			if p == perm {
				return true, nil
			}
		}
	}

	return false, nil
}

// InvalidateUser removes all cached entries for a user
func (a *DefaultAuthBackend) InvalidateUser(userID string) {
	a.cache.InvalidateUser(userID)
}

// InvalidateKey removes a cached entry by key hash
func (a *DefaultAuthBackend) InvalidateKey(keyHash string) {
	a.cache.Invalidate(keyHash)
}

// Close releases resources
func (a *DefaultAuthBackend) Close() {
	if a.cache != nil {
		a.cache.Close()
	}
}

// NoOpAuthBackend is an AuthBackend that always allows access (for testing)
type NoOpAuthBackend struct{}

var _ AuthBackend = &NoOpAuthBackend{}

// Authenticate returns the anonymous principal
func (n *NoOpAuthBackend) Authenticate(_ context.Context, _ string) (types.Principal, error) {
	return types.AnonymousPrincipal, nil
}

// HasPermission always returns true
func (n *NoOpAuthBackend) HasPermission(_ context.Context, _ types.Principal, _ string, _ string) (bool, error) {
	return true, nil
}

// InvalidateUser does nothing
func (n *NoOpAuthBackend) InvalidateUser(_ string) {}

// InvalidateKey does nothing
func (n *NoOpAuthBackend) InvalidateKey(_ string) {}

// Close does nothing
func (n *NoOpAuthBackend) Close() {}
