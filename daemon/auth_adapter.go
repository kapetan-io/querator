package daemon

import (
	"context"
	"sync"

	"github.com/kapetan-io/querator/internal/auth"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport"
)

// AuthBackendAdapter wraps internal/auth.AuthBackend to implement transport.AuthBackend.
// It bridges the gap between the transport.Principal (used in HTTP layer) and
// types.Principal (used in internal auth layer) without creating import cycles.
type AuthBackendAdapter struct {
	principals sync.Map // map[string]types.Principal - keyed by UserID
	backend    auth.AuthBackend
}

// NewAuthBackendAdapter creates an adapter that wraps an internal auth backend
func NewAuthBackendAdapter(backend auth.AuthBackend) *AuthBackendAdapter {
	return &AuthBackendAdapter{backend: backend}
}

// Authenticate validates a token and returns the principal.
// It stores the internal principal keyed by UserID for later use in HasPermission.
func (a *AuthBackendAdapter) Authenticate(ctx context.Context, token string) (transport.Principal, error) {
	principal, err := a.backend.Authenticate(ctx, token)
	if err != nil {
		return transport.Principal{}, err
	}

	// Store the internal principal for later use in HasPermission
	a.principals.Store(principal.User.ID, principal)

	return transport.Principal{
		NamespaceScope: principal.NamespaceScope,
		UserID:         principal.User.ID,
		Username:       principal.User.Username,
	}, nil
}

// HasPermission checks if a principal has a permission in a namespace.
// It retrieves the stored internal principal and delegates to the backend.
func (a *AuthBackendAdapter) HasPermission(ctx context.Context, principal transport.Principal, targetNS string, perm string) (bool, error) {
	// Try to get the cached internal principal
	if cached, ok := a.principals.Load(principal.UserID); ok {
		return a.backend.HasPermission(ctx, cached.(types.Principal), targetNS, perm)
	}

	// If not found (e.g., anonymous), reconstruct a minimal principal
	internalPrincipal := types.Principal{
		User: types.User{
			Username: principal.Username,
			ID:       principal.UserID,
		},
		NamespaceScope: principal.NamespaceScope,
		IsAnonymous:    principal.UserID == "" || principal.Username == "anonymous",
	}
	return a.backend.HasPermission(ctx, internalPrincipal, targetNS, perm)
}

// Close releases resources
func (a *AuthBackendAdapter) Close() {
	a.backend.Close()
}
