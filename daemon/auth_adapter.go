package daemon

import (
	"context"

	"github.com/kapetan-io/querator/internal/auth"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport"
)

// AuthBackendAdapter wraps internal/auth.AuthBackend to implement transport.AuthBackend.
// It bridges the gap between the transport.Principal (used in HTTP layer) and
// types.Principal (used in internal auth layer) without creating import cycles.
type AuthBackendAdapter struct {
	backend auth.AuthBackend
}

// NewAuthBackendAdapter creates an adapter that wraps an internal auth backend
func NewAuthBackendAdapter(backend auth.AuthBackend) *AuthBackendAdapter {
	return &AuthBackendAdapter{backend: backend}
}

// Authenticate validates a token and returns the principal.
func (a *AuthBackendAdapter) Authenticate(ctx context.Context, token string) (transport.Principal, error) {
	principal, err := a.backend.Authenticate(ctx, token)
	if err != nil {
		return transport.Principal{}, err
	}

	return transport.Principal{
		NamespaceScope: principal.NamespaceScope,
		UserID:         principal.User.ID,
		Username:       principal.User.Username,
	}, nil
}

// HasPermission checks if a principal has a permission in a namespace.
// It reconstructs types.Principal from transport.Principal fields.
func (a *AuthBackendAdapter) HasPermission(ctx context.Context, principal transport.Principal, targetNS string, perm string) (bool, error) {
	internalPrincipal := types.Principal{
		NamespaceScope: principal.NamespaceScope,
		IsAnonymous:    principal.UserID == "" || principal.Username == "anonymous",
		User: types.User{
			Username: principal.Username,
			ID:       principal.UserID,
		},
	}
	return a.backend.HasPermission(ctx, internalPrincipal, targetNS, perm)
}

// Close releases resources
func (a *AuthBackendAdapter) Close() {
	a.backend.Close()
}
