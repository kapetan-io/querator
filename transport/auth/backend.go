package auth

import "context"

// AuthBackend defines the interface for authentication and authorization
type AuthBackend interface {
	// Authenticate validates a token and returns the principal
	Authenticate(ctx context.Context, token string) (Principal, error)
	// HasPermission checks if a principal has a permission in a namespace
	HasPermission(ctx context.Context, principal Principal, targetNS string, perm string) (bool, error)
	// InvalidateUser removes all cached entries for a user
	InvalidateUser(userID string)
	// InvalidateKey removes a cached entry by key hash
	InvalidateKey(keyHash string)
	// Close releases any resources
	Close()
}

// NoOpAuthBackend is an AuthBackend that always allows access (default open)
type NoOpAuthBackend struct{}

var _ AuthBackend = &NoOpAuthBackend{}

// Authenticate returns the anonymous principal
func (n *NoOpAuthBackend) Authenticate(_ context.Context, _ string) (Principal, error) {
	return AnonymousPrincipal, nil
}

// HasPermission always returns true
func (n *NoOpAuthBackend) HasPermission(_ context.Context, _ Principal, _ string, _ string) (bool, error) {
	return true, nil
}

// InvalidateUser does nothing
func (n *NoOpAuthBackend) InvalidateUser(_ string) {}

// InvalidateKey does nothing
func (n *NoOpAuthBackend) InvalidateKey(_ string) {}

// Close does nothing
func (n *NoOpAuthBackend) Close() {}
