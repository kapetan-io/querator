package auth

import "context"

type AuthBackend interface {
	Authenticate(ctx context.Context, token string) (Principal, error)
	HasPermission(ctx context.Context, principal Principal, access Access) (bool, error)
	InvalidateUser(userID string)
	InvalidateKey(keyHash string)
	Close()
}

type Access struct {
	Namespace  string
	Permission string
}

type NoOpAuthBackend struct{}

var _ AuthBackend = &NoOpAuthBackend{}

func (n *NoOpAuthBackend) Authenticate(_ context.Context, _ string) (Principal, error) {
	return AnonymousPrincipal(), nil
}

func (n *NoOpAuthBackend) HasPermission(_ context.Context, _ Principal, _ Access) (bool, error) {
	return true, nil
}

func (n *NoOpAuthBackend) InvalidateUser(_ string) {}

func (n *NoOpAuthBackend) InvalidateKey(_ string) {}

func (n *NoOpAuthBackend) Close() {}
