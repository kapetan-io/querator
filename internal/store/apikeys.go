package store

import (
	"context"

	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/tackle/clock"
)

type APIKeys interface {
	Get(ctx context.Context, id string, key *types.APIKey) error

	GetByHash(ctx context.Context, hash string, key *types.APIKey) error

	Add(ctx context.Context, key types.APIKey) error

	List(ctx context.Context, keys *[]types.APIKey, opts types.ListOptions) error

	ListByUser(ctx context.Context, userID string, keys *[]types.APIKey, opts types.ListOptions) error

	Delete(ctx context.Context, id string) error

	DeleteByUser(ctx context.Context, userID string) error

	UpdateLastUsed(ctx context.Context, id string, lastUsed clock.Time) error

	Close(ctx context.Context) error
}
