package store

import (
	"context"

	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/tackle/clock"
)

// APIKeys is storage for listing and storing API keys.
type APIKeys interface {
	// Get fetches an APIKey from storage by ID. Returns ErrAPIKeyNotExist if not found.
	Get(ctx context.Context, id string, key *types.APIKey) error

	// GetByHash fetches an APIKey from storage by key hash (for authentication).
	// Returns ErrAPIKeyNotExist if not found.
	GetByHash(ctx context.Context, hash string, key *types.APIKey) error

	// Add an APIKey to the store.
	Add(ctx context.Context, key types.APIKey) error

	// List returns all API keys with pagination support.
	List(ctx context.Context, keys *[]types.APIKey, opts types.ListOptions) error

	// ListByUser returns API keys for a specific user with pagination.
	ListByUser(ctx context.Context, userID string, keys *[]types.APIKey, opts types.ListOptions) error

	// Delete removes an API key by ID.
	Delete(ctx context.Context, id string) error

	// DeleteByUser removes all API keys belonging to a user. Used for CASCADE delete.
	DeleteByUser(ctx context.Context, userID string) error

	// UpdateLastUsed updates the last used timestamp for an API key.
	UpdateLastUsed(ctx context.Context, id string, lastUsed clock.Time) error

	// Close closes all open database connections or files.
	Close(ctx context.Context) error
}
