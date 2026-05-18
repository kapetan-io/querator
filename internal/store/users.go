package store

import (
	"context"

	"github.com/kapetan-io/querator/internal/types"
)

// Users is storage for listing and storing information about users.
type Users interface {
	// Get fetches User from storage by ID. Returns ErrUserNotExist if the user does not exist.
	Get(ctx context.Context, id string, user *types.User) error

	// GetByUsername fetches User from storage by username. Returns ErrUserNotExist if not found.
	GetByUsername(ctx context.Context, username string, user *types.User) error

	// Add a User to the store. Returns ErrUserAlreadyExists if ID exists,
	// or ErrUsernameAlreadyTaken if username is taken.
	Add(ctx context.Context, user types.User) error

	// List returns a list of users with pagination support.
	List(ctx context.Context, users *[]types.User, opts types.ListOptions) error

	// Delete removes a user by ID. Returns ErrUserNotExist if not found.
	Delete(ctx context.Context, id string) error

	// Close closes all open database connections or files.
	Close(ctx context.Context) error
}
