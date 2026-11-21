package store

import (
	"context"

	"github.com/kapetan-io/querator/internal/types"
)

type Users interface {
	Get(ctx context.Context, id string, user *types.User) error

	GetByUsername(ctx context.Context, username string, user *types.User) error

	Add(ctx context.Context, user types.User) error

	List(ctx context.Context, users *[]types.User, opts types.ListOptions) error

	Delete(ctx context.Context, id string) error

	Close(ctx context.Context) error
}
