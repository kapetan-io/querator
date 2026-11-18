package store

import (
	"context"

	"github.com/kapetan-io/querator/internal/types"
)

type Namespaces interface {
	Get(ctx context.Context, name string, ns *types.Namespace) error

	Add(ctx context.Context, ns types.Namespace) error

	List(ctx context.Context, namespaces *[]types.Namespace, opts types.ListOptions) error

	Update(ctx context.Context, ns types.Namespace) error

	Delete(ctx context.Context, name string) error

	Close(ctx context.Context) error
}
