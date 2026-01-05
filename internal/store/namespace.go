package store

import (
	"context"

	"github.com/kapetan-io/querator/internal/types"
)

// Namespaces is storage for listing and storing information about namespaces.
// Implementations should employ lazy storage initialization such that it makes contact
// or creates underlying tables only upon first invocation.
type Namespaces interface {
	// Get fetches Namespace from storage. Returns ErrNamespaceNotExist if the
	// namespace requested does not exist.
	Get(ctx context.Context, name string, ns *types.Namespace) error

	// Add a Namespace to the store. If the namespace already exists returns an error.
	Add(ctx context.Context, ns types.Namespace) error

	// List returns a list of namespaces with pagination support.
	List(ctx context.Context, namespaces *[]types.Namespace, opts types.ListOptions) error

	// Delete deletes a namespace. Returns ErrNamespaceNotExist if the namespace does not exist.
	Delete(ctx context.Context, name string) error

	// Close the all open database connections or files.
	Close(ctx context.Context) error
}
