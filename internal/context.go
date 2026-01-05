package internal

import (
	"context"

	"github.com/kapetan-io/querator/internal/types"
)

type contextKey string

const principalKey contextKey = "principal"

// PrincipalFromContext extracts the Principal from the context.
// Returns AnonymousPrincipal if no principal is stored in the context.
func PrincipalFromContext(ctx context.Context) types.Principal {
	p, ok := ctx.Value(principalKey).(types.Principal)
	if !ok {
		return types.AnonymousPrincipal
	}
	return p
}

// ContextWithPrincipal returns a new context with the Principal stored in it.
func ContextWithPrincipal(ctx context.Context, p types.Principal) context.Context {
	return context.WithValue(ctx, principalKey, p)
}
