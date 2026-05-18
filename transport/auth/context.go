package auth

import "context"

type contextKey string

const principalKey contextKey = "principal"

// PrincipalFromContext extracts the Principal from the context.
// Returns AnonymousPrincipal if no principal is stored in the context.
func PrincipalFromContext(ctx context.Context) Principal {
	p, ok := ctx.Value(principalKey).(Principal)
	if !ok {
		return AnonymousPrincipal()
	}
	return p
}

// ContextWithPrincipal returns a new context with the Principal stored in it
func ContextWithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey, p)
}
