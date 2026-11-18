package auth

import "context"

type contextKey string

const principalKey contextKey = "principal"

func PrincipalFromContext(ctx context.Context) Principal {
	p, ok := ctx.Value(principalKey).(Principal)
	if !ok {
		return AnonymousPrincipal()
	}
	return p
}

func ContextWithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey, p)
}
