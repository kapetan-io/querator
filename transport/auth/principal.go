package auth

type Principal struct {
	NamespaceScope *string
	UserID         string
	Username       string
}

const (
	AnonymousUserID   = "anonymous"
	AnonymousUsername = "anonymous"
)

func AnonymousPrincipal() Principal {
	return Principal{
		UserID:   AnonymousUserID,
		Username: AnonymousUsername,
	}
}
