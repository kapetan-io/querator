package auth

// Principal represents an authenticated entity making a request
type Principal struct {
	NamespaceScope *string
	UserID         string
	Username       string
}

const (
	AnonymousUserID   = "anonymous"
	AnonymousUsername = "anonymous"
)

// AnonymousPrincipal represents an unauthenticated user.
// UserID must be "anonymous" so DefaultAuthBackend permission lookups work correctly.
var AnonymousPrincipal = Principal{
	UserID:   AnonymousUserID,
	Username: AnonymousUsername,
}
