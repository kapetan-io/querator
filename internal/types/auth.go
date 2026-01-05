package types

import "github.com/kapetan-io/tackle/clock"

// Principal represents the authenticated entity making a request
type Principal struct {
	User           User
	NamespaceScope *string
	IsAnonymous    bool
}

// User represents a user in the system
type User struct {
	ExternalID string
	Username   string
	Email      string
	CreatedAt  clock.Time
	UpdatedAt  clock.Time
	ID         string
}

// AnonymousUser is the default user for unauthenticated requests
var AnonymousUser = User{
	Username: "anonymous",
	ID:       "anonymous",
}

// AnonymousPrincipal is the default principal for unauthenticated requests
var AnonymousPrincipal = Principal{
	User:        AnonymousUser,
	IsAnonymous: true,
}
