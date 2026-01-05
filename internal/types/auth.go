package types

import (
	"github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/tackle/clock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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

// ToProto converts a User to its proto representation
func (u *User) ToProto(p *proto.User) *proto.User {
	p.ExternalId = u.ExternalID
	p.Username = u.Username
	p.Email = u.Email
	p.Id = u.ID
	if !u.CreatedAt.IsZero() {
		p.CreatedAt = timestamppb.New(u.CreatedAt)
	}
	if !u.UpdatedAt.IsZero() {
		p.UpdatedAt = timestamppb.New(u.UpdatedAt)
	}
	return p
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

// APIKey represents an API key for authentication
type APIKey struct {
	NamespaceScope *string
	ExpiresAt      *clock.Time
	LastUsedAt     *clock.Time
	CreatedAt      clock.Time
	KeyHash        string
	KeyPrefix      string
	UserID         string
	Name           string
	ID             string
}

// ToProto converts an APIKey to its proto representation (excludes KeyHash)
func (k *APIKey) ToProto(p *proto.APIKeyMetadata) *proto.APIKeyMetadata {
	p.UserId = k.UserID
	p.Prefix = k.KeyPrefix
	p.Name = k.Name
	p.Id = k.ID
	if k.NamespaceScope != nil {
		p.NamespaceScope = *k.NamespaceScope
	}
	if k.ExpiresAt != nil {
		p.ExpiresAt = timestamppb.New(*k.ExpiresAt)
	}
	if k.LastUsedAt != nil {
		p.LastUsedAt = timestamppb.New(*k.LastUsedAt)
	}
	if !k.CreatedAt.IsZero() {
		p.CreatedAt = timestamppb.New(k.CreatedAt)
	}
	return p
}
