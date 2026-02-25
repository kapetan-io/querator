package types

import (
	"github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/tackle/clock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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

// Role represents a role with a set of permissions within a namespace
type Role struct {
	Permissions []string
	CreatedAt   clock.Time
	Namespace   string
	Name        string
	ID          string
}

// ToProto converts a Role to its proto representation
func (r *Role) ToProto(p *proto.Role) *proto.Role {
	p.Permissions = r.Permissions
	p.Namespace = r.Namespace
	p.Name = r.Name
	p.Id = r.ID
	if !r.CreatedAt.IsZero() {
		p.CreatedAt = timestamppb.New(r.CreatedAt)
	}
	return p
}

// RoleBinding represents the binding of a user to a role within a namespace
type RoleBinding struct {
	CreatedAt clock.Time
	Namespace string
	UserID    string
	RoleID    string
	ID        string
}

// ToProto converts a RoleBinding to its proto representation
func (b *RoleBinding) ToProto(p *proto.RoleBinding) *proto.RoleBinding {
	p.Namespace = b.Namespace
	p.UserId = b.UserID
	p.RoleId = b.RoleID
	p.Id = b.ID
	if !b.CreatedAt.IsZero() {
		p.CreatedAt = timestamppb.New(b.CreatedAt)
	}
	return p
}
