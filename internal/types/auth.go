package types

import (
	"github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/tackle/clock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type User struct {
	ExternalID string
	Username   string
	Email      string
	CreatedAt  clock.Time
	UpdatedAt  clock.Time
	ID         string
}

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

type Role struct {
	Permissions []string
	CreatedAt   clock.Time
	Namespace   string
	Name        string
	ID          string
}

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

const MaxRoleBindingsPerUserInNamespace = 100

type RoleBinding struct {
	CreatedAt clock.Time
	Namespace string
	UserID    string
	RoleID    string
	ID        string
}

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
