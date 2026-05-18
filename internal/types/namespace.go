package types

import (
	"strings"

	pb "github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/tackle/clock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Namespace represents an isolation boundary for queues
type Namespace struct {
	Description string
	CreatedAt   clock.Time
	APIKeyTag   string
	Name        string
}

// IsReserved returns true if the namespace name starts with '_' prefix
func (n *Namespace) IsReserved() bool {
	return strings.HasPrefix(n.Name, "_")
}

// ToProto converts the Namespace to its proto representation
func (n *Namespace) ToProto(in *pb.NamespaceInfo) *pb.NamespaceInfo {
	in.Description = n.Description
	in.CreatedAt = timestamppb.New(n.CreatedAt)
	in.ApiKeyTag = n.APIKeyTag
	in.Name = n.Name
	return in
}
