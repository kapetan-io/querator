package types

import (
	"strings"

	pb "github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/tackle/clock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Namespace struct {
	Description string
	CreatedAt   clock.Time
	APIKeyTag   string
	Name        string
}

func (n *Namespace) IsReserved() bool {
	return strings.HasPrefix(n.Name, "_")
}

func (n *Namespace) ToProto(in *pb.NamespaceInfo) *pb.NamespaceInfo {
	in.Description = n.Description
	in.CreatedAt = timestamppb.New(n.CreatedAt)
	in.ApiKeyTag = n.APIKeyTag
	in.Name = n.Name
	return in
}
