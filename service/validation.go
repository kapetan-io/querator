package service

import (
	"strings"
	"unicode"

	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/querator/transport/auth"
	"github.com/kapetan-io/querator/transport/reply"
	"github.com/kapetan-io/tackle/clock"
)

const (
	maxTimeoutLength   = 15
	maxQueueNameLength = 512
	defaultAllocation  = 512  // 2<<8
	maxAllocation      = 2048 // 2<<10
	maxTagLength       = 16
)

func validateTag(field, value string) error {
	if len(value) > maxTagLength {
		return reply.NewInvalidOption("%s is invalid; cannot be greater than %d characters", field, maxTagLength)
	}
	for _, c := range value {
		if (c < '0' || c > '9') && (c < 'a' || c > 'z') {
			return reply.NewInvalidOption("%s is invalid; must be lowercase alphanumeric only", field)
		}
	}
	return nil
}

// validateNamespaceName validates the namespace name format.
// This validation happens at the service layer to protect downstream systems.
func validateNamespaceName(name string) error {
	const maxNamespaceNameLength = 256

	if len(name) > maxNamespaceNameLength {
		return reply.NewInvalidOption("namespace name is invalid; cannot be greater than '%d' characters", maxNamespaceNameLength)
	}

	if strings.TrimSpace(name) == "" {
		return reply.NewInvalidOption("namespace name is invalid; cannot be empty")
	}

	if strings.ContainsFunc(name, unicode.IsSpace) {
		return reply.NewInvalidOption("namespace name is invalid; '%s' cannot contain whitespace", name)
	}

	if strings.Contains(name, "~") {
		return reply.NewInvalidOption("namespace name is invalid; '%s' cannot contain '~' character", name)
	}

	if strings.Contains(name, ":") {
		return reply.NewInvalidOption("namespace name is invalid; '%s' cannot contain ':' character", name)
	}

	return nil
}

// validateQueueName validates the queue name format.
// This validation happens at the service layer to protect downstream systems.
func validateQueueName(name string) error {
	if len(name) > maxQueueNameLength {
		return reply.NewInvalidOption("queue name is invalid; cannot be greater than '%d' characters", maxQueueNameLength)
	}

	if strings.TrimSpace(name) == "" {
		return reply.NewInvalidOption("queue name is invalid; queue name cannot be empty")
	}

	if strings.ContainsFunc(name, unicode.IsSpace) {
		return reply.NewInvalidOption("queue name is invalid; '%s' cannot contain whitespace", name)
	}

	if strings.Contains(name, "~") {
		return reply.NewInvalidOption("queue name is invalid; '%s' cannot contain '~' character", name)
	}
	return nil
}

func allocInt32(mem int32) int {
	if mem < 0 {
		return defaultAllocation
	}
	if mem > maxAllocation {
		return defaultAllocation
	}
	return int(mem)
}

func (s *Service) validateQueueProduceProto(in *proto.QueueProduceRequest, out *types.ProduceRequest) error {
	var err error

	if in.RequestTimeout != "" {
		out.RequestTimeout, err = clock.ParseDuration(in.RequestTimeout)
		if err != nil {
			return reply.NewInvalidOption("request timeout is invalid; %s - expected format: 900ms, 5m or 15m", err.Error())
		}
	}

	for _, item := range in.Items {
		// TODO: From Memory Pool
		qi := new(types.Item)
		qi.Encoding = item.Encoding
		qi.Kind = item.Kind
		qi.Reference = item.Reference
		qi.EnqueueAt = item.EnqueueAt.AsTime()
		if item.Bytes != nil {
			qi.Payload = item.Bytes
		} else {
			qi.Payload = []byte(item.Utf8)
		}
		out.Items = append(out.Items, qi)
	}
	return nil
}

func (s *Service) validateQueueLeaseProto(in *proto.QueueLeaseRequest, out *types.LeaseRequest) error {
	var err error

	if in.RequestTimeout != "" {
		out.RequestTimeout, err = clock.ParseDuration(in.RequestTimeout)
		if err != nil {
			return reply.NewInvalidOption("request timeout is invalid; %s - expected format: 900ms, 5m or 15m", err.Error())
		}
	}

	out.ClientID = in.ClientId
	out.NumRequested = int(in.BatchSize)

	return nil
}

func (s *Service) validateQueueCompleteProto(in *proto.QueueCompleteRequest, out *types.CompleteRequest) error {
	var err error

	// TODO: Move this into Queue.Complete()
	// if strings.TrimSpace(in.QueueName) == "" {
	// 	return reply.NewInvalidOption("'queue_name' cannot be empty")
	// }

	if in.RequestTimeout != "" {
		out.RequestTimeout, err = clock.ParseDuration(in.RequestTimeout)
		if err != nil {
			return reply.NewInvalidOption("request timeout is invalid; %s - expected format: 900ms, 5m or 15m", err.Error())
		}
	}

	for _, id := range in.Ids {
		out.Ids = append(out.Ids, []byte(id))
	}

	out.Partition = int(in.Partition)

	return nil
}

func (s *Service) validateQueueRetryProto(in *proto.QueueRetryRequest, out *types.RetryRequest) error {
	// Note: RequestTimeout field is not defined in proto, using default timeout
	// TODO: Add RequestTimeout field to QueueRetryRequest proto definition
	out.RequestTimeout = clock.Duration(5 * clock.Minute)

	for _, item := range in.Items {
		retryItem := types.RetryItem{
			ID:   []byte(item.Id),
			Dead: item.Dead,
		}
		
		if item.RetryAt != nil {
			retryItem.RetryAt = clock.Time(item.RetryAt.AsTime())
		}
		
		out.Items = append(out.Items, retryItem)
	}

	out.Partition = int(in.Partition)

	return nil
}

func (s *Service) validateQueueOptionsProto(in *proto.QueueInfo, out *types.QueueInfo) error {
	var err error

	if len(in.LeaseTimeout) > maxTimeoutLength {
		return reply.NewInvalidOption("lease timeout is invalid; cannot be greater than '%d' characters", maxTimeoutLength)
	}

	if len(in.ExpireTimeout) > maxTimeoutLength {
		return reply.NewInvalidOption("expire timeout is invalid; cannot be greater than '%d' characters", maxTimeoutLength)
	}

	if in.ExpireTimeout != "" {
		out.ExpireTimeout, err = clock.ParseDuration(in.ExpireTimeout)
		if err != nil {
			return reply.NewInvalidOption("expire timeout is invalid; %s - expected format: 60m, 2h or 24h", err.Error())
		}
	}

	if in.LeaseTimeout != "" {
		out.LeaseTimeout, err = clock.ParseDuration(in.LeaseTimeout)
		if err != nil {
			return reply.NewInvalidOption("lease timeout is invalid; %s - expected format: 8m, 15m or 1h", err.Error())
		}
	}

	out.RequestedPartitions = int(in.RequestedPartitions)
	out.MaxAttempts = int(in.MaxAttempts)
	out.Namespace = in.Namespace
	out.DeadQueue = in.DeadQueue
	out.Reference = in.Reference
	out.Name = in.QueueName
	return nil
}

func (s *Service) validateNamespaceProto(in *proto.NamespaceInfo, out *types.Namespace) error {
	const maxNamespaceNameLength = 256

	if len(in.Name) > maxNamespaceNameLength {
		return reply.NewInvalidOption("namespace name is invalid; cannot be greater than '%d' characters", maxNamespaceNameLength)
	}

	if strings.TrimSpace(in.Name) == "" {
		return reply.NewInvalidOption("namespace name is invalid; cannot be empty")
	}

	if strings.ContainsFunc(in.Name, unicode.IsSpace) {
		return reply.NewInvalidOption("namespace name is invalid; '%s' cannot contain whitespace", in.Name)
	}

	if strings.Contains(in.Name, "~") {
		return reply.NewInvalidOption("namespace name is invalid; '%s' cannot contain '~' character", in.Name)
	}

	if strings.Contains(in.Name, ":") {
		return reply.NewInvalidOption("namespace name is invalid; '%s' cannot contain ':' character", in.Name)
	}

	if err := validateTag("api_key_tag", in.ApiKeyTag); err != nil {
		return err
	}

	out.APIKeyTag = in.ApiKeyTag
	out.Description = in.Description
	out.Name = in.Name
	return nil
}

func (s *Service) validateUserCreateProto(in *proto.UserCreateRequest, out *types.User) error {
	const maxUsernameLength = 128
	const maxEmailLength = 256
	const maxExternalIDLength = 256

	if len(in.Username) > maxUsernameLength {
		return reply.NewInvalidOption("username is invalid; cannot be greater than '%d' characters", maxUsernameLength)
	}

	if in.Username == "" {
		return reply.NewInvalidOption("username is invalid; cannot be empty")
	}

	if len(in.Email) > maxEmailLength {
		return reply.NewInvalidOption("email is invalid; cannot be greater than '%d' characters", maxEmailLength)
	}

	if len(in.ExternalId) > maxExternalIDLength {
		return reply.NewInvalidOption("external_id is invalid; cannot be greater than '%d' characters", maxExternalIDLength)
	}

	out.ExternalID = in.ExternalId
	out.Username = in.Username
	out.Email = in.Email
	return nil
}

func (s *Service) validateAPIKeyCreateProto(in *proto.APIKeyCreateRequest, out *types.APIKey) error {
	const maxNameLength = 128

	if in.UserId == "" {
		return reply.NewInvalidOption("user_id is invalid; cannot be empty")
	}

	if len(in.Name) > maxNameLength {
		return reply.NewInvalidOption("name is invalid; cannot be greater than '%d' characters", maxNameLength)
	}

	out.UserID = in.UserId
	out.Name = in.Name

	if in.NamespaceScope != "" {
		out.NamespaceScope = &in.NamespaceScope
	}

	if in.ExpiresAt != nil && in.ExpiresAt.IsValid() {
		expiresAt := clock.Time(in.ExpiresAt.AsTime())
		out.ExpiresAt = &expiresAt
	}

	if in.KeyTag != "" {
		if err := validateTag("key_tag", in.KeyTag); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) validateRoleCreateProto(in *proto.RoleCreateRequest, out *types.Role) error {
	const maxNameLength = 128

	if in.Namespace == "" {
		return reply.NewInvalidOption("namespace is invalid; cannot be empty")
	}

	if in.Name == "" {
		return reply.NewInvalidOption("name is invalid; cannot be empty")
	}

	if len(in.Name) > maxNameLength {
		return reply.NewInvalidOption("name is invalid; cannot be greater than '%d' characters", maxNameLength)
	}

	if strings.Contains(in.Name, ":") {
		return reply.NewInvalidOption("name is invalid; '%s' cannot contain ':' character", in.Name)
	}

	for _, perm := range in.Permissions {
		if !auth.IsValidPermission(perm) {
			return reply.NewInvalidOption("permission is invalid; '%s' is not a recognized permission", perm)
		}
	}

	out.Namespace = in.Namespace
	out.Name = in.Name
	out.Permissions = in.Permissions
	return nil
}

func (s *Service) validateRoleUpdateProto(in *proto.RoleUpdateRequest, out *types.Role) error {
	if in.Namespace == "" {
		return reply.NewInvalidOption("namespace is invalid; cannot be empty")
	}

	if in.Name == "" {
		return reply.NewInvalidOption("name is invalid; cannot be empty")
	}

	for _, perm := range in.Permissions {
		if !auth.IsValidPermission(perm) {
			return reply.NewInvalidOption("permission is invalid; '%s' is not a recognized permission", perm)
		}
	}

	out.Namespace = in.Namespace
	out.Name = in.Name
	out.Permissions = in.Permissions
	return nil
}

func (s *Service) validateRoleBindingCreateProto(in *proto.RoleBindingCreateRequest, out *types.RoleBinding) error {
	if in.Namespace == "" {
		return reply.NewInvalidOption("namespace is invalid; cannot be empty")
	}

	if in.RoleName == "" {
		return reply.NewInvalidOption("role_name is invalid; cannot be empty")
	}

	if in.UserId == "" {
		return reply.NewInvalidOption("user_id is invalid; cannot be empty")
	}

	out.Namespace = in.Namespace
	out.UserID = in.UserId
	return nil
}
