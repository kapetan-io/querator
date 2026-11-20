package types

import "github.com/kapetan-io/querator/transport/reply"

const (
	KindNamespaceAlreadyExists   reply.Kind = "namespace.already_exists"
	KindUserAlreadyExists        reply.Kind = "user.already_exists"
	KindUsernameAlreadyTaken     reply.Kind = "user.username_taken"
	KindRoleAlreadyExists        reply.Kind = "role.already_exists"
	KindRoleBindingAlreadyExists reply.Kind = "role_binding.already_exists"
	KindRoleBindingLimitReached  reply.Kind = "role_binding.limit_reached"
)

var (
	ErrNamespaceAlreadyExists   = reply.NewInvalidOptionKind(KindNamespaceAlreadyExists, "")
	ErrUserAlreadyExists        = reply.NewInvalidOptionKind(KindUserAlreadyExists, "")
	ErrUsernameAlreadyTaken     = reply.NewInvalidOptionKind(KindUsernameAlreadyTaken, "")
	ErrRoleAlreadyExists        = reply.NewInvalidOptionKind(KindRoleAlreadyExists, "")
	ErrRoleBindingAlreadyExists = reply.NewInvalidOptionKind(KindRoleBindingAlreadyExists, "")
	ErrRoleBindingLimitReached  = reply.NewInvalidOptionKind(KindRoleBindingLimitReached, "")
)

func NewErrNamespaceNotExist(name string) *reply.ErrRequestFailed {
	return reply.NewRequestFailed("namespace does not exist; '%s' was not found", name)
}

func NewErrNamespaceAlreadyExists(name string) *reply.ErrInvalidOption {
	return reply.NewInvalidOptionKind(KindNamespaceAlreadyExists, "namespace already exists; '%s' already exists", name)
}

func NewErrNamespaceHasQueues(name string) *reply.ErrInvalidOption {
	return reply.NewInvalidOption("namespace has queues; '%s' has queues, delete queues first", name)
}

func NewErrNamespaceReserved(name string) *reply.ErrInvalidOption {
	return reply.NewInvalidOption("namespace name is reserved; '%s' names starting with '_' are reserved", name)
}

func NewErrUserNotExist(identifier string) *reply.ErrRequestFailed {
	return reply.NewRequestFailed("user does not exist; '%s' was not found", identifier)
}

func NewErrUserAlreadyExists(id string) *reply.ErrInvalidOption {
	return reply.NewInvalidOptionKind(KindUserAlreadyExists, "user already exists; '%s' already exists", id)
}

func NewErrUsernameAlreadyTaken(username string) *reply.ErrInvalidOption {
	return reply.NewInvalidOptionKind(KindUsernameAlreadyTaken, "username is already taken; '%s' is already taken", username)
}

var (
	ErrAPIKeyNotExist = reply.NewUnauthorized("api key does not exist")
	ErrAPIKeyExpired  = reply.NewUnauthorized("api key has expired")
	ErrAPIKeyInvalid  = reply.NewUnauthorized("api key is invalid")
)

func NewErrRoleNotExist(identifier string) *reply.ErrRequestFailed {
	return reply.NewRequestFailed("role does not exist; '%s' was not found", identifier)
}

func NewErrRoleAlreadyExists(role Role) *reply.ErrInvalidOption {
	return reply.NewInvalidOptionKind(KindRoleAlreadyExists, "role already exists; '%s:%s' already exists", role.Namespace, role.Name)
}

func NewErrRoleBindingAlreadyExists(binding RoleBinding) *reply.ErrInvalidOption {
	return reply.NewInvalidOptionKind(KindRoleBindingAlreadyExists, "role binding already exists; binding for user '%s' to role '%s' in namespace '%s' already exists", binding.UserID, binding.RoleID, binding.Namespace)
}

func NewErrRoleBindingLimitReached(binding RoleBinding) *reply.ErrInvalidOption {
	return reply.NewInvalidOptionKind(KindRoleBindingLimitReached, "user_id is invalid; user '%s' already holds %d role bindings in namespace '%s', the most one user can hold in one namespace", binding.UserID, MaxRoleBindingsPerUserInNamespace, binding.Namespace)
}

func NewErrRoleHasBindings(name string) *reply.ErrInvalidOption {
	return reply.NewInvalidOption("role has active bindings; '%s' has active bindings, delete bindings first", name)
}

func NewErrRoleIsStandard(name string) *reply.ErrInvalidOption {
	return reply.NewInvalidOption("cannot modify or delete standard role; '%s' is a standard role", name)
}

func NewErrRoleBindingNotExist(id string) *reply.ErrRequestFailed {
	return reply.NewRequestFailed("role binding does not exist; '%s' was not found", id)
}

func NewErrNamespaceHasRoles(name string) *reply.ErrInvalidOption {
	return reply.NewInvalidOption("namespace has roles; '%s' has roles, delete roles first", name)
}

func NewErrNamespaceHasRoleBindings(name string) *reply.ErrInvalidOption {
	return reply.NewInvalidOption("namespace has role bindings; '%s' has role bindings, delete role bindings first", name)
}
