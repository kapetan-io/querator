package types

import "github.com/kapetan-io/querator/transport/reply"

// Kind constants for errors that must be sentinel-matched with errors.Is
const (
	KindNamespaceAlreadyExists   = "namespace.already_exists"
	KindUserAlreadyExists        = "user.already_exists"
	KindUsernameAlreadyTaken     = "user.username_taken"
	KindRoleAlreadyExists        = "role.already_exists"
	KindRoleBindingAlreadyExists = "role_binding.already_exists"
)

// Sentinels for errors.Is matching — use these instead of calling constructors with empty strings.
var (
	ErrNamespaceAlreadyExists   = reply.NewInvalidOptionKind(KindNamespaceAlreadyExists, "")
	ErrUserAlreadyExists        = reply.NewInvalidOptionKind(KindUserAlreadyExists, "")
	ErrUsernameAlreadyTaken     = reply.NewInvalidOptionKind(KindUsernameAlreadyTaken, "")
	ErrRoleAlreadyExists        = reply.NewInvalidOptionKind(KindRoleAlreadyExists, "")
	ErrRoleBindingAlreadyExists = reply.NewInvalidOptionKind(KindRoleBindingAlreadyExists, "")
)

// Namespace errors

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

// User errors

func NewErrUserNotExist(identifier string) *reply.ErrRequestFailed {
	return reply.NewRequestFailed("user does not exist; '%s' was not found", identifier)
}

func NewErrUserAlreadyExists(id string) *reply.ErrInvalidOption {
	return reply.NewInvalidOptionKind(KindUserAlreadyExists, "user already exists; '%s' already exists", id)
}

func NewErrUsernameAlreadyTaken(username string) *reply.ErrInvalidOption {
	return reply.NewInvalidOptionKind(KindUsernameAlreadyTaken, "username is already taken; '%s' is already taken", username)
}

// API Key errors — kept as static sentinels for security (no details)
var (
	ErrAPIKeyNotExist = reply.NewUnauthorized("api key does not exist")
	ErrAPIKeyExpired  = reply.NewUnauthorized("api key has expired")
	ErrAPIKeyInvalid  = reply.NewUnauthorized("api key is invalid")
)

// Role errors

func NewErrRoleNotExist(identifier string) *reply.ErrRequestFailed {
	return reply.NewRequestFailed("role does not exist; '%s' was not found", identifier)
}

func NewErrRoleAlreadyExists(namespace, name string) *reply.ErrInvalidOption {
	return reply.NewInvalidOptionKind(KindRoleAlreadyExists, "role already exists; '%s:%s' already exists", namespace, name)
}

func NewErrRoleBindingAlreadyExists(userID, roleID, namespace string) *reply.ErrInvalidOption {
	return reply.NewInvalidOptionKind(KindRoleBindingAlreadyExists, "role binding already exists; binding for user '%s' to role '%s' in namespace '%s' already exists", userID, roleID, namespace)
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

// Namespace auth resource errors

func NewErrNamespaceHasRoles(name string) *reply.ErrInvalidOption {
	return reply.NewInvalidOption("namespace has roles; '%s' has roles, delete roles first", name)
}

func NewErrNamespaceHasRoleBindings(name string) *reply.ErrInvalidOption {
	return reply.NewInvalidOption("namespace has role bindings; '%s' has role bindings, delete role bindings first", name)
}
