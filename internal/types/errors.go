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

// Namespace errors

func ErrNamespaceNotExist(name string) *reply.ErrRequestFailed {
	return reply.NewRequestFailed("namespace does not exist; '%s' was not found", name)
}

func ErrNamespaceAlreadyExists(name string) *reply.ErrInvalidOption {
	return reply.NewInvalidOptionKind(KindNamespaceAlreadyExists, "namespace already exists; '%s' already exists", name)
}

func ErrNamespaceHasQueues(name string) *reply.ErrInvalidOption {
	return reply.NewInvalidOption("namespace has queues; '%s' has queues, delete queues first", name)
}

func ErrNamespaceReserved(name string) *reply.ErrInvalidOption {
	return reply.NewInvalidOption("namespace name is reserved; '%s' names starting with '_' are reserved", name)
}

// User errors

func ErrUserNotExist(identifier string) *reply.ErrRequestFailed {
	return reply.NewRequestFailed("user does not exist; '%s' was not found", identifier)
}

func ErrUserAlreadyExists(id string) *reply.ErrInvalidOption {
	return reply.NewInvalidOptionKind(KindUserAlreadyExists, "user already exists; '%s' already exists", id)
}

func ErrUsernameAlreadyTaken(username string) *reply.ErrInvalidOption {
	return reply.NewInvalidOptionKind(KindUsernameAlreadyTaken, "username is already taken; '%s' is already taken", username)
}

// API Key errors — kept as static sentinels for security (no details)
var (
	ErrAPIKeyNotExist = reply.NewUnauthorized("api key does not exist")
	ErrAPIKeyExpired  = reply.NewUnauthorized("api key has expired")
	ErrAPIKeyInvalid  = reply.NewUnauthorized("api key is invalid")
)

// Role errors

func ErrRoleNotExist(identifier string) *reply.ErrRequestFailed {
	return reply.NewRequestFailed("role does not exist; '%s' was not found", identifier)
}

func ErrRoleAlreadyExists(namespace, name string) *reply.ErrInvalidOption {
	return reply.NewInvalidOptionKind(KindRoleAlreadyExists, "role already exists; '%s:%s' already exists", namespace, name)
}

func ErrRoleBindingAlreadyExists(namespace, userID, roleID string) *reply.ErrInvalidOption {
	return reply.NewInvalidOptionKind(KindRoleBindingAlreadyExists, "role binding already exists; binding for user '%s' to role '%s' in namespace '%s' already exists", userID, roleID, namespace)
}

func ErrRoleHasBindings(name string) *reply.ErrInvalidOption {
	return reply.NewInvalidOption("role has active bindings; '%s' has active bindings, delete bindings first", name)
}

func ErrRoleIsStandard(name string) *reply.ErrInvalidOption {
	return reply.NewInvalidOption("cannot modify or delete standard role; '%s' is a standard role", name)
}

func ErrRoleBindingNotExist(id string) *reply.ErrRequestFailed {
	return reply.NewRequestFailed("role binding does not exist; '%s' was not found", id)
}

// Namespace auth resource errors

func ErrNamespaceHasRoles(name string) *reply.ErrInvalidOption {
	return reply.NewInvalidOption("namespace has roles; '%s' has roles, delete roles first", name)
}

func ErrNamespaceHasRoleBindings(name string) *reply.ErrInvalidOption {
	return reply.NewInvalidOption("namespace has role bindings; '%s' has role bindings, delete role bindings first", name)
}
