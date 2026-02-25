package types

import "github.com/kapetan-io/querator/transport/reply"

// Namespace errors
var (
	ErrNamespaceNotExist      = reply.NewRequestFailed("namespace does not exist")
	ErrNamespaceAlreadyExists = reply.NewInvalidOption("namespace already exists")
	ErrNamespaceHasQueues     = reply.NewInvalidOption("namespace has queues; delete queues first")
	ErrNamespaceReserved      = reply.NewInvalidOption("namespace name is reserved; names starting with '_' are reserved")
)

// User errors
var (
	ErrUserNotExist         = reply.NewRequestFailed("user does not exist")
	ErrUserAlreadyExists    = reply.NewInvalidOption("user already exists")
	ErrUsernameAlreadyTaken = reply.NewInvalidOption("username is already taken")
)

// API Key errors
var (
	ErrAPIKeyNotExist = reply.NewUnauthorized("api key does not exist")
	ErrAPIKeyExpired  = reply.NewUnauthorized("api key has expired")
	ErrAPIKeyInvalid  = reply.NewUnauthorized("api key is invalid")
)

// Authorization errors
var (
	ErrAuthRequired = reply.NewUnauthorized("authentication required")
	ErrAccessDenied = reply.NewForbidden("access denied")
)

// Role errors
var (
	ErrRoleNotExist             = reply.NewRequestFailed("role does not exist")
	ErrRoleAlreadyExists        = reply.NewInvalidOption("role already exists")
	ErrRoleBindingAlreadyExists = reply.NewInvalidOption("role binding already exists")
	ErrRoleHasBindings          = reply.NewInvalidOption("role has active bindings; delete bindings first")
	ErrRoleIsStandard           = reply.NewInvalidOption("cannot modify or delete standard role")
	ErrRoleBindingNotExist      = reply.NewRequestFailed("role binding does not exist")
)

// Namespace auth resource errors
var (
	ErrNamespaceHasRoles        = reply.NewInvalidOption("namespace has roles; delete roles first")
	ErrNamespaceHasRoleBindings = reply.NewInvalidOption("namespace has role bindings; delete role bindings first")
)
