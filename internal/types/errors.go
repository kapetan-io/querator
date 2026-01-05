package types

import "github.com/kapetan-io/querator/transport"

// Namespace errors
var (
	ErrNamespaceNotExist      = transport.NewRequestFailed("namespace does not exist")
	ErrNamespaceAlreadyExists = transport.NewInvalidOption("namespace already exists")
	ErrNamespaceHasQueues     = transport.NewInvalidOption("namespace has queues; delete queues first")
	ErrNamespaceReserved      = transport.NewInvalidOption("namespace name is reserved; names starting with '_' are reserved")
)

// User errors
var (
	ErrUserNotExist         = transport.NewRequestFailed("user does not exist")
	ErrUserAlreadyExists    = transport.NewInvalidOption("user already exists")
	ErrUsernameAlreadyTaken = transport.NewInvalidOption("username is already taken")
)

// API Key errors
var (
	ErrAPIKeyNotExist = transport.NewUnauthorized("api key does not exist")
	ErrAPIKeyExpired  = transport.NewUnauthorized("api key has expired")
	ErrAPIKeyInvalid  = transport.NewUnauthorized("api key is invalid")
)

// Authorization errors
var (
	ErrAuthRequired = transport.NewUnauthorized("authentication required")
	ErrAccessDenied = transport.NewForbidden("access denied")
)
