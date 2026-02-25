package transport

import "github.com/kapetan-io/querator/reply"

// Type aliases -- these are identical types, not wrappers
type ErrRequestFailed = reply.ErrRequestFailed
type ErrInvalidOption = reply.ErrInvalidOption
type ErrRetryRequest = reply.ErrRetryRequest
type ErrConflict = reply.ErrConflict
type ErrUnauthorized = reply.ErrUnauthorized
type ErrForbidden = reply.ErrForbidden

// Constructor aliases
var (
	NewRequestFailed = reply.NewRequestFailed
	NewInvalidOption = reply.NewInvalidOption
	NewRetryRequest  = reply.NewRetryRequest
	NewConflict      = reply.NewConflict
	NewUnauthorized  = reply.NewUnauthorized
	NewForbidden     = reply.NewForbidden
)
