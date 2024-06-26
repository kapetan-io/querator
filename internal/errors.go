package internal

import (
	"fmt"
	"github.com/kapetan-io/errors"
)

// ErrInternal is any error un handled by ErrRequestFailed or ErrBadRequest. The message
// and any additional fields are logged but not returned to the client
type ErrInternal errors.Fields

// ErrRequestFailed is used to tell the client that the request was valid, but it failed for some reason.
type ErrRequestFailed struct {
	Msg string
}

func NewRequestFailed(msg string, args ...any) *ErrRequestFailed {
	return &ErrRequestFailed{Msg: fmt.Sprintf(msg, args...)}
}

func (e *ErrRequestFailed) Error() string {
	return e.Msg
}

func (e *ErrRequestFailed) Is(target error) bool {
	var err *ErrRequestFailed
	return errors.As(target, &err)
}

// ErrRetryRequest is used to tell the client that the request was valid, the server did not encounter a failure, but
// the request did not succeed. The client should retry
type ErrRetryRequest struct {
	Msg string
}

func NewRetryRequest(msg string, args ...any) *ErrRetryRequest {
	return &ErrRetryRequest{Msg: fmt.Sprintf(msg, args...)}
}

func (e *ErrRetryRequest) Error() string {
	return e.Msg
}

func (e *ErrRetryRequest) Is(target error) bool {
	var err *ErrRetryRequest
	return errors.As(target, &err)
}

// ErrBadRequest is used to indicate the client's request was invalid for some reason
type ErrBadRequest struct {
	Msg string
}

func NewBadRequest(msg string, args ...any) *ErrBadRequest {
	return &ErrBadRequest{Msg: fmt.Sprintf(msg, args...)}
}

func (e *ErrBadRequest) Error() string {
	return e.Msg
}

func (e *ErrBadRequest) Is(target error) bool {
	var err *ErrBadRequest
	return errors.As(target, &err)
}
