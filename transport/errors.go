package transport

import (
	"fmt"
	"github.com/duh-rpc/duh-go"
	v1 "github.com/duh-rpc/duh-go/proto/v1"
	"github.com/kapetan-io/errors"
	"google.golang.org/protobuf/proto"
)

// -------------------------------------------------

// ErrRequestFailed is used to tell the client that the request was valid, but it failed for some reason.
type ErrRequestFailed struct {
	msg string
}

func NewRequestFailed(msg string, args ...any) *ErrRequestFailed {
	return &ErrRequestFailed{msg: fmt.Sprintf(msg, args...)}
}

func (e *ErrRequestFailed) Error() string {
	return e.msg
}

func (e *ErrRequestFailed) Is(target error) bool {
	var err *ErrRequestFailed
	return errors.As(target, &err)
}

func (e *ErrRequestFailed) Code() int {
	return duh.CodeRequestFailed
}

func (e *ErrRequestFailed) ProtoMessage() proto.Message {
	return &v1.Reply{
		Message:  e.msg,
		CodeText: duh.CodeText(duh.CodeRequestFailed),
		Code:     int32(duh.CodeRequestFailed),
		Details:  nil,
	}
}

func (e *ErrRequestFailed) Details() map[string]string {
	return nil
}

func (e *ErrRequestFailed) Message() string {
	return e.msg
}

var _ duh.Error = &ErrRequestFailed{}

// -------------------------------------------------

// ErrInvalidOption is used to indicate an option provided was invalid for some reason
type ErrInvalidOption struct {
	msg string
}

func NewInvalidOption(msg string, args ...any) *ErrInvalidOption {
	return &ErrInvalidOption{msg: fmt.Sprintf(msg, args...)}
}

func (e *ErrInvalidOption) Error() string {
	return e.msg
}

func (e *ErrInvalidOption) Is(target error) bool {
	var err *ErrInvalidOption
	return errors.As(target, &err)
}

func (e *ErrInvalidOption) Code() int {
	return duh.CodeBadRequest
}

func (e *ErrInvalidOption) ProtoMessage() proto.Message {
	return &v1.Reply{
		Message:  e.msg,
		CodeText: duh.CodeText(duh.CodeBadRequest),
		Code:     int32(duh.CodeBadRequest),
		Details:  nil,
	}
}

func (e *ErrInvalidOption) Details() map[string]string {
	return nil
}

func (e *ErrInvalidOption) Message() string {
	return e.msg
}

var _ duh.Error = &ErrInvalidOption{}

// -------------------------------------------------

// ErrRetryRequest is used to tell the client that the request was valid, the server did not encounter a failure, but
// the request did not succeed. The client should retry
type ErrRetryRequest struct {
	msg string
}

func NewRetryRequest(msg string, args ...any) *ErrRetryRequest {
	return &ErrRetryRequest{msg: fmt.Sprintf(msg, args...)}
}

func (e *ErrRetryRequest) Error() string {
	return e.msg
}

func (e *ErrRetryRequest) Is(target error) bool {
	var err *ErrRetryRequest
	return errors.As(target, &err)
}

func (e *ErrRetryRequest) Code() int {
	return duh.CodeRetryRequest
}

func (e *ErrRetryRequest) ProtoMessage() proto.Message {
	return &v1.Reply{
		Message:  e.msg,
		CodeText: duh.CodeText(duh.CodeRetryRequest),
		Code:     int32(duh.CodeRetryRequest),
		Details:  nil,
	}
}

func (e *ErrRetryRequest) Details() map[string]string {
	return nil
}

func (e *ErrRetryRequest) Message() string {
	return e.msg
}

var _ duh.Error = &ErrRetryRequest{}

// -------------------------------------------------

// ErrConflict is used to indicate there was a conflict
type ErrConflict struct {
	msg string
}

func NewConflict(msg string, args ...any) *ErrConflict {
	return &ErrConflict{
		msg: fmt.Sprintf(msg, args...),
	}
}

func (e *ErrConflict) Error() string {
	return e.msg
}

func (e *ErrConflict) Is(target error) bool {
	var err *ErrConflict
	return errors.As(target, &err)
}

func (e *ErrConflict) Code() int {
	return duh.CodeBadRequest
}

func (e *ErrConflict) ProtoMessage() proto.Message {
	return &v1.Reply{
		Message:  e.msg,
		CodeText: duh.CodeText(duh.CodeBadRequest),
		Code:     int32(duh.CodeBadRequest),
		Details:  nil,
	}
}

func (e *ErrConflict) Details() map[string]string {
	return nil
}

func (e *ErrConflict) Message() string {
	return e.msg
}

var _ duh.Error = &ErrConflict{}
