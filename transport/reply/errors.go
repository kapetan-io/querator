package reply

import (
	"fmt"
	"strconv"

	"github.com/duh-rpc/duh-go"
	v1 "github.com/duh-rpc/duh-go/proto/v1"
	"github.com/kapetan-io/errors"
	"google.golang.org/protobuf/proto"
)

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

type Kind string

type ErrInvalidOption struct {
	msg  string
	kind Kind
}

func NewInvalidOption(msg string, args ...any) *ErrInvalidOption {
	return &ErrInvalidOption{msg: fmt.Sprintf(msg, args...)}
}

func NewInvalidOptionKind(kind Kind, msg string, args ...any) *ErrInvalidOption {
	return &ErrInvalidOption{kind: kind, msg: fmt.Sprintf(msg, args...)}
}

func (e *ErrInvalidOption) Error() string {
	return e.msg
}

func (e *ErrInvalidOption) Is(target error) bool {
	var t *ErrInvalidOption
	if !errors.As(target, &t) {
		return false
	}
	if t.kind == "" {
		return true
	}
	return e.kind == t.kind
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

type ErrUnauthorized struct {
	msg string
}

func NewUnauthorized(msg string, args ...any) *ErrUnauthorized {
	return &ErrUnauthorized{msg: fmt.Sprintf(msg, args...)}
}

func (e *ErrUnauthorized) Error() string {
	return e.msg
}

func (e *ErrUnauthorized) Is(target error) bool {
	var err *ErrUnauthorized
	return errors.As(target, &err)
}

func (e *ErrUnauthorized) Code() int {
	return duh.CodeUnauthorized
}

func (e *ErrUnauthorized) ProtoMessage() proto.Message {
	return &v1.Reply{
		Message:  e.msg,
		CodeText: duh.CodeText(duh.CodeUnauthorized),
		Code:     int32(duh.CodeUnauthorized),
		Details:  nil,
	}
}

func (e *ErrUnauthorized) Details() map[string]string {
	return nil
}

func (e *ErrUnauthorized) Message() string {
	return e.msg
}

var _ duh.Error = &ErrUnauthorized{}

type ErrForbidden struct {
	msg string
}

func NewForbidden(msg string, args ...any) *ErrForbidden {
	return &ErrForbidden{msg: fmt.Sprintf(msg, args...)}
}

func (e *ErrForbidden) Error() string {
	return e.msg
}

func (e *ErrForbidden) Is(target error) bool {
	var err *ErrForbidden
	return errors.As(target, &err)
}

func (e *ErrForbidden) Code() int {
	return duh.CodeForbidden
}

func (e *ErrForbidden) ProtoMessage() proto.Message {
	return &v1.Reply{
		Message:  e.msg,
		CodeText: duh.CodeText(duh.CodeForbidden),
		Code:     int32(duh.CodeForbidden),
		Details:  nil,
	}
}

func (e *ErrForbidden) Details() map[string]string {
	return nil
}

func (e *ErrForbidden) Message() string {
	return e.msg
}

var _ duh.Error = &ErrForbidden{}

const (
	DetailQueuesChanged      = "queues_changed"
	DetailPartitionsCreated  = "partitions_created"
	DetailPartitionsDraining = "partitions_draining"
	DetailQueueName          = "queue_name"
)

type ErrRebalanceStopped struct {
	cause              error
	queueName          string
	queuesChanged      int
	partitionsCreated  int
	partitionsDraining int
}

func NewRebalanceStopped(cause error, queueName string, queuesChanged, partitionsCreated,
	partitionsDraining int) *ErrRebalanceStopped {
	return &ErrRebalanceStopped{
		cause:              cause,
		queueName:          queueName,
		queuesChanged:      queuesChanged,
		partitionsCreated:  partitionsCreated,
		partitionsDraining: partitionsDraining,
	}
}

func (e *ErrRebalanceStopped) Error() string {
	return fmt.Sprintf("rebalance stopped at queue '%s' after saving %d queues changed, %d partitions created, "+
		"%d partitions draining: %s", e.queueName, e.queuesChanged, e.partitionsCreated, e.partitionsDraining, e.cause)
}

func (e *ErrRebalanceStopped) Unwrap() error {
	return e.cause
}

func (e *ErrRebalanceStopped) Is(target error) bool {
	var err *ErrRebalanceStopped
	return errors.As(target, &err)
}

func (e *ErrRebalanceStopped) Code() int {
	var cause duh.Error
	if errors.As(e.cause, &cause) {
		return cause.Code()
	}
	return duh.CodeInternalError
}

func (e *ErrRebalanceStopped) ProtoMessage() proto.Message {
	return &v1.Reply{
		Message:  e.Message(),
		CodeText: duh.CodeText(e.Code()),
		Code:     int32(e.Code()),
		Details:  e.Details(),
	}
}

func (e *ErrRebalanceStopped) Details() map[string]string {
	return map[string]string{
		DetailQueuesChanged:      strconv.Itoa(e.queuesChanged),
		DetailPartitionsCreated:  strconv.Itoa(e.partitionsCreated),
		DetailPartitionsDraining: strconv.Itoa(e.partitionsDraining),
		DetailQueueName:          e.queueName,
	}
}

func (e *ErrRebalanceStopped) Message() string {
	var cause duh.Error
	if errors.As(e.cause, &cause) && cause.Code() != duh.CodeInternalError {
		return cause.Message()
	}
	return "Internal Error"
}

var _ duh.Error = &ErrRebalanceStopped{}
