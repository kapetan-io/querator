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
	detailQueuesChanged      = "queues_changed"
	detailPartitionsCreated  = "partitions_created"
	detailPartitionsDraining = "partitions_draining"
	detailQueueName          = "queue_name"
)

type RebalanceCounts struct {
	QueuesChanged      int
	PartitionsCreated  int
	PartitionsDraining int
}

func ParseRebalanceStopped(details map[string]string) (RebalanceCounts, string, bool) {
	queueName := details[detailQueueName]
	changed, changedErr := strconv.Atoi(details[detailQueuesChanged])
	created, createdErr := strconv.Atoi(details[detailPartitionsCreated])
	draining, drainingErr := strconv.Atoi(details[detailPartitionsDraining])
	if queueName == "" || changedErr != nil || createdErr != nil || drainingErr != nil {
		return RebalanceCounts{}, "", false
	}
	return RebalanceCounts{QueuesChanged: changed, PartitionsCreated: created, PartitionsDraining: draining}, queueName, true
}

type ErrRebalanceStopped struct {
	queueName string
	saved     RebalanceCounts
	cause     error
}

func NewRebalanceStopped(queueName string, saved RebalanceCounts, cause error) *ErrRebalanceStopped {
	return &ErrRebalanceStopped{queueName: queueName, saved: saved, cause: cause}
}

func (e *ErrRebalanceStopped) Error() string {
	return fmt.Sprintf("rebalance stopped at queue '%s': %s", e.queueName, e.cause)
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
		detailQueuesChanged:      strconv.Itoa(e.saved.QueuesChanged),
		detailPartitionsCreated:  strconv.Itoa(e.saved.PartitionsCreated),
		detailPartitionsDraining: strconv.Itoa(e.saved.PartitionsDraining),
		detailQueueName:          e.queueName,
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
