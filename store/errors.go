package store

import (
	"errors"
	"fmt"
)

// ErrInvalidOption is used to tell the caller that the call was invalid
type ErrInvalidOption struct {
	Msg string
}

func NewInvalidOption(msg string, args ...any) *ErrInvalidOption {
	return &ErrInvalidOption{Msg: fmt.Sprintf(msg, args...)}
}

func (e *ErrInvalidOption) Error() string {
	return e.Msg
}

func (e *ErrInvalidOption) Is(target error) bool {
	var err *ErrInvalidOption
	return errors.As(target, &err)
}

func IsErrInvalid(target error) bool {
	var err *ErrInvalidOption
	return errors.As(target, &err)
}
