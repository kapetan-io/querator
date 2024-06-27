package store

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewErrInvalid(t *testing.T) {
	err := NewInvalidOption("invalid key")
	assert.Equal(t, "invalid key", err.Error())
	assert.True(t, IsErrInvalidOption(err))
}
