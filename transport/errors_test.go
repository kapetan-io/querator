package transport

import (
	"errors"
	"fmt"
	"github.com/duh-rpc/duh-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewErrInvalid(t *testing.T) {
	in := NewInvalidOption("invalid key")
	assert.Equal(t, "invalid key", in.Error())
	err := fmt.Errorf("wrap: %w", in)
	assert.Equal(t, "wrap: invalid key", err.Error())

	var d duh.Error
	require.True(t, errors.As(err, &d))
	assert.Equal(t, "invalid key", d.Error())
	assert.Equal(t, "invalid key", d.Message())
}
