package auth_test

import (
	"testing"

	"github.com/kapetan-io/querator/transport/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdminPermissionsIndependentOfAllPermissions(t *testing.T) {
	// Both functions must return slices with the same contents
	require.ElementsMatch(t, auth.AllPermissions(), auth.AdminPermissions())

	// Each call returns a fresh slice; lengths must be stable across calls
	require.Equal(t, len(auth.AllPermissions()), len(auth.AdminPermissions()))
	assert.NotContains(t, auth.AllPermissions(), "test.permission")
	assert.NotContains(t, auth.AdminPermissions(), "test.permission")
}
