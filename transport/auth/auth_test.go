package auth_test

import (
	"testing"

	"github.com/kapetan-io/querator/transport/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdminPermissionsIndependentOfAllPermissions(t *testing.T) {
	originalLen := len(auth.AllPermissions)

	// Append to AdminPermissions should not affect AllPermissions
	auth.AdminPermissions = append(auth.AdminPermissions, "test.permission")

	require.Equal(t, originalLen, len(auth.AllPermissions))
	assert.NotContains(t, auth.AllPermissions, "test.permission")
}
