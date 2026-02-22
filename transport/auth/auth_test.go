package auth_test

import (
	"testing"

	tauth "github.com/kapetan-io/querator/transport/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdminPermissionsIndependentOfAllPermissions(t *testing.T) {
	originalLen := len(tauth.AllPermissions)

	// Append to AdminPermissions should not affect AllPermissions
	tauth.AdminPermissions = append(tauth.AdminPermissions, "test.permission")

	require.Equal(t, originalLen, len(tauth.AllPermissions))
	assert.NotContains(t, tauth.AllPermissions, "test.permission")
}
