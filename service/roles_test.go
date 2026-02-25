package service_test

import (
	"fmt"
	"testing"

	"github.com/duh-rpc/duh-go"
	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	pb "github.com/kapetan-io/querator/proto"
	svc "github.com/kapetan-io/querator/service"
	"github.com/kapetan-io/querator/transport/auth"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoles(t *testing.T) {
	for _, tc := range []struct {
		Setup NewStorageFunc
		Name  string
	}{
		{
			Name: "InMemory",
			Setup: func() store.Config {
				return setupMemoryStorage(store.Config{})
			},
		},
		{
			Name: "BadgerDB",
			Setup: func() store.Config {
				badger := &badgerTestSetup{Dir: t.TempDir()}
				t.Cleanup(func() {
					badger.Teardown()
				})
				return badger.Setup(store.BadgerConfig{})
			},
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			testRoles(t, tc.Setup)
		})
	}
}

func testRoles(t *testing.T, setup NewStorageFunc) {
	t.Run("RoleCreateAndList", func(t *testing.T) {
		d, c, ctx := newDaemon(t, clock.Minute*10, svc.Config{StorageConfig: setup()})
		defer d.Shutdown(t)

		ns := random.String("ns-", 10)
		err := c.NamespacesCreate(ctx, &pb.NamespaceInfo{Name: ns})
		require.NoError(t, err)

		// Create a role with specific permissions
		roleName := "test-role-" + random.String("", 5)
		var createRes pb.RoleCreateResponse
		err = c.RolesCreate(ctx, &pb.RoleCreateRequest{
			Permissions: []string{auth.QueueCreate, auth.QueueList},
			Namespace:   ns,
			Name:        roleName,
		}, &createRes)
		require.NoError(t, err)
		require.NotEmpty(t, createRes.Id)

		// List roles and verify
		var listRes pb.RolesListResponse
		err = c.RolesList(ctx, ns, &listRes, nil)
		require.NoError(t, err)
		require.Len(t, listRes.Items, 1)
		assert.Equal(t, createRes.Id, listRes.Items[0].Id)
		assert.Equal(t, roleName, listRes.Items[0].Name)
		assert.Equal(t, ns, listRes.Items[0].Namespace)
		assert.ElementsMatch(t, []string{auth.QueueCreate, auth.QueueList}, listRes.Items[0].Permissions)
	})

	t.Run("RoleUpdate", func(t *testing.T) {
		d, c, ctx := newDaemon(t, clock.Minute*10, svc.Config{StorageConfig: setup()})
		defer d.Shutdown(t)

		ns := random.String("ns-", 10)
		err := c.NamespacesCreate(ctx, &pb.NamespaceInfo{Name: ns})
		require.NoError(t, err)

		// Create a role with initial permissions
		roleName := "update-role-" + random.String("", 5)
		var createRes pb.RoleCreateResponse
		err = c.RolesCreate(ctx, &pb.RoleCreateRequest{
			Permissions: []string{auth.QueueCreate},
			Namespace:   ns,
			Name:        roleName,
		}, &createRes)
		require.NoError(t, err)

		// Update the role with new permissions
		err = c.RolesUpdate(ctx, &pb.RoleUpdateRequest{
			Permissions: []string{auth.QueueCreate, auth.QueueDelete},
			Namespace:   ns,
			Name:        roleName,
		})
		require.NoError(t, err)

		// List roles and verify updated permissions
		var listRes pb.RolesListResponse
		err = c.RolesList(ctx, ns, &listRes, nil)
		require.NoError(t, err)
		require.Len(t, listRes.Items, 1)
		assert.ElementsMatch(t, []string{auth.QueueCreate, auth.QueueDelete}, listRes.Items[0].Permissions)
	})

	t.Run("RoleDelete", func(t *testing.T) {
		d, c, ctx := newDaemon(t, clock.Minute*10, svc.Config{StorageConfig: setup()})
		defer d.Shutdown(t)

		ns := random.String("ns-", 10)
		err := c.NamespacesCreate(ctx, &pb.NamespaceInfo{Name: ns})
		require.NoError(t, err)

		// Create a role
		roleName := "delete-role-" + random.String("", 5)
		var createRes pb.RoleCreateResponse
		err = c.RolesCreate(ctx, &pb.RoleCreateRequest{
			Permissions: []string{auth.QueueList},
			Namespace:   ns,
			Name:        roleName,
		}, &createRes)
		require.NoError(t, err)

		// Delete the role
		err = c.RolesDelete(ctx, &pb.RolesDeleteRequest{
			Namespace: ns,
			Name:      roleName,
		})
		require.NoError(t, err)

		// Verify the role is deleted
		var listRes pb.RolesListResponse
		err = c.RolesList(ctx, ns, &listRes, nil)
		require.NoError(t, err)
		assert.Empty(t, listRes.Items)
	})

	t.Run("RoleBindingCreateAndList", func(t *testing.T) {
		d, c, ctx := newDaemon(t, clock.Minute*10, svc.Config{StorageConfig: setup()})
		defer d.Shutdown(t)

		ns := random.String("ns-", 10)
		err := c.NamespacesCreate(ctx, &pb.NamespaceInfo{Name: ns})
		require.NoError(t, err)

		// Create a user
		var userRes pb.UserCreateResponse
		err = c.UsersCreate(ctx, &pb.UserCreateRequest{
			Username: "binding-user-" + random.String("", 5),
		}, &userRes)
		require.NoError(t, err)

		// Create a role
		roleName := "binding-role-" + random.String("", 5)
		var roleRes pb.RoleCreateResponse
		err = c.RolesCreate(ctx, &pb.RoleCreateRequest{
			Permissions: []string{auth.QueueCreate, auth.QueueList},
			Namespace:   ns,
			Name:        roleName,
		}, &roleRes)
		require.NoError(t, err)

		// Create a role binding
		var bindingRes pb.RoleBindingCreateResponse
		err = c.RoleBindingsCreate(ctx, &pb.RoleBindingCreateRequest{
			Namespace: ns,
			RoleName:  roleName,
			UserId:    userRes.Id,
		}, &bindingRes)
		require.NoError(t, err)
		require.NotEmpty(t, bindingRes.Id)

		// List bindings and verify
		var listRes pb.RoleBindingsListResponse
		err = c.RoleBindingsList(ctx, ns, &listRes, nil)
		require.NoError(t, err)
		require.Len(t, listRes.Items, 1)
		assert.Equal(t, userRes.Id, listRes.Items[0].UserId)
		assert.Equal(t, roleRes.Id, listRes.Items[0].RoleId)
		assert.Equal(t, ns, listRes.Items[0].Namespace)
	})

	t.Run("RoleBindingDelete", func(t *testing.T) {
		d, c, ctx := newDaemon(t, clock.Minute*10, svc.Config{StorageConfig: setup()})
		defer d.Shutdown(t)

		ns := random.String("ns-", 10)
		err := c.NamespacesCreate(ctx, &pb.NamespaceInfo{Name: ns})
		require.NoError(t, err)

		// Create a user
		var userRes pb.UserCreateResponse
		err = c.UsersCreate(ctx, &pb.UserCreateRequest{
			Username: "del-binding-user-" + random.String("", 5),
		}, &userRes)
		require.NoError(t, err)

		// Create a role
		roleName := "del-binding-role-" + random.String("", 5)
		var roleRes pb.RoleCreateResponse
		err = c.RolesCreate(ctx, &pb.RoleCreateRequest{
			Permissions: []string{auth.QueueList},
			Namespace:   ns,
			Name:        roleName,
		}, &roleRes)
		require.NoError(t, err)

		// Create a role binding
		var bindingRes pb.RoleBindingCreateResponse
		err = c.RoleBindingsCreate(ctx, &pb.RoleBindingCreateRequest{
			Namespace: ns,
			RoleName:  roleName,
			UserId:    userRes.Id,
		}, &bindingRes)
		require.NoError(t, err)

		// Delete the binding
		err = c.RoleBindingsDelete(ctx, &pb.RoleBindingDeleteRequest{
			Namespace: ns,
			RoleName:  roleName,
			UserId:    userRes.Id,
		})
		require.NoError(t, err)

		// Verify the binding is deleted
		var listRes pb.RoleBindingsListResponse
		err = c.RoleBindingsList(ctx, ns, &listRes, nil)
		require.NoError(t, err)
		assert.Empty(t, listRes.Items)
	})

	t.Run("UserDeleteCascadeRoleBindings", func(t *testing.T) {
		d, c, ctx := newDaemon(t, clock.Minute*10, svc.Config{StorageConfig: setup()})
		defer d.Shutdown(t)

		ns := random.String("ns-", 10)
		err := c.NamespacesCreate(ctx, &pb.NamespaceInfo{Name: ns})
		require.NoError(t, err)

		// Create a user
		var userRes pb.UserCreateResponse
		err = c.UsersCreate(ctx, &pb.UserCreateRequest{
			Username: "cascade-binding-user-" + random.String("", 5),
		}, &userRes)
		require.NoError(t, err)

		// Create a role
		roleName := "cascade-binding-role-" + random.String("", 5)
		var roleRes pb.RoleCreateResponse
		err = c.RolesCreate(ctx, &pb.RoleCreateRequest{
			Permissions: []string{auth.QueueCreate, auth.QueueList},
			Namespace:   ns,
			Name:        roleName,
		}, &roleRes)
		require.NoError(t, err)

		// Create a role binding
		var bindingRes pb.RoleBindingCreateResponse
		err = c.RoleBindingsCreate(ctx, &pb.RoleBindingCreateRequest{
			Namespace: ns,
			RoleName:  roleName,
			UserId:    userRes.Id,
		}, &bindingRes)
		require.NoError(t, err)

		// Delete the user (should cascade delete role bindings)
		err = c.UsersDelete(ctx, &pb.UsersDeleteRequest{Id: userRes.Id})
		require.NoError(t, err)

		// Verify the role binding is deleted
		var listRes pb.RoleBindingsListResponse
		err = c.RoleBindingsList(ctx, ns, &listRes, nil)
		require.NoError(t, err)
		assert.Empty(t, listRes.Items)
	})

	t.Run("RolesListPagination", func(t *testing.T) {
		d, c, ctx := newDaemon(t, clock.Minute*10, svc.Config{StorageConfig: setup()})
		defer d.Shutdown(t)

		ns := random.String("ns-", 10)
		err := c.NamespacesCreate(ctx, &pb.NamespaceInfo{Name: ns})
		require.NoError(t, err)

		// Create 10 roles
		const numRoles = 10
		for i := 0; i < numRoles; i++ {
			var res pb.RoleCreateResponse
			err := c.RolesCreate(ctx, &pb.RoleCreateRequest{
				Namespace:   ns,
				Name:        fmt.Sprintf("page-role-%02d-%s", i, random.String("", 5)),
				Permissions: []string{auth.QueueList},
			}, &res)
			require.NoError(t, err)
		}

		// List first 3
		var page1 pb.RolesListResponse
		err = c.RolesList(ctx, ns, &page1, &querator.ListOptions{Limit: 3})
		require.NoError(t, err)
		require.Len(t, page1.Items, 3)

		// Use last item's ID as pivot — pivot item should be first in next page (inclusive)
		pivot := page1.Items[2].Id
		var page2 pb.RolesListResponse
		err = c.RolesList(ctx, ns, &page2, &querator.ListOptions{Pivot: pivot, Limit: 3})
		require.NoError(t, err)
		require.NotEmpty(t, page2.Items)
		assert.Equal(t, pivot, page2.Items[0].Id)
	})

	t.Run("RoleBindingsListPagination", func(t *testing.T) {
		d, c, ctx := newDaemon(t, clock.Minute*10, svc.Config{StorageConfig: setup()})
		defer d.Shutdown(t)

		ns := random.String("ns-", 10)
		err := c.NamespacesCreate(ctx, &pb.NamespaceInfo{Name: ns})
		require.NoError(t, err)

		// Create a role
		roleName := "pagination-binding-role-" + random.String("", 5)
		var roleRes pb.RoleCreateResponse
		err = c.RolesCreate(ctx, &pb.RoleCreateRequest{
			Namespace:   ns,
			Name:        roleName,
			Permissions: []string{auth.QueueList},
		}, &roleRes)
		require.NoError(t, err)

		// Create 10 users and bind each to the role
		const numBindings = 10
		for i := 0; i < numBindings; i++ {
			var userRes pb.UserCreateResponse
			err := c.UsersCreate(ctx, &pb.UserCreateRequest{
				Username: fmt.Sprintf("page-bind-user-%02d-%s", i, random.String("", 5)),
			}, &userRes)
			require.NoError(t, err)

			var bindRes pb.RoleBindingCreateResponse
			err = c.RoleBindingsCreate(ctx, &pb.RoleBindingCreateRequest{
				Namespace: ns,
				RoleName:  roleName,
				UserId:    userRes.Id,
			}, &bindRes)
			require.NoError(t, err)
		}

		// List first 3
		var page1 pb.RoleBindingsListResponse
		err = c.RoleBindingsList(ctx, ns, &page1, &querator.ListOptions{Limit: 3})
		require.NoError(t, err)
		require.Len(t, page1.Items, 3)

		// Use last item's ID as pivot — pivot item should be first in next page (inclusive)
		pivot := page1.Items[2].Id
		var page2 pb.RoleBindingsListResponse
		err = c.RoleBindingsList(ctx, ns, &page2, &querator.ListOptions{Pivot: pivot, Limit: 3})
		require.NoError(t, err)
		require.NotEmpty(t, page2.Items)
		assert.Equal(t, pivot, page2.Items[0].Id)
	})

	// Error Tests
	t.Run("Errors", func(t *testing.T) {
		t.Run("RolesCreate", func(t *testing.T) {
			d, c, ctx := newDaemon(t, clock.Minute*10, svc.Config{StorageConfig: setup()})
			defer d.Shutdown(t)

			ns := random.String("ns-", 10)
			err := c.NamespacesCreate(ctx, &pb.NamespaceInfo{Name: ns})
			require.NoError(t, err)

			for _, test := range []struct {
				Name    string
				Req     *pb.RoleCreateRequest
				WantErr string
			}{
				{
					Name: "EmptyNamespace",
					Req: &pb.RoleCreateRequest{
						Permissions: []string{auth.QueueList},
						Name:        "some-role",
					},
					WantErr: "namespace is invalid",
				},
				{
					Name: "EmptyName",
					Req: &pb.RoleCreateRequest{
						Permissions: []string{auth.QueueList},
						Namespace:   ns,
					},
					WantErr: "name is invalid",
				},
				{
					Name: "StandardRoleName",
					Req: &pb.RoleCreateRequest{
						Permissions: []string{auth.QueueList},
						Namespace:   ns,
						Name:        auth.RoleAdmin,
					},
					WantErr: "cannot modify or delete standard role",
				},
				{
					Name: "InvalidPermission",
					Req: &pb.RoleCreateRequest{
						Permissions: []string{"invalid.perm"},
						Namespace:   ns,
						Name:        "invalid-perm-role",
					},
					WantErr: "is invalid",
				},
				{
					Name: "NamespaceNotExist",
					Req: &pb.RoleCreateRequest{
						Permissions: []string{auth.QueueList},
						Namespace:   "non-existent-ns-" + random.String("", 5),
						Name:        "orphan-role",
					},
					WantErr: "namespace does not exist",
				},
			} {
				t.Run(test.Name, func(t *testing.T) {
					var res pb.RoleCreateResponse
					err := c.RolesCreate(ctx, test.Req, &res)
					require.Error(t, err)
					var duhErr duh.Error
					require.ErrorAs(t, err, &duhErr)
					assert.Contains(t, duhErr.Message(), test.WantErr)
				})
			}
		})

		t.Run("RolesUpdate", func(t *testing.T) {
			t.Run("StandardRole", func(t *testing.T) {
				storageConf := setup()
				d, c, ctx := newDaemon(t, clock.Minute*10, svc.Config{StorageConfig: storageConf})
				defer d.Shutdown(t)

				ns := random.String("ns-", 10)
				err := c.NamespacesCreate(ctx, &pb.NamespaceInfo{Name: ns})
				require.NoError(t, err)

				// Seed standard role directly in storage since the API rejects standard names
				err = storageConf.Roles.Add(ctx, types.Role{
					Permissions: auth.AdminPermissions,
					Namespace:   ns,
					Name:        auth.RoleAdmin,
					ID:          random.String("role-", 10),
				})
				require.NoError(t, err)

				// Attempt to update the standard role
				err = c.RolesUpdate(ctx, &pb.RoleUpdateRequest{
					Permissions: []string{auth.QueueList},
					Namespace:   ns,
					Name:        auth.RoleAdmin,
				})
				require.Error(t, err)
				var duhErr duh.Error
				require.ErrorAs(t, err, &duhErr)
				assert.Contains(t, duhErr.Message(), "cannot modify or delete standard role")
			})

			t.Run("InvalidPermission", func(t *testing.T) {
				d, c, ctx := newDaemon(t, clock.Minute*10, svc.Config{StorageConfig: setup()})
				defer d.Shutdown(t)

				ns := random.String("ns-", 10)
				err := c.NamespacesCreate(ctx, &pb.NamespaceInfo{Name: ns})
				require.NoError(t, err)

				// Create a role normally
				roleName := "update-invalid-role-" + random.String("", 5)
				var createRes pb.RoleCreateResponse
				err = c.RolesCreate(ctx, &pb.RoleCreateRequest{
					Permissions: []string{auth.QueueList},
					Namespace:   ns,
					Name:        roleName,
				}, &createRes)
				require.NoError(t, err)

				// Attempt to update with an invalid permission
				err = c.RolesUpdate(ctx, &pb.RoleUpdateRequest{
					Permissions: []string{"bogus.perm"},
					Namespace:   ns,
					Name:        roleName,
				})
				require.Error(t, err)
				var duhErr duh.Error
				require.ErrorAs(t, err, &duhErr)
				assert.Contains(t, duhErr.Message(), "is invalid")
			})
		})

		t.Run("RolesDelete", func(t *testing.T) {
			t.Run("StandardRole", func(t *testing.T) {
				storageConf := setup()
				d, c, ctx := newDaemon(t, clock.Minute*10, svc.Config{StorageConfig: storageConf})
				defer d.Shutdown(t)

				ns := random.String("ns-", 10)
				err := c.NamespacesCreate(ctx, &pb.NamespaceInfo{Name: ns})
				require.NoError(t, err)

				// Seed standard role directly in storage
				err = storageConf.Roles.Add(ctx, types.Role{
					Permissions: auth.AdminPermissions,
					Namespace:   ns,
					Name:        auth.RoleAdmin,
					ID:          random.String("role-", 10),
				})
				require.NoError(t, err)

				// Attempt to delete the standard role
				err = c.RolesDelete(ctx, &pb.RolesDeleteRequest{
					Namespace: ns,
					Name:      auth.RoleAdmin,
				})
				require.Error(t, err)
				var duhErr duh.Error
				require.ErrorAs(t, err, &duhErr)
				assert.Contains(t, duhErr.Message(), "cannot modify or delete standard role")
			})

			t.Run("RoleHasBindings", func(t *testing.T) {
				d, c, ctx := newDaemon(t, clock.Minute*10, svc.Config{StorageConfig: setup()})
				defer d.Shutdown(t)

				ns := random.String("ns-", 10)
				err := c.NamespacesCreate(ctx, &pb.NamespaceInfo{Name: ns})
				require.NoError(t, err)

				// Create a user
				var userRes pb.UserCreateResponse
				err = c.UsersCreate(ctx, &pb.UserCreateRequest{
					Username: "bound-user-" + random.String("", 5),
				}, &userRes)
				require.NoError(t, err)

				// Create a role
				roleName := "bound-role-" + random.String("", 5)
				var roleRes pb.RoleCreateResponse
				err = c.RolesCreate(ctx, &pb.RoleCreateRequest{
					Permissions: []string{auth.QueueList},
					Namespace:   ns,
					Name:        roleName,
				}, &roleRes)
				require.NoError(t, err)

				// Create a binding
				var bindingRes pb.RoleBindingCreateResponse
				err = c.RoleBindingsCreate(ctx, &pb.RoleBindingCreateRequest{
					Namespace: ns,
					RoleName:  roleName,
					UserId:    userRes.Id,
				}, &bindingRes)
				require.NoError(t, err)

				// Attempt to delete the role that has bindings
				err = c.RolesDelete(ctx, &pb.RolesDeleteRequest{
					Namespace: ns,
					Name:      roleName,
				})
				require.Error(t, err)
				var duhErr duh.Error
				require.ErrorAs(t, err, &duhErr)
				assert.Contains(t, duhErr.Message(), "role has active bindings")
			})

			t.Run("NotExist", func(t *testing.T) {
				d, c, ctx := newDaemon(t, clock.Minute*10, svc.Config{StorageConfig: setup()})
				defer d.Shutdown(t)

				ns := random.String("ns-", 10)
				err := c.NamespacesCreate(ctx, &pb.NamespaceInfo{Name: ns})
				require.NoError(t, err)

				err = c.RolesDelete(ctx, &pb.RolesDeleteRequest{
					Namespace: ns,
					Name:      "non-existent-role-" + random.String("", 5),
				})
				require.Error(t, err)
				var duhErr duh.Error
				require.ErrorAs(t, err, &duhErr)
				assert.Contains(t, duhErr.Message(), "role does not exist")
			})
		})

		t.Run("RoleBindingsCreate", func(t *testing.T) {
			d, c, ctx := newDaemon(t, clock.Minute*10, svc.Config{StorageConfig: setup()})
			defer d.Shutdown(t)

			ns := random.String("ns-", 10)
			err := c.NamespacesCreate(ctx, &pb.NamespaceInfo{Name: ns})
			require.NoError(t, err)

			// Create user and role for use in tests that need them
			var userRes pb.UserCreateResponse
			err = c.UsersCreate(ctx, &pb.UserCreateRequest{
				Username: "rb-create-user-" + random.String("", 5),
			}, &userRes)
			require.NoError(t, err)

			roleName := "rb-create-role-" + random.String("", 5)
			var roleRes pb.RoleCreateResponse
			err = c.RolesCreate(ctx, &pb.RoleCreateRequest{
				Permissions: []string{auth.QueueList},
				Namespace:   ns,
				Name:        roleName,
			}, &roleRes)
			require.NoError(t, err)

			for _, test := range []struct {
				Name    string
				Req     *pb.RoleBindingCreateRequest
				WantErr string
			}{
				{
					Name: "EmptyNamespace",
					Req: &pb.RoleBindingCreateRequest{
						RoleName: roleName,
						UserId:   userRes.Id,
					},
					WantErr: "namespace is invalid",
				},
				{
					Name: "EmptyRoleName",
					Req: &pb.RoleBindingCreateRequest{
						Namespace: ns,
						UserId:    userRes.Id,
					},
					WantErr: "role_name is invalid",
				},
				{
					Name: "EmptyUserID",
					Req: &pb.RoleBindingCreateRequest{
						Namespace: ns,
						RoleName:  roleName,
					},
					WantErr: "user_id is invalid",
				},
				{
					Name: "RoleNotExist",
					Req: &pb.RoleBindingCreateRequest{
						Namespace: ns,
						RoleName:  "non-existent-role-" + random.String("", 5),
						UserId:    userRes.Id,
					},
					WantErr: "role does not exist",
				},
				{
					Name: "UserNotExist",
					Req: &pb.RoleBindingCreateRequest{
						Namespace: ns,
						RoleName:  roleName,
						UserId:    "non-existent-user-" + random.String("", 5),
					},
					WantErr: "user does not exist",
				},
			} {
				t.Run(test.Name, func(t *testing.T) {
					var res pb.RoleBindingCreateResponse
					err := c.RoleBindingsCreate(ctx, test.Req, &res)
					require.Error(t, err)
					var duhErr duh.Error
					require.ErrorAs(t, err, &duhErr)
					assert.Contains(t, duhErr.Message(), test.WantErr)
				})
			}
		})

		t.Run("RoleBindingsDelete", func(t *testing.T) {
			t.Run("NotExist", func(t *testing.T) {
				d, c, ctx := newDaemon(t, clock.Minute*10, svc.Config{StorageConfig: setup()})
				defer d.Shutdown(t)

				ns := random.String("ns-", 10)
				err := c.NamespacesCreate(ctx, &pb.NamespaceInfo{Name: ns})
				require.NoError(t, err)

				// Create a role (but no binding)
				roleName := "no-binding-role-" + random.String("", 5)
				var roleRes pb.RoleCreateResponse
				err = c.RolesCreate(ctx, &pb.RoleCreateRequest{
					Permissions: []string{auth.QueueList},
					Namespace:   ns,
					Name:        roleName,
				}, &roleRes)
				require.NoError(t, err)

				// Attempt to delete a non-existent binding
				err = c.RoleBindingsDelete(ctx, &pb.RoleBindingDeleteRequest{
					Namespace: ns,
					RoleName:  roleName,
					UserId:    "non-existent-user-" + random.String("", 5),
				})
				require.Error(t, err)
				var duhErr duh.Error
				require.ErrorAs(t, err, &duhErr)
				assert.Contains(t, duhErr.Message(), "role binding does not exist")
			})
		})
	})
}
