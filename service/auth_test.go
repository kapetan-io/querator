package service_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/duh-rpc/duh-go"
	v1 "github.com/duh-rpc/duh-go/proto/v1"
	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/daemon"
	"github.com/kapetan-io/querator/internal"
	"github.com/kapetan-io/querator/internal/store"
	pb "github.com/kapetan-io/querator/proto"
	svc "github.com/kapetan-io/querator/service"
	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/querator/transport/auth"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestAuth(t *testing.T) {
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
				t.Cleanup(func() { badger.Teardown() })
				return badger.Setup(store.BadgerConfig{})
			},
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			testAuth(t, tc.Setup)
		})
	}
}

func testAuth(t *testing.T, setup NewStorageFunc) {
	defer goleak.VerifyNone(t, goleakOptions...)
	t.Run("DefaultOpen", func(t *testing.T) {
		t.Run("AllowsRequestsWithoutAPIKey", func(t *testing.T) {
			d, c, ctx := newDaemon(t, clock.Minute*10, svc.Config{StorageConfig: setup()})
			defer d.Shutdown(t)

			// Without any auth backend configured (default NoOp), requests should succeed
			var listRes pb.QueuesListResponse
			err := c.QueuesList(ctx, &listRes, nil)
			require.NoError(t, err)
		})

		t.Run("AllowsQueueOperationsWithoutAPIKey", func(t *testing.T) {
			d, c, ctx := newDaemon(t, clock.Minute*10, svc.Config{StorageConfig: setup()})
			defer d.Shutdown(t)

			queueName := random.String("queue-", 10)

			// Create queue
			err := c.QueuesCreate(ctx, &pb.QueueInfo{
				QueueName:           queueName,
				LeaseTimeout:        LeaseTimeout,
				ExpireTimeout:       ExpireTimeout,
				RequestedPartitions: 1,
			})
			require.NoError(t, err)

			// Produce
			err = c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "1m",
				Items: []*pb.QueueProduceItem{
					{
						Reference: "test-ref",
						Bytes:     []byte("test payload"),
					},
				},
			})
			require.NoError(t, err)

			// Lease
			var leaseRes pb.QueueLeaseResponse
			err = c.QueueLease(ctx, &pb.QueueLeaseRequest{
				QueueName:      queueName,
				ClientId:       random.String("client-", 10),
				RequestTimeout: "5s",
				BatchSize:      1,
			}, &leaseRes)
			require.NoError(t, err)
			require.Len(t, leaseRes.Items, 1)
		})
	})

	t.Run("Authentication", func(t *testing.T) {
		t.Run("ValidAPIKey", func(t *testing.T) {
			d, apiKey := newDaemonWithAuth(t, setup)
			defer d.Shutdown(t)

			// Create client with valid API key
			c := newClientWithAPIKey(t, d, apiKey)
			ctx := d.Context()

			// Request should succeed
			var listRes pb.QueuesListResponse
			err := c.QueuesList(ctx, &listRes, nil)
			require.NoError(t, err)
		})

		t.Run("InvalidAPIKey", func(t *testing.T) {
			d, _ := newDaemonWithAuth(t, setup)
			defer d.Shutdown(t)

			// Create client with invalid API key
			c := newClientWithAPIKey(t, d, "bad_key_format")
			ctx := d.Context()

			// Request should fail with 401
			var listRes pb.QueuesListResponse
			err := c.QueuesList(ctx, &listRes, nil)
			require.Error(t, err)
			var duhErr duh.Error
			require.ErrorAs(t, err, &duhErr)
			assert.Equal(t, duh.CodeUnauthorized, duhErr.Code())
		})

		t.Run("AnonymousAccessInOpenDoor", func(t *testing.T) {
			d, _ := newDaemonWithAuth(t, setup)
			defer d.Shutdown(t)

			// Create client without API key (anonymous)
			c := newClientWithAPIKey(t, d, "")
			ctx := d.Context()

			// In Open Door mode, anonymous has Admin privileges via bootstrap binding
			var listRes pb.QueuesListResponse
			err := c.QueuesList(ctx, &listRes, nil)
			require.NoError(t, err)
		})

		t.Run("ExpiredAPIKey", func(t *testing.T) {
			d, adminKey := newDaemonWithAuth(t, setup)
			defer d.Shutdown(t)

			ctx := d.Context()
			adminClient := newClientWithAPIKey(t, d, adminKey)

			// Create a user via the API
			var userRes pb.UserCreateResponse
			err := adminClient.UsersCreate(ctx, &pb.UserCreateRequest{
				Username: "expired-" + random.String("", 5),
			}, &userRes)
			require.NoError(t, err)

			// Bind the user to Admin role so the key would have permissions (if not expired)
			var bindingRes pb.RoleBindingCreateResponse
			err = adminClient.RoleBindingsCreate(ctx, &pb.RoleBindingCreateRequest{
				Namespace: auth.SystemNamespace,
				UserId:    userRes.Id,
				RoleName:  auth.RoleAdmin,
			}, &bindingRes)
			require.NoError(t, err)

			// Create an API key with an expiration in the past
			var keyRes pb.APIKeyCreateResponse
			err = adminClient.APIKeysCreate(ctx, &pb.APIKeyCreateRequest{
				UserId:    userRes.Id,
				Name:      "expired-key",
				ExpiresAt: timestamppb.New(clock.Now().UTC().Add(-clock.Hour)),
			}, &keyRes)
			require.NoError(t, err)

			// Authenticate with the expired key
			c := newClientWithAPIKey(t, d, keyRes.Key)
			var listRes pb.QueuesListResponse
			err = c.QueuesList(ctx, &listRes, nil)
			require.Error(t, err)
			var duhErr duh.Error
			require.ErrorAs(t, err, &duhErr)
			assert.Equal(t, duh.CodeUnauthorized, duhErr.Code())
			assert.Contains(t, duhErr.Message(), "api key has expired")
		})
	})

	t.Run("Authorization", func(t *testing.T) {
		t.Run("HasPermission", func(t *testing.T) {
			d, apiKey := newDaemonWithAuth(t, setup)
			defer d.Shutdown(t)

			c := newClientWithAPIKey(t, d, apiKey)
			ctx := d.Context()

			queueName := random.String("queue-", 10)

			// Create queue should succeed (admin has all permissions)
			err := c.QueuesCreate(ctx, &pb.QueueInfo{
				QueueName:           queueName,
				LeaseTimeout:        LeaseTimeout,
				ExpireTimeout:       ExpireTimeout,
				RequestedPartitions: 1,
			})
			require.NoError(t, err)

			// Produce should succeed
			err = c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "1m",
				Items: []*pb.QueueProduceItem{
					{
						Reference: "test-ref",
						Bytes:     []byte("test payload"),
					},
				},
			})
			require.NoError(t, err)
		})

		t.Run("LacksPermission", func(t *testing.T) {
			d, adminKey := newDaemonWithAuth(t, setup)
			defer d.Shutdown(t)

			ctx := d.Context()
			adminClient := newClientWithAPIKey(t, d, adminKey)

			// Create a user with limited permissions (only queue.list, no queue.create)
			limitedKey := CreateUserWithLimitedPermissions(t, ctx, adminClient, []string{auth.QueueList})

			// Create client with limited API key
			limitedClient := newClientWithAPIKey(t, d, limitedKey)

			// List queues should succeed
			var listRes pb.QueuesListResponse
			err := limitedClient.QueuesList(ctx, &listRes, nil)
			require.NoError(t, err)

			// Create queue should fail with 403
			err = limitedClient.QueuesCreate(ctx, &pb.QueueInfo{
				QueueName:           random.String("queue-", 10),
				LeaseTimeout:        LeaseTimeout,
				ExpireTimeout:       ExpireTimeout,
				RequestedPartitions: 1,
			})
			require.Error(t, err)
			var duhErr duh.Error
			require.ErrorAs(t, err, &duhErr)
			assert.Equal(t, duh.CodeForbidden, duhErr.Code())
		})

		t.Run("NamespaceScoping", func(t *testing.T) {
			d, adminKey := newDaemonWithAuth(t, setup)
			defer d.Shutdown(t)

			ctx := d.Context()
			adminClient := newClientWithAPIKey(t, d, adminKey)

			// Create two namespaces
			ns1 := random.String("ns1-", 10)
			ns2 := random.String("ns2-", 10)

			err := adminClient.NamespacesCreate(ctx, &pb.NamespaceInfo{Name: ns1})
			require.NoError(t, err)

			err = adminClient.NamespacesCreate(ctx, &pb.NamespaceInfo{Name: ns2})
			require.NoError(t, err)

			// Create a user with permissions only in ns1
			ns1Key := CreateUserWithNamespacePermissions(t, ctx, adminClient, ns1, auth.NamespaceOwnerPermissions())

			// Create client with ns1-scoped API key
			ns1Client := newClientWithAPIKey(t, d, ns1Key)

			// Create queue in ns1 should succeed
			queue1 := random.String("queue-", 10)
			err = ns1Client.QueuesCreate(ctx, &pb.QueueInfo{
				Namespace:           ns1,
				QueueName:           queue1,
				LeaseTimeout:        LeaseTimeout,
				ExpireTimeout:       ExpireTimeout,
				RequestedPartitions: 1,
			})
			require.NoError(t, err)

			// Create queue in ns2 should fail with 403
			queue2 := random.String("queue-", 10)
			err = ns1Client.QueuesCreate(ctx, &pb.QueueInfo{
				Namespace:           ns2,
				QueueName:           queue2,
				LeaseTimeout:        LeaseTimeout,
				ExpireTimeout:       ExpireTimeout,
				RequestedPartitions: 1,
			})
			require.Error(t, err)
			var duhErr duh.Error
			require.ErrorAs(t, err, &duhErr)
			assert.Equal(t, duh.CodeForbidden, duhErr.Code())
		})

		t.Run("AdminCanUpdateQueue", func(t *testing.T) {
			d, adminKey := newDaemonWithAuth(t, setup)
			defer d.Shutdown(t)

			ctx := d.Context()
			c := newClientWithAPIKey(t, d, adminKey)

			queueName := random.String("queue-", 10)
			err := c.QueuesCreate(ctx, &pb.QueueInfo{
				QueueName:           queueName,
				LeaseTimeout:        LeaseTimeout,
				ExpireTimeout:       ExpireTimeout,
				RequestedPartitions: 1,
			})
			require.NoError(t, err)

			// Before fix: service layer sees AnonymousPrincipal → denies admin
			// After fix: service layer reads real principal → allows admin
			err = c.QueuesUpdate(ctx, &pb.QueueInfo{
				QueueName:    queueName,
				LeaseTimeout: "2m0s",
			})
			require.NoError(t, err)
		})

		t.Run("AdminCanDeleteQueue", func(t *testing.T) {
			d, adminKey := newDaemonWithAuth(t, setup)
			defer d.Shutdown(t)

			ctx := d.Context()
			c := newClientWithAPIKey(t, d, adminKey)

			queueName := random.String("queue-", 10)
			err := c.QueuesCreate(ctx, &pb.QueueInfo{
				QueueName:           queueName,
				LeaseTimeout:        LeaseTimeout,
				ExpireTimeout:       ExpireTimeout,
				RequestedPartitions: 1,
			})
			require.NoError(t, err)

			// Before fix: service layer sees AnonymousPrincipal → denies admin
			// After fix: service layer reads real principal → allows admin
			err = c.QueuesDelete(ctx, &pb.QueuesDeleteRequest{
				QueueName: queueName,
			})
			require.NoError(t, err)
		})

		t.Run("QueuesUpdateDenied", func(t *testing.T) {
			d, adminKey := newDaemonWithAuth(t, setup)
			defer d.Shutdown(t)

			ctx := d.Context()
			adminClient := newClientWithAPIKey(t, d, adminKey)

			queueName := random.String("queue-", 10)
			err := adminClient.QueuesCreate(ctx, &pb.QueueInfo{
				QueueName:           queueName,
				LeaseTimeout:        LeaseTimeout,
				ExpireTimeout:       ExpireTimeout,
				RequestedPartitions: 1,
			})
			require.NoError(t, err)

			// Create user with only queue.list — no queue.update
			limitedKey := CreateUserWithLimitedPermissions(t, ctx, adminClient, []string{auth.QueueList})
			limitedClient := newClientWithAPIKey(t, d, limitedKey)

			err = limitedClient.QueuesUpdate(ctx, &pb.QueueInfo{
				QueueName:    queueName,
				LeaseTimeout: "2m0s",
			})
			require.Error(t, err)
			var duhErr duh.Error
			require.ErrorAs(t, err, &duhErr)
			assert.Equal(t, duh.CodeForbidden, duhErr.Code())
		})

		t.Run("QueuesDeleteDenied", func(t *testing.T) {
			d, adminKey := newDaemonWithAuth(t, setup)
			defer d.Shutdown(t)

			ctx := d.Context()
			adminClient := newClientWithAPIKey(t, d, adminKey)

			queueName := random.String("queue-", 10)
			err := adminClient.QueuesCreate(ctx, &pb.QueueInfo{
				QueueName:           queueName,
				LeaseTimeout:        LeaseTimeout,
				ExpireTimeout:       ExpireTimeout,
				RequestedPartitions: 1,
			})
			require.NoError(t, err)

			// Create user with only queue.list — no queue.delete
			limitedKey := CreateUserWithLimitedPermissions(t, ctx, adminClient, []string{auth.QueueList})
			limitedClient := newClientWithAPIKey(t, d, limitedKey)

			err = limitedClient.QueuesDelete(ctx, &pb.QueuesDeleteRequest{
				QueueName: queueName,
			})
			require.Error(t, err)
			var duhErr duh.Error
			require.ErrorAs(t, err, &duhErr)
			assert.Equal(t, duh.CodeForbidden, duhErr.Code())
		})

		t.Run("QueuesListNamespaceScoping", func(t *testing.T) {
			d, adminKey := newDaemonWithAuth(t, setup)
			defer d.Shutdown(t)

			ctx := d.Context()
			adminClient := newClientWithAPIKey(t, d, adminKey)

			// Create two tenant namespaces
			tenantA := random.String("tenant-a-", 10)
			tenantB := random.String("tenant-b-", 10)
			require.NoError(t, adminClient.NamespacesCreate(ctx, &pb.NamespaceInfo{Name: tenantA}))
			require.NoError(t, adminClient.NamespacesCreate(ctx, &pb.NamespaceInfo{Name: tenantB}))

			// Create queues in each namespace using the admin client
			require.NoError(t, adminClient.QueuesCreate(ctx, &pb.QueueInfo{
				Namespace:           tenantA,
				QueueName:           random.String("q-a-", 10),
				LeaseTimeout:        LeaseTimeout,
				ExpireTimeout:       ExpireTimeout,
				RequestedPartitions: 1,
			}))
			require.NoError(t, adminClient.QueuesCreate(ctx, &pb.QueueInfo{
				Namespace:           tenantB,
				QueueName:           random.String("q-b-", 10),
				LeaseTimeout:        LeaseTimeout,
				ExpireTimeout:       ExpireTimeout,
				RequestedPartitions: 1,
			}))

			// Create a tenant-a scoped API key with QueueList permission in tenant-a only
			tenantAKey := CreateUserWithNamespacePermissions(t, ctx, adminClient, tenantA, auth.NamespaceOwnerPermissions())
			tenantAClient := newClientWithAPIKey(t, d, tenantAKey)

			t.Run("SystemAdminNoFilterSeesAllNamespaces", func(t *testing.T) {
				// System admin with no namespace filter can list across all namespaces
				var res pb.QueuesListResponse
				err := adminClient.QueuesList(ctx, &res, nil)
				require.NoError(t, err)
				assert.GreaterOrEqual(t, len(res.Items), 2)
			})

			t.Run("SystemAdminWithNamespaceFilterSeesOnlyThatNamespace", func(t *testing.T) {
				// System admin with namespace filter only sees queues in that namespace
				var res pb.QueuesListResponse
				err := adminClient.QueuesList(ctx, &res, &querator.ListOptions{Namespace: tenantA})
				require.NoError(t, err)
				require.NotEmpty(t, res.Items)
				for _, item := range res.Items {
					assert.Equal(t, tenantA, item.Namespace)
				}
			})

			t.Run("TenantPrincipalCanListOwnNamespace", func(t *testing.T) {
				// Tenant-a principal with QueueList in tenant-a can list queues in tenant-a
				var res pb.QueuesListResponse
				err := tenantAClient.QueuesList(ctx, &res, &querator.ListOptions{Namespace: tenantA})
				require.NoError(t, err)
				require.NotEmpty(t, res.Items)
				for _, item := range res.Items {
					assert.Equal(t, tenantA, item.Namespace)
				}
			})

			t.Run("TenantPrincipalDeniedWithNoNamespaceFilter", func(t *testing.T) {
				// Tenant-a principal without a namespace filter requires _system access — denied
				var res pb.QueuesListResponse
				err := tenantAClient.QueuesList(ctx, &res, nil)
				require.Error(t, err)
				var duhErr duh.Error
				require.ErrorAs(t, err, &duhErr)
				assert.Equal(t, duh.CodeForbidden, duhErr.Code())
			})

			t.Run("TenantPrincipalDeniedForOtherNamespace", func(t *testing.T) {
				// Tenant-a principal is denied when listing queues in tenant-b
				var res pb.QueuesListResponse
				err := tenantAClient.QueuesList(ctx, &res, &querator.ListOptions{Namespace: tenantB})
				require.Error(t, err)
				var duhErr duh.Error
				require.ErrorAs(t, err, &duhErr)
				assert.Equal(t, duh.CodeForbidden, duhErr.Code())
			})
		})

		t.Run("SystemNamespaceCascade", func(t *testing.T) {
			d, adminKey := newDaemonWithAuth(t, setup)
			defer d.Shutdown(t)

			ctx := d.Context()
			adminClient := newClientWithAPIKey(t, d, adminKey)

			// Admin user (with _system role binding) should be able to operate on any namespace
			ns := random.String("ns-", 10)
			err := adminClient.NamespacesCreate(ctx, &pb.NamespaceInfo{Name: ns})
			require.NoError(t, err)

			// Create queue in the new namespace using admin key
			queueName := random.String("queue-", 10)
			err = adminClient.QueuesCreate(ctx, &pb.QueueInfo{
				Namespace:           ns,
				QueueName:           queueName,
				LeaseTimeout:        LeaseTimeout,
				ExpireTimeout:       ExpireTimeout,
				RequestedPartitions: 1,
			})
			require.NoError(t, err)

			// Produce to the queue in the new namespace
			err = adminClient.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "1m",
				Items: []*pb.QueueProduceItem{
					{
						Reference: "test-ref",
						Bytes:     []byte("test payload"),
					},
				},
			})
			require.NoError(t, err)
		})

		// Issue 1: A namespace-scoped API key must still be able to exercise permissions
		// granted via _system role bindings when the target namespace is _system directly.
		t.Run("ScopedKeyWithSystemBindingCanAccessSystem", func(t *testing.T) {
			d, adminKey := newDaemonWithAuth(t, setup)
			defer d.Shutdown(t)

			ctx := d.Context()
			adminClient := newClientWithAPIKey(t, d, adminKey)

			// Create a namespace to scope the key to
			ns := random.String("ns-", 10)
			require.NoError(t, adminClient.NamespacesCreate(ctx, &pb.NamespaceInfo{Name: ns}))

			// Create a user and bind them to the Admin role in _system
			var userRes pb.UserCreateResponse
			require.NoError(t, adminClient.UsersCreate(ctx, &pb.UserCreateRequest{
				Username: "scoped-system-user-" + random.String("", 5),
			}, &userRes))
			var bindingRes pb.RoleBindingCreateResponse
			require.NoError(t, adminClient.RoleBindingsCreate(ctx, &pb.RoleBindingCreateRequest{
				Namespace: auth.SystemNamespace,
				UserId:    userRes.Id,
				RoleName:  auth.RoleAdmin,
			}, &bindingRes))

			// Create an API key scoped to ns (not _system)
			var keyRes pb.APIKeyCreateResponse
			require.NoError(t, adminClient.APIKeysCreate(ctx, &pb.APIKeyCreateRequest{
				UserId:         userRes.Id,
				Name:           "scoped-key",
				NamespaceScope: ns,
			}, &keyRes))

			// The scoped key still holds an _system Admin binding. UsersList targets _system.
			// Before the fix, the namespace scope guard fires ("ns" != "_system") and returns
			// 403 even though the user has an Admin binding in _system.
			scopedClient := newClientWithAPIKey(t, d, keyRes.Key)
			var listRes pb.UsersListResponse
			err := scopedClient.UsersList(ctx, &listRes, nil)
			require.NoError(t, err)
		})

		// Issue 3 regression: an unauthorized user must receive the same error code
		// for both existing and nonexistent namespaces in RolesCreate (no namespace oracle).
		t.Run("RolesCreateNoNamespaceOracle", func(t *testing.T) {
			d, adminKey := newDaemonWithAuth(t, setup)
			defer d.Shutdown(t)

			ctx := d.Context()
			adminClient := newClientWithAPIKey(t, d, adminKey)

			// Create a real namespace
			existingNS := random.String("ns-", 10)
			require.NoError(t, adminClient.NamespacesCreate(ctx, &pb.NamespaceInfo{Name: existingNS}))

			// Create a user with only queue.list — no role.create permission
			limitedKey := CreateUserWithLimitedPermissions(t, ctx, adminClient, []string{auth.QueueList})
			limitedClient := newClientWithAPIKey(t, d, limitedKey)

			// Attempt in an existing namespace — must be 403 (unauthorized, not 404)
			var roleRes pb.RoleCreateResponse
			err := limitedClient.RolesCreate(ctx, &pb.RoleCreateRequest{
				Namespace:   existingNS,
				Name:        "some-role-" + random.String("", 5),
				Permissions: []string{auth.QueueList},
			}, &roleRes)
			require.Error(t, err)
			var duhErrExisting duh.Error
			require.ErrorAs(t, err, &duhErrExisting)
			assert.Equal(t, duh.CodeForbidden, duhErrExisting.Code())

			// Attempt in a nonexistent namespace — must also be 403 (no namespace oracle)
			err = limitedClient.RolesCreate(ctx, &pb.RoleCreateRequest{
				Namespace:   "nonexistent-ns-" + random.String("", 5),
				Name:        "some-role-" + random.String("", 5),
				Permissions: []string{auth.QueueList},
			}, &roleRes)
			require.Error(t, err)
			var duhErrMissing duh.Error
			require.ErrorAs(t, err, &duhErrMissing)
			assert.Equal(t, duh.CodeForbidden, duhErrMissing.Code())
		})
	})

	t.Run("CacheInvalidation", func(t *testing.T) {
		t.Run("DeletedAPIKeyDenied", func(t *testing.T) {
			d, adminKey := newDaemonWithAuth(t, setup)
			defer d.Shutdown(t)

			ctx := d.Context()
			adminClient := newClientWithAPIKey(t, d, adminKey)

			// Create a user with permissions
			userKey := CreateUserWithLimitedPermissions(t, ctx, adminClient, []string{auth.QueueList})
			userClient := newClientWithAPIKey(t, d, userKey)

			// Authenticate successfully with the key
			var listRes pb.QueuesListResponse
			err := userClient.QueuesList(ctx, &listRes, nil)
			require.NoError(t, err)

			// Find the API key ID to delete
			var keysRes pb.APIKeysListResponse
			err = adminClient.APIKeysList(ctx, &keysRes, nil)
			require.NoError(t, err)

			// Find the non-admin key (the one named "limited-key")
			var keyID string
			for _, k := range keysRes.Items {
				if k.Name == "limited-key" {
					keyID = k.Id
					break
				}
			}
			require.NotEmpty(t, keyID)

			// Delete the API key
			err = adminClient.APIKeysDelete(ctx, &pb.APIKeysDeleteRequest{Id: keyID})
			require.NoError(t, err)

			// Attempt to authenticate with the deleted key — should fail immediately
			err = userClient.QueuesList(ctx, &listRes, nil)
			require.Error(t, err)
			var duhErr duh.Error
			require.ErrorAs(t, err, &duhErr)
			assert.Equal(t, duh.CodeUnauthorized, duhErr.Code())
		})

		t.Run("DeletedUserDenied", func(t *testing.T) {
			d, adminKey := newDaemonWithAuth(t, setup)
			defer d.Shutdown(t)

			ctx := d.Context()
			adminClient := newClientWithAPIKey(t, d, adminKey)

			// Create a user with permissions
			var userRes pb.UserCreateResponse
			err := adminClient.UsersCreate(ctx, &pb.UserCreateRequest{
				Username: "delete-cache-user-" + random.String("", 5),
			}, &userRes)
			require.NoError(t, err)

			// Create role and binding for the user
			var roleRes pb.RoleCreateResponse
			err = adminClient.RolesCreate(ctx, &pb.RoleCreateRequest{
				Namespace:   auth.SystemNamespace,
				Name:        "delete-cache-role-" + random.String("", 5),
				Permissions: []string{auth.QueueList},
			}, &roleRes)
			require.NoError(t, err)

			// Find the role name
			var rolesListRes pb.RolesListResponse
			err = adminClient.RolesList(ctx, auth.SystemNamespace, &rolesListRes, nil)
			require.NoError(t, err)
			var roleName string
			for _, role := range rolesListRes.Items {
				if role.Id == roleRes.Id {
					roleName = role.Name
					break
				}
			}
			require.NotEmpty(t, roleName)

			var bindingRes pb.RoleBindingCreateResponse
			err = adminClient.RoleBindingsCreate(ctx, &pb.RoleBindingCreateRequest{
				Namespace: auth.SystemNamespace,
				UserId:    userRes.Id,
				RoleName:  roleName,
			}, &bindingRes)
			require.NoError(t, err)

			// Create API key for the user
			var keyRes pb.APIKeyCreateResponse
			err = adminClient.APIKeysCreate(ctx, &pb.APIKeyCreateRequest{
				UserId: userRes.Id,
				Name:   "delete-cache-key",
			}, &keyRes)
			require.NoError(t, err)

			userClient := newClientWithAPIKey(t, d, keyRes.Key)

			// Authenticate successfully
			var listRes pb.QueuesListResponse
			err = userClient.QueuesList(ctx, &listRes, nil)
			require.NoError(t, err)

			// Delete the user (cascade deletes keys and bindings)
			err = adminClient.UsersDelete(ctx, &pb.UsersDeleteRequest{Id: userRes.Id})
			require.NoError(t, err)

			// Attempt to authenticate with the deleted user's key — should fail immediately
			err = userClient.QueuesList(ctx, &listRes, nil)
			require.Error(t, err)
			var duhErr duh.Error
			require.ErrorAs(t, err, &duhErr)
			assert.Equal(t, duh.CodeUnauthorized, duhErr.Code())
		})

		// Issue 2: Once an API key is cached, its own ExpiresAt must still be enforced.
		// A key that expires within the cache TTL window must be rejected after expiry even
		// when the cache entry itself has not yet been evicted.
		t.Run("CachedExpiredKeyRejected", func(t *testing.T) {
			d, adminKey := newDaemonWithAuth(t, setup)
			defer d.Shutdown(t)

			ctx := d.Context()
			adminClient := newClientWithAPIKey(t, d, adminKey)

			// Create a user with Admin permissions
			var userRes pb.UserCreateResponse
			require.NoError(t, adminClient.UsersCreate(ctx, &pb.UserCreateRequest{
				Username: "cache-expire-user-" + random.String("", 5),
			}, &userRes))
			var bindingRes pb.RoleBindingCreateResponse
			require.NoError(t, adminClient.RoleBindingsCreate(ctx, &pb.RoleBindingCreateRequest{
				Namespace: auth.SystemNamespace,
				UserId:    userRes.Id,
				RoleName:  auth.RoleAdmin,
			}, &bindingRes))

			// Create an API key that expires in 200ms
			expiresAt := clock.Now().UTC().Add(200 * time.Millisecond)
			var keyRes pb.APIKeyCreateResponse
			require.NoError(t, adminClient.APIKeysCreate(ctx, &pb.APIKeyCreateRequest{
				UserId:    userRes.Id,
				Name:      "short-lived-key",
				ExpiresAt: timestamppb.New(expiresAt),
			}, &keyRes))

			shortLivedClient := newClientWithAPIKey(t, d, keyRes.Key)

			// First request: key is not yet expired — must succeed and also populate the cache
			var listRes pb.QueuesListResponse
			require.NoError(t, shortLivedClient.QueuesList(ctx, &listRes, nil))

			// Wait for the key's own ExpiresAt to pass while the cache entry is still live
			time.Sleep(300 * time.Millisecond)

			// Second request: key is now expired but the cache TTL has not elapsed.
			// Before the fix, the cache hit skips the ExpiresAt check and returns success.
			// After the fix, the cached entry evicts itself and returns 401.
			err := shortLivedClient.QueuesList(ctx, &listRes, nil)
			require.Error(t, err)
			var duhErr duh.Error
			require.ErrorAs(t, err, &duhErr)
			assert.Equal(t, duh.CodeUnauthorized, duhErr.Code())
			assert.Contains(t, duhErr.Message(), "api key has expired")
		})
	})

	t.Run("UserDeleteCascadeOrder", func(t *testing.T) {
		d, adminKey := newDaemonWithAuth(t, setup)
		defer d.Shutdown(t)

		ctx := d.Context()
		adminClient := newClientWithAPIKey(t, d, adminKey)

		// Create a user with a role binding and API key
		var userRes pb.UserCreateResponse
		err := adminClient.UsersCreate(ctx, &pb.UserCreateRequest{
			Username: "cascade-order-user-" + random.String("", 5),
		}, &userRes)
		require.NoError(t, err)

		var bindingRes pb.RoleBindingCreateResponse
		err = adminClient.RoleBindingsCreate(ctx, &pb.RoleBindingCreateRequest{
			Namespace: auth.SystemNamespace,
			UserId:    userRes.Id,
			RoleName:  auth.RoleAdmin,
		}, &bindingRes)
		require.NoError(t, err)

		var keyRes pb.APIKeyCreateResponse
		err = adminClient.APIKeysCreate(ctx, &pb.APIKeyCreateRequest{
			UserId: userRes.Id,
			Name:   "cascade-order-key",
		}, &keyRes)
		require.NoError(t, err)

		userClient := newClientWithAPIKey(t, d, keyRes.Key)

		// Verify the key works before deletion
		var listRes pb.QueuesListResponse
		err = userClient.QueuesList(ctx, &listRes, nil)
		require.NoError(t, err)

		// Delete the user
		err = adminClient.UsersDelete(ctx, &pb.UsersDeleteRequest{Id: userRes.Id})
		require.NoError(t, err)

		// After deletion, the old key must return a clean 401 — not a 500 or panic
		err = userClient.QueuesList(ctx, &listRes, nil)
		require.Error(t, err)
		var duhErr duh.Error
		require.ErrorAs(t, err, &duhErr)
		assert.Equal(t, duh.CodeUnauthorized, duhErr.Code())
	})

	t.Run("DoubleClose", func(t *testing.T) {
		d, _ := newDaemonWithAuth(t, setup)

		// First close via Shutdown (calls authBackend.Close())
		d.authBackend.Close()
		// Second close should not panic
		d.authBackend.Close()

		d.testDaemon.Shutdown(t)
	})

	t.Run("ConcurrentAuthentication", func(t *testing.T) {
		d, adminKey := newDaemonWithAuth(t, setup)
		defer d.Shutdown(t)

		ctx := d.Context()

		// Create multiple users with API keys to trigger concurrent cache misses
		const numUsers = 10
		keys := make([]string, numUsers)
		for i := 0; i < numUsers; i++ {
			keys[i] = CreateUserWithLimitedPermissions(t, ctx,
				newClientWithAPIKey(t, d, adminKey), []string{auth.QueueList})
		}

		// Authenticate all keys concurrently, triggering concurrent UpdateLastUsed goroutines
		var wg sync.WaitGroup
		wg.Add(numUsers)
		for i := 0; i < numUsers; i++ {
			go func(key string) {
				defer wg.Done()
				c := newClientWithAPIKey(t, d, key)
				var listRes pb.QueuesListResponse
				_ = c.QueuesList(ctx, &listRes, nil)
			}(keys[i])
		}
		wg.Wait()

		// Shutdown will call Close(), which waits for all UpdateLastUsed goroutines.
		// goleak.VerifyNone (deferred in testAuth) verifies no goroutines leak.
	})

	t.Run("Errors", func(t *testing.T) {
		t.Run("MalformedAuthHeader", func(t *testing.T) {
			d, _ := newDaemonWithAuth(t, setup)
			defer d.Shutdown(t)

			ctx := d.Context()
			endpoint := fmt.Sprintf("http://%s%s", d.d.Listener.Addr().String(), transport.RPCQueuesList)

			// Send a raw HTTP request with a non-Bearer authorization header
			payload, err := proto.Marshal(&pb.QueuesListRequest{})
			require.NoError(t, err)

			r, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
			require.NoError(t, err)

			r.Header.Set("Content-Type", duh.ContentTypeProtoBuf)
			r.Header.Set("Authorization", "Basic c29tZS1jcmVkZW50aWFscw==")

			var res v1.Reply
			err = duh.HTTP1Client.Do(r, &res)
			require.Error(t, err)
			var duhErr duh.Error
			require.ErrorAs(t, err, &duhErr)
			assert.Equal(t, duh.CodeUnauthorized, duhErr.Code())
			assert.Contains(t, duhErr.Message(), "invalid authorization header format")
		})
	})

	t.Run("Bootstrap", func(t *testing.T) {
		t.Run("SystemNamespaceExists", func(t *testing.T) {
			d, adminKey := newDaemonWithAuth(t, setup)
			defer d.Shutdown(t)

			c := newClientWithAPIKey(t, d, adminKey)
			ctx := d.Context()

			var listRes pb.NamespacesListResponse
			err := c.NamespacesList(ctx, &listRes, nil)
			require.NoError(t, err)

			var found bool
			for _, ns := range listRes.Items {
				if ns.Name == auth.SystemNamespace {
					found = true
					break
				}
			}
			assert.True(t, found)
		})

		t.Run("AnonymousUserExists", func(t *testing.T) {
			d, adminKey := newDaemonWithAuth(t, setup)
			defer d.Shutdown(t)

			c := newClientWithAPIKey(t, d, adminKey)
			ctx := d.Context()

			var listRes pb.UsersListResponse
			err := c.UsersList(ctx, &listRes, nil)
			require.NoError(t, err)

			var found bool
			for _, u := range listRes.Items {
				if u.Username == "anonymous" && u.Id == "anonymous" {
					found = true
					break
				}
			}
			assert.True(t, found)
		})

		t.Run("StandardRolesExist", func(t *testing.T) {
			d, adminKey := newDaemonWithAuth(t, setup)
			defer d.Shutdown(t)

			c := newClientWithAPIKey(t, d, adminKey)
			ctx := d.Context()

			var listRes pb.RolesListResponse
			err := c.RolesList(ctx, auth.SystemNamespace, &listRes, nil)
			require.NoError(t, err)

			roles := make(map[string][]string)
			for _, role := range listRes.Items {
				roles[role.Name] = role.Permissions
			}

			// Verify Admin role exists with correct permissions
			adminPerms, ok := roles[auth.RoleAdmin]
			require.True(t, ok)
			assert.ElementsMatch(t, auth.AdminPermissions(), adminPerms)

			// Verify NamespaceOwner role exists with correct permissions
			ownerPerms, ok := roles[auth.RoleNamespaceOwner]
			require.True(t, ok)
			assert.ElementsMatch(t, auth.NamespaceOwnerPermissions(), ownerPerms)

			// Verify PublicViewer role exists with correct permissions
			viewerPerms, ok := roles[auth.RolePublicViewer]
			require.True(t, ok)
			assert.ElementsMatch(t, auth.PublicViewerPermissions(), viewerPerms)
		})

		t.Run("AnonymousAdminBindingExists", func(t *testing.T) {
			d, adminKey := newDaemonWithAuth(t, setup)
			defer d.Shutdown(t)

			c := newClientWithAPIKey(t, d, adminKey)
			ctx := d.Context()

			// Find the Admin role ID
			var rolesRes pb.RolesListResponse
			err := c.RolesList(ctx, auth.SystemNamespace, &rolesRes, nil)
			require.NoError(t, err)

			var adminRoleID string
			for _, role := range rolesRes.Items {
				if role.Name == auth.RoleAdmin {
					adminRoleID = role.Id
					break
				}
			}
			require.NotEmpty(t, adminRoleID)

			// List role bindings and find the anonymous->Admin binding
			var bindingsRes pb.RoleBindingsListResponse
			err = c.RoleBindingsList(ctx, auth.SystemNamespace, &bindingsRes, nil)
			require.NoError(t, err)

			var found bool
			for _, binding := range bindingsRes.Items {
				if binding.UserId == "anonymous" && binding.RoleId == adminRoleID {
					found = true
					break
				}
			}
			assert.True(t, found)
		})

		t.Run("SurvivesRestart", func(t *testing.T) {
			d, adminKey := newDaemonWithAuth(t, setup)
			defer d.Shutdown(t)

			ctx := d.Context()
			adminClient := newClientWithAPIKey(t, d, adminKey)

			// Record the Admin role ID before re-bootstrap
			var rolesRes pb.RolesListResponse
			err := adminClient.RolesList(ctx, auth.SystemNamespace, &rolesRes, nil)
			require.NoError(t, err)
			var adminRoleID string
			for _, role := range rolesRes.Items {
				if role.Name == auth.RoleAdmin {
					adminRoleID = role.Id
					break
				}
			}
			require.NotEmpty(t, adminRoleID)

			// Simulate restart: call Bootstrap again on the same storage
			err = d.Service().Bootstrap(ctx)
			require.NoError(t, err)

			// Admin role ID should be stable (not regenerated)
			var rolesRes2 pb.RolesListResponse
			err = adminClient.RolesList(ctx, auth.SystemNamespace, &rolesRes2, nil)
			require.NoError(t, err)
			var adminRoleID2 string
			for _, role := range rolesRes2.Items {
				if role.Name == auth.RoleAdmin {
					adminRoleID2 = role.Id
					break
				}
			}
			require.Equal(t, adminRoleID, adminRoleID2)

			// Admin API key should still work (role bindings reference unchanged IDs)
			var listRes pb.NamespacesListResponse
			err = adminClient.NamespacesList(ctx, &listRes, nil)
			require.NoError(t, err)
		})
	})

	t.Run("OpenDoor", func(t *testing.T) {
		t.Run("AnonymousCanCreateUser", func(t *testing.T) {
			d, _ := newDaemonWithAuth(t, setup)
			defer d.Shutdown(t)

			// Create an unauthenticated client (anonymous has Admin via bootstrap)
			c := newClientWithAPIKey(t, d, "")
			ctx := d.Context()

			var userRes pb.UserCreateResponse
			err := c.UsersCreate(ctx, &pb.UserCreateRequest{
				Username: "opendoor-" + random.String("", 5),
			}, &userRes)
			require.NoError(t, err)
			assert.NotEmpty(t, userRes.Id)
		})

		t.Run("AnonymousCanCreateAPIKey", func(t *testing.T) {
			d, _ := newDaemonWithAuth(t, setup)
			defer d.Shutdown(t)

			c := newClientWithAPIKey(t, d, "")
			ctx := d.Context()

			// Create a user first
			var userRes pb.UserCreateResponse
			err := c.UsersCreate(ctx, &pb.UserCreateRequest{
				Username: "opendoor-key-" + random.String("", 5),
			}, &userRes)
			require.NoError(t, err)

			// Create an API key for the user
			var keyRes pb.APIKeyCreateResponse
			err = c.APIKeysCreate(ctx, &pb.APIKeyCreateRequest{
				UserId: userRes.Id,
				Name:   "opendoor-key",
			}, &keyRes)
			require.NoError(t, err)
			assert.NotEmpty(t, keyRes.Key)
		})

		t.Run("AnonymousCanCreateQueue", func(t *testing.T) {
			d, _ := newDaemonWithAuth(t, setup)
			defer d.Shutdown(t)

			c := newClientWithAPIKey(t, d, "")
			ctx := d.Context()

			err := c.QueuesCreate(ctx, &pb.QueueInfo{
				QueueName:           random.String("queue-", 10),
				LeaseTimeout:        LeaseTimeout,
				ExpireTimeout:       ExpireTimeout,
				RequestedPartitions: 1,
			})
			require.NoError(t, err)
		})

		t.Run("RemovingAnonymousBindingClosesOpenDoor", func(t *testing.T) {
			d, adminKey := newDaemonWithAuth(t, setup)
			defer d.Shutdown(t)

			ctx := d.Context()
			adminClient := newClientWithAPIKey(t, d, adminKey)
			anonClient := newClientWithAPIKey(t, d, "")

			// Unauthenticated requests succeed while Open Door is active
			var listRes pb.NamespacesListResponse
			err := anonClient.NamespacesList(ctx, &listRes, nil)
			require.NoError(t, err)

			// Delete the anonymous->Admin binding in _system
			err = adminClient.RoleBindingsDelete(ctx, &pb.RoleBindingDeleteRequest{
				Namespace: auth.SystemNamespace,
				RoleName:  auth.RoleAdmin,
				UserId:    auth.AnonymousUserID,
			})
			require.NoError(t, err)

			// Unauthenticated requests are now denied
			err = anonClient.NamespacesList(ctx, &listRes, nil)
			require.Error(t, err)
			var duhErr duh.Error
			require.ErrorAs(t, err, &duhErr)
			assert.Equal(t, duh.CodeForbidden, duhErr.Code())
		})
	})

	testAPIKeyTagCascade(t, setup)
}

func testAPIKeyTagCascade(t *testing.T, setup NewStorageFunc) {
	t.Helper()
	t.Run("KeyTagCascade", func(t *testing.T) {
		t.Run("RequestKeyTagOverridesAll", func(t *testing.T) {
			d, adminKey := newDaemonWithAuth(t, setup)
			defer d.Shutdown(t)

			c := newClientWithAPIKey(t, d, adminKey)
			ctx := d.Context()

			// Create a namespace with APIKeyTag "prod"
			nsName := random.String("ns-", 10)
			require.NoError(t, c.NamespacesCreate(ctx, &pb.NamespaceInfo{
				Name:      nsName,
				ApiKeyTag: "prod",
			}))

			// Create user and key with explicit KeyTag "ci" scoped to that namespace
			var userRes pb.UserCreateResponse
			require.NoError(t, c.UsersCreate(ctx, &pb.UserCreateRequest{
				Username: "cascade-user-" + random.String("", 5),
			}, &userRes))

			var keyRes pb.APIKeyCreateResponse
			require.NoError(t, c.APIKeysCreate(ctx, &pb.APIKeyCreateRequest{
				UserId:         userRes.Id,
				Name:           "cascade-key",
				KeyTag:         "ci",
				NamespaceScope: nsName,
			}, &keyRes))

			// The explicit KeyTag "ci" overrides the namespace APIKeyTag "prod"
			assert.True(t, len(keyRes.Key) > 0)
			assert.Contains(t, keyRes.Key, "qtr-ci-")
		})

		t.Run("NamespaceTagUsedWhenNoRequestTag", func(t *testing.T) {
			d, adminKey := newDaemonWithAuth(t, setup)
			defer d.Shutdown(t)

			c := newClientWithAPIKey(t, d, adminKey)
			ctx := d.Context()

			// Create a namespace with APIKeyTag "stg"
			nsName := random.String("ns-", 10)
			require.NoError(t, c.NamespacesCreate(ctx, &pb.NamespaceInfo{
				Name:      nsName,
				ApiKeyTag: "stg",
			}))

			var userRes pb.UserCreateResponse
			require.NoError(t, c.UsersCreate(ctx, &pb.UserCreateRequest{
				Username: "ns-tag-user-" + random.String("", 5),
			}, &userRes))

			// No KeyTag on request — should pick up namespace APIKeyTag "stg"
			var keyRes pb.APIKeyCreateResponse
			require.NoError(t, c.APIKeysCreate(ctx, &pb.APIKeyCreateRequest{
				UserId:         userRes.Id,
				Name:           "ns-tag-key",
				NamespaceScope: nsName,
			}, &keyRes))

			assert.Contains(t, keyRes.Key, "qtr-stg-")
		})

		t.Run("DefaultTagLiveWhenNoCascadeMatch", func(t *testing.T) {
			d, adminKey := newDaemonWithAuth(t, setup)
			defer d.Shutdown(t)

			c := newClientWithAPIKey(t, d, adminKey)
			ctx := d.Context()

			var userRes pb.UserCreateResponse
			require.NoError(t, c.UsersCreate(ctx, &pb.UserCreateRequest{
				Username: "default-tag-user-" + random.String("", 5),
			}, &userRes))

			// No KeyTag and no NamespaceScope — should default to "live"
			var keyRes pb.APIKeyCreateResponse
			require.NoError(t, c.APIKeysCreate(ctx, &pb.APIKeyCreateRequest{
				UserId: userRes.Id,
				Name:   "default-tag-key",
			}, &keyRes))

			assert.Contains(t, keyRes.Key, "qtr-live-")
		})

		t.Run("KeyTagValidationRejectsInvalidValues", func(t *testing.T) {
			d, adminKey := newDaemonWithAuth(t, setup)
			defer d.Shutdown(t)

			c := newClientWithAPIKey(t, d, adminKey)
			ctx := d.Context()

			var userRes pb.UserCreateResponse
			require.NoError(t, c.UsersCreate(ctx, &pb.UserCreateRequest{
				Username: "tag-validation-user-" + random.String("", 5),
			}, &userRes))

			for _, test := range []struct {
				Name   string
				KeyTag string
				Msg    string
			}{
				{
					Name:   "UppercaseLetters",
					KeyTag: "PROD",
					Msg:    "key_tag is invalid; must be lowercase alphanumeric only",
				},
				{
					Name:   "ContainsSpace",
					KeyTag: "my tag",
					Msg:    "key_tag is invalid; must be lowercase alphanumeric only",
				},
				{
					Name:   "TooLong",
					KeyTag: "thistagistoolong1",
					Msg:    "key_tag is invalid; cannot be greater than 16 characters",
				},
			} {
				t.Run(test.Name, func(t *testing.T) {
					var keyRes pb.APIKeyCreateResponse
					err := c.APIKeysCreate(ctx, &pb.APIKeyCreateRequest{
						UserId: userRes.Id,
						Name:   "invalid-tag-key",
						KeyTag: test.KeyTag,
					}, &keyRes)
					require.Error(t, err)
					var duhErr duh.Error
					require.ErrorAs(t, err, &duhErr)
					assert.Equal(t, duh.CodeBadRequest, duhErr.Code())
					assert.Contains(t, duhErr.Message(), test.Msg)
				})
			}
		})
	})
}

// authTestDaemon wraps testDaemon with auth-specific fields
type authTestDaemon struct {
	*testDaemon
	authBackend auth.AuthBackend
}

func (a *authTestDaemon) Shutdown(t *testing.T) {
	t.Helper()
	a.authBackend.Close()
	a.testDaemon.Shutdown(t)
}

// newDaemonWithAuth creates a daemon with DefaultAuthBackend and returns the admin API key.
// Bootstrap runs automatically during service.New(), creating the _system namespace, anonymous
// user, standard roles, and anonymous->Admin binding (Open Door mode).
func newDaemonWithAuth(t *testing.T, setup NewStorageFunc) (*authTestDaemon, string) {
	t.Helper()

	storageConf := setup()

	backend := internal.NewAuthBackend(internal.AuthBackendConfig{
		RoleBindings: storageConf.RoleBindings,
		APIKeys:      storageConf.APIKeys,
		Users:        storageConf.Users,
		Roles:        storageConf.Roles,
		Log:          log,
	})

	td := &testDaemon{}
	var err error
	td.ctx, td.cancel = context.WithTimeout(context.Background(), clock.Minute*10)

	td.d, err = daemon.NewDaemon(td.ctx, daemon.Config{
		Service:       svc.Config{StorageConfig: storageConf, Auth: backend, Log: log},
		AuthBackend:   backend,
		ListenAddress: "localhost:0",
	})
	require.NoError(t, err)

	atd := &authTestDaemon{
		testDaemon:  td,
		authBackend: backend,
	}

	// Use Open Door mode (anonymous has admin) to create admin credentials via the public API
	anonClient := newClientWithAPIKey(t, atd, "")
	adminKey := createAdminUser(t, td.ctx, anonClient)

	return atd, adminKey
}

// newClientWithAPIKey creates a client with the specified API key
func newClientWithAPIKey(t *testing.T, d *authTestDaemon, apiKey string) *querator.Client {
	t.Helper()

	c, err := querator.NewClient(querator.ClientConfig{
		Endpoint: "http://" + d.d.Listener.Addr().String(),
		APIKey:   apiKey,
	})
	require.NoError(t, err)
	return c
}

// createAdminUser creates an admin user with all permissions via the public API and returns the API key.
// The caller must provide an unauthenticated client (anonymous = admin via Open Door bootstrap).
func createAdminUser(t *testing.T, ctx context.Context, c *querator.Client) string {
	t.Helper()

	// Create user via the API
	var userRes pb.UserCreateResponse
	err := c.UsersCreate(ctx, &pb.UserCreateRequest{
		Username: "admin-" + random.String("", 5),
	}, &userRes)
	require.NoError(t, err)

	// Bind the new user to the existing Admin role in _system (created by bootstrap)
	var bindingRes pb.RoleBindingCreateResponse
	err = c.RoleBindingsCreate(ctx, &pb.RoleBindingCreateRequest{
		Namespace: auth.SystemNamespace,
		UserId:    userRes.Id,
		RoleName:  auth.RoleAdmin,
	}, &bindingRes)
	require.NoError(t, err)

	// Create an API key for the admin user
	var keyRes pb.APIKeyCreateResponse
	err = c.APIKeysCreate(ctx, &pb.APIKeyCreateRequest{
		UserId: userRes.Id,
		Name:   "admin-key",
	}, &keyRes)
	require.NoError(t, err)

	return keyRes.Key
}

// CreateUserWithLimitedPermissions creates a user with specific permissions in _system namespace
func CreateUserWithLimitedPermissions(t *testing.T, ctx context.Context, c *querator.Client, perms []string) string {
	t.Helper()

	// Create user
	var userRes pb.UserCreateResponse
	err := c.UsersCreate(ctx, &pb.UserCreateRequest{
		Username: "limited-" + random.String("", 5),
	}, &userRes)
	require.NoError(t, err)

	// Create role with limited permissions
	var roleRes pb.RoleCreateResponse
	err = c.RolesCreate(ctx, &pb.RoleCreateRequest{
		Namespace:   auth.SystemNamespace,
		Name:        "limited-role-" + random.String("", 5),
		Permissions: perms,
	}, &roleRes)
	require.NoError(t, err)

	// Get the role to find its name for role binding
	var rolesListRes pb.RolesListResponse
	err = c.RolesList(ctx, auth.SystemNamespace, &rolesListRes, nil)
	require.NoError(t, err)

	// Find the role we just created
	var roleName string
	for _, role := range rolesListRes.Items {
		if role.Id == roleRes.Id {
			roleName = role.Name
			break
		}
	}
	require.NotEmpty(t, roleName)

	// Create role binding
	var bindingRes pb.RoleBindingCreateResponse
	err = c.RoleBindingsCreate(ctx, &pb.RoleBindingCreateRequest{
		Namespace: auth.SystemNamespace,
		UserId:    userRes.Id,
		RoleName:  roleName,
	}, &bindingRes)
	require.NoError(t, err)

	// Create API key
	var keyRes pb.APIKeyCreateResponse
	err = c.APIKeysCreate(ctx, &pb.APIKeyCreateRequest{
		UserId: userRes.Id,
		Name:   "limited-key",
	}, &keyRes)
	require.NoError(t, err)

	return keyRes.Key
}

// CreateUserWithNamespacePermissions creates a user with permissions in a specific namespace
func CreateUserWithNamespacePermissions(t *testing.T, ctx context.Context, c *querator.Client, namespace string, perms []string) string {
	t.Helper()

	// Create user
	var userRes pb.UserCreateResponse
	err := c.UsersCreate(ctx, &pb.UserCreateRequest{
		Username: "ns-user-" + random.String("", 5),
	}, &userRes)
	require.NoError(t, err)

	// Create role with namespace permissions
	var roleRes pb.RoleCreateResponse
	err = c.RolesCreate(ctx, &pb.RoleCreateRequest{
		Namespace:   namespace,
		Name:        "ns-role-" + random.String("", 5),
		Permissions: perms,
	}, &roleRes)
	require.NoError(t, err)

	// Get the role to find its name for role binding
	var rolesListRes pb.RolesListResponse
	err = c.RolesList(ctx, namespace, &rolesListRes, nil)
	require.NoError(t, err)

	// Find the role we just created
	var roleName string
	for _, role := range rolesListRes.Items {
		if role.Id == roleRes.Id {
			roleName = role.Name
			break
		}
	}
	require.NotEmpty(t, roleName)

	// Create role binding in the specific namespace
	var bindingRes pb.RoleBindingCreateResponse
	err = c.RoleBindingsCreate(ctx, &pb.RoleBindingCreateRequest{
		Namespace: namespace,
		UserId:    userRes.Id,
		RoleName:  roleName,
	}, &bindingRes)
	require.NoError(t, err)

	// Create API key scoped to the namespace
	var keyRes pb.APIKeyCreateResponse
	err = c.APIKeysCreate(ctx, &pb.APIKeyCreateRequest{
		UserId:         userRes.Id,
		Name:           "ns-key",
		NamespaceScope: namespace,
	}, &keyRes)
	require.NoError(t, err)

	return keyRes.Key
}
