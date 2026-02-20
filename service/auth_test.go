package service_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/duh-rpc/duh-go"
	v1 "github.com/duh-rpc/duh-go/proto/v1"
	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/daemon"
	"github.com/kapetan-io/querator/internal/auth"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	pb "github.com/kapetan-io/querator/proto"
	svc "github.com/kapetan-io/querator/service"
	"github.com/kapetan-io/querator/transport"
	tauth "github.com/kapetan-io/querator/transport/auth"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/kapetan-io/tackle/set"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/proto"
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
			storageConf := setup()
			d, apiKey := newDaemonWithAuth(t, storageConf)
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
			storageConf := setup()
			d, _ := newDaemonWithAuth(t, storageConf)
			defer d.Shutdown(t)

			// Create client with invalid API key
			c := newClientWithAPIKey(t, d, "qtr_invalid_invalidkey")
			ctx := d.Context()

			// Request should fail with 401
			var listRes pb.QueuesListResponse
			err := c.QueuesList(ctx, &listRes, nil)
			require.Error(t, err)
			var duhErr duh.Error
			require.ErrorAs(t, err, &duhErr)
			assert.Equal(t, duh.CodeUnauthorized, duhErr.Code())
		})

		t.Run("MissingAPIKeyWithAuthEnabled", func(t *testing.T) {
			storageConf := setup()
			d, _ := newDaemonWithAuth(t, storageConf)
			defer d.Shutdown(t)

			// Create client without API key
			c := newClientWithAPIKey(t, d, "")
			ctx := d.Context()

			// Request should fail with 401
			var listRes pb.QueuesListResponse
			err := c.QueuesList(ctx, &listRes, nil)
			require.Error(t, err)
			var duhErr duh.Error
			require.ErrorAs(t, err, &duhErr)
			assert.Equal(t, duh.CodeUnauthorized, duhErr.Code())
		})

		t.Run("ExpiredAPIKey", func(t *testing.T) {
			storageConf := setup()
			d, _ := newDaemonWithAuth(t, storageConf)
			defer d.Shutdown(t)

			ctx := d.Context()

			// Create user directly in storage
			user := types.User{
				ID:       random.String("user-", 10),
				Username: "expired-" + random.String("", 5),
			}
			err := storageConf.Users.Add(ctx, user)
			require.NoError(t, err)

			// Generate an API key and store it with an expiration in the past
			generatedKey, err := auth.GenerateAPIKey("")
			require.NoError(t, err)

			expiredAt := clock.Now().UTC().Add(-clock.Hour)
			err = storageConf.APIKeys.Add(ctx, types.APIKey{
				ExpiresAt: &expiredAt,
				KeyPrefix: generatedKey.Prefix,
				KeyHash:   generatedKey.KeyHash,
				UserID:    user.ID,
				Name:      "expired-key",
				ID:        random.String("key-", 10),
			})
			require.NoError(t, err)

			// Authenticate with the expired key
			c := newClientWithAPIKey(t, d, generatedKey.Key)
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
			storageConf := setup()
			d, apiKey := newDaemonWithAuth(t, storageConf)
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
			storageConf := setup()
			d, adminKey := newDaemonWithAuth(t, storageConf)
			defer d.Shutdown(t)

			ctx := d.Context()
			adminClient := newClientWithAPIKey(t, d, adminKey)

			// Create a user with limited permissions (only queue.list, no queue.create)
			limitedKey := createUserWithLimitedPermissions(t, ctx, adminClient, []string{tauth.QueueList})

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
			storageConf := setup()
			d, adminKey := newDaemonWithAuth(t, storageConf)
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
			ns1Key := createUserWithNamespacePermissions(t, ctx, adminClient, ns1, tauth.NamespaceOwnerPermissions)

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

		t.Run("SystemNamespaceCascade", func(t *testing.T) {
			storageConf := setup()
			d, adminKey := newDaemonWithAuth(t, storageConf)
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
	})

	t.Run("Errors", func(t *testing.T) {
		t.Run("MalformedAuthHeader", func(t *testing.T) {
			storageConf := setup()
			d, _ := newDaemonWithAuth(t, storageConf)
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
}

// authTestDaemon wraps testDaemon with auth-specific fields
type authTestDaemon struct {
	*testDaemon
	authBackend *daemon.AuthBackendAdapter
	storageConf store.Config
}

func (a *authTestDaemon) Shutdown(t *testing.T) {
	t.Helper()
	a.authBackend.Close()
	a.testDaemon.Shutdown(t)
}

// newDaemonWithAuth creates a daemon with DefaultAuthBackend and returns the admin API key
func newDaemonWithAuth(t *testing.T, storageConf store.Config) (*authTestDaemon, string) {
	t.Helper()

	set.Default(&storageConf.Namespaces, store.NewMemoryNamespaces(log))
	set.Default(&storageConf.Users, store.NewMemoryUsers(log))
	set.Default(&storageConf.APIKeys, store.NewMemoryAPIKeys(log))
	set.Default(&storageConf.Roles, store.NewMemoryRoles(log))
	set.Default(&storageConf.RoleBindings, store.NewMemoryRoleBindings(log))

	// Create internal auth backend
	internalBackend := auth.NewDefaultAuthBackend(auth.DefaultAuthBackendConfig{
		RoleBindings: storageConf.RoleBindings,
		APIKeys:      storageConf.APIKeys,
		Users:        storageConf.Users,
		Roles:        storageConf.Roles,
		Log:          log,
	})

	// Create adapter
	authAdapter := daemon.NewAuthBackendAdapter(internalBackend)

	td := &testDaemon{}
	var err error
	td.ctx, td.cancel = context.WithTimeout(context.Background(), clock.Minute*10)

	td.d, err = daemon.NewDaemon(td.ctx, daemon.Config{
		Service:       svc.Config{StorageConfig: storageConf, Log: log},
		AuthBackend:   authAdapter,
		ListenAddress: "localhost:0", // Use random available port
	})
	require.NoError(t, err)

	// Create admin user and API key
	adminKey := createAdminUser(t, td.ctx, storageConf)

	return &authTestDaemon{
		testDaemon:  td,
		authBackend: authAdapter,
		storageConf: storageConf,
	}, adminKey
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

// createAdminUser creates an admin user with all permissions and returns the API key
func createAdminUser(t *testing.T, ctx context.Context, storageConf store.Config) string {
	t.Helper()

	// First, ensure _system namespace exists
	err := storageConf.Namespaces.Add(ctx, types.Namespace{
		Name:        tauth.SystemNamespace,
		Description: "System namespace for global administration",
	})
	require.NoError(t, err)

	// Create admin role in _system namespace
	adminRole := types.Role{
		ID:          random.String("role-", 10),
		Name:        "admin-" + random.String("", 5),
		Namespace:   tauth.SystemNamespace,
		Permissions: tauth.AllPermissions,
	}
	err = storageConf.Roles.Add(ctx, adminRole)
	require.NoError(t, err)

	// Create admin user
	adminUser := types.User{
		ID:       random.String("user-", 10),
		Username: "admin-" + random.String("", 5),
	}
	err = storageConf.Users.Add(ctx, adminUser)
	require.NoError(t, err)

	// Create role binding
	binding := types.RoleBinding{
		ID:        random.String("binding-", 10),
		UserID:    adminUser.ID,
		RoleID:    adminRole.ID,
		Namespace: tauth.SystemNamespace,
	}
	err = storageConf.RoleBindings.Add(ctx, binding)
	require.NoError(t, err)

	// Generate and store API key
	generatedKey, err := auth.GenerateAPIKey("")
	require.NoError(t, err)

	apiKey := types.APIKey{
		ID:        random.String("key-", 10),
		UserID:    adminUser.ID,
		Name:      "admin-key",
		KeyHash:   generatedKey.KeyHash,
		KeyPrefix: generatedKey.Prefix,
	}
	err = storageConf.APIKeys.Add(ctx, apiKey)
	require.NoError(t, err)

	return generatedKey.Key
}

// createUserWithLimitedPermissions creates a user with specific permissions in _system namespace
func createUserWithLimitedPermissions(t *testing.T, ctx context.Context, c *querator.Client, perms []string) string {
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
		Namespace:   tauth.SystemNamespace,
		Name:        "limited-role-" + random.String("", 5),
		Permissions: perms,
	}, &roleRes)
	require.NoError(t, err)

	// Get the role to find its name for role binding
	var rolesListRes pb.RolesListResponse
	err = c.RolesList(ctx, tauth.SystemNamespace, &rolesListRes, nil)
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
		Namespace: tauth.SystemNamespace,
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

// createUserWithNamespacePermissions creates a user with permissions in a specific namespace
func createUserWithNamespacePermissions(t *testing.T, ctx context.Context, c *querator.Client, namespace string, perms []string) string {
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
