package daemon_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/duh-rpc/duh-go"
	v1 "github.com/duh-rpc/duh-go/proto/v1"
	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/daemon"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/querator/transport/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestConfigFileInternalAuth(t *testing.T) {
	t.Run("MemoryQueueStorageChecksKeys", func(t *testing.T) {
		ctx := context.Background()
		d := startFromConfig(t, `
address: localhost:0
auth-backend:
  driver: internal
`)
		defer func() { require.NoError(t, d.Shutdown(ctx)) }()

		adminKey := createAdminKey(t, ctx, d)
		closeOpenDoor(t, ctx, d, adminKey)

		var list pb.QueuesListResponse
		require.NoError(t, clientWithKey(t, d, adminKey).QueuesList(ctx, &list, nil))

		err := clientWithKey(t, d, "").QueuesList(ctx, &list, nil)
		var duhErr duh.Error
		require.ErrorAs(t, err, &duhErr)
		assert.Equal(t, duh.CodeForbidden, duhErr.Code())

		err = clientWithKey(t, d, "qtr-not-a-real-key").QueuesList(ctx, &list, nil)
		require.ErrorAs(t, err, &duhErr)
		assert.Equal(t, duh.CodeUnauthorized, duhErr.Code())
	})

	t.Run("BearerSchemeInAnyCase", func(t *testing.T) {
		ctx := context.Background()
		d := startFromConfig(t, `
address: localhost:0
auth-backend:
  driver: internal
`)
		defer func() { require.NoError(t, d.Shutdown(ctx)) }()

		adminKey := createAdminKey(t, ctx, d)
		closeOpenDoor(t, ctx, d, adminKey)

		for _, scheme := range []string{"Bearer", "bearer", "BEARER", "bEaReR"} {
			t.Run(scheme, func(t *testing.T) {
				require.NoError(t, listQueuesWithHeader(t, ctx, d, scheme+" "+adminKey))
			})
		}
	})

	t.Run("OtherSchemesRefused", func(t *testing.T) {
		ctx := context.Background()
		d := startFromConfig(t, `
address: localhost:0
auth-backend:
  driver: internal
`)
		defer func() { require.NoError(t, d.Shutdown(ctx)) }()

		adminKey := createAdminKey(t, ctx, d)
		closeOpenDoor(t, ctx, d, adminKey)

		for _, test := range []struct {
			name   string
			header string
		}{
			{name: "Basic", header: "Basic " + adminKey},
			{name: "Token", header: "Token " + adminKey},
			{name: "BearerWithColon", header: "Bearer:" + adminKey},
			{name: "BearerPrefixedWord", header: "Bearerx " + adminKey},
			{name: "NoScheme", header: adminKey},
		} {
			t.Run(test.name, func(t *testing.T) {
				err := listQueuesWithHeader(t, ctx, d, test.header)
				var duhErr duh.Error
				require.ErrorAs(t, err, &duhErr)
				assert.Equal(t, duh.CodeUnauthorized, duhErr.Code())
			})
		}
	})

	t.Run("BadgerQueueStorageKeepsKeysAcrossRestart", func(t *testing.T) {
		ctx := context.Background()
		config := fmt.Sprintf(`
address: localhost:0
queue-storage:
  badger:
    storage-dir: %[1]s
partition-storage:
  - name: badger-00
    affinity: 1
    badger:
      storage-dir: %[1]s
auth-backend:
  driver: internal
`, t.TempDir())

		d := startFromConfig(t, config)
		adminKey := createAdminKey(t, ctx, d)
		require.NoError(t, d.Shutdown(ctx))

		d = startFromConfig(t, config)
		defer func() { require.NoError(t, d.Shutdown(ctx)) }()

		var users pb.UsersListResponse
		require.NoError(t, clientWithKey(t, d, adminKey).UsersList(ctx, &users, nil))
		var names []string
		for _, user := range users.Items {
			names = append(names, user.Username)
		}
		assert.Contains(t, names, "admin")
	})

	t.Run("NamespacesInMemoryQueueStorage", func(t *testing.T) {
		ctx := context.Background()
		d := startFromConfig(t, `
address: localhost:0
auth-backend:
  driver: internal
`)
		defer func() { require.NoError(t, d.Shutdown(ctx)) }()

		c := clientWithKey(t, d, createAdminKey(t, ctx, d))
		require.NoError(t, c.NamespacesCreate(ctx, &pb.NamespaceInfo{Name: "orders"}))
		require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
			QueueName:           "orders-queue",
			Namespace:           "orders",
			LeaseTimeout:        "1m",
			ExpireTimeout:       "10m",
			RequestedPartitions: 1,
		}))

		err := c.QueuesCreate(ctx, &pb.QueueInfo{
			QueueName:           "billing-queue",
			Namespace:           "billing",
			LeaseTimeout:        "1m",
			ExpireTimeout:       "10m",
			RequestedPartitions: 1,
		})
		var duhErr duh.Error
		require.ErrorAs(t, err, &duhErr)
		assert.Equal(t, duh.CodeRequestFailed, duhErr.Code())
		assert.Contains(t, duhErr.Message(), "billing")
	})

	t.Run("NamespacesInBadgerQueueStorageSurviveRestart", func(t *testing.T) {
		ctx := context.Background()
		config := fmt.Sprintf(`
address: localhost:0
queue-storage:
  badger:
    storage-dir: %[1]s
partition-storage:
  - name: badger-00
    affinity: 1
    badger:
      storage-dir: %[1]s
auth-backend:
  driver: internal
`, t.TempDir())

		d := startFromConfig(t, config)
		adminKey := createAdminKey(t, ctx, d)
		require.NoError(t, clientWithKey(t, d, adminKey).NamespacesCreate(ctx, &pb.NamespaceInfo{Name: "orders"}))
		require.NoError(t, d.Shutdown(ctx))

		d = startFromConfig(t, config)
		defer func() { require.NoError(t, d.Shutdown(ctx)) }()

		var namespaces pb.NamespacesListResponse
		require.NoError(t, clientWithKey(t, d, adminKey).NamespacesList(ctx, &namespaces, nil))
		var names []string
		for _, namespace := range namespaces.Items {
			names = append(names, namespace.Name)
		}
		assert.Contains(t, names, "orders")
	})
}

func TestConfigFileExampleWithInternalAuth(t *testing.T) {
	example, err := os.ReadFile("../example.yaml")
	require.NoError(t, err)

	config := string(example)
	for _, line := range []string{
		"address: 0.0.0.0:2319",
		"storage-dir: /data/queues",
		"storage-dir: /data/partitions",
		"#auth-backend:\n#  driver: internal",
	} {
		require.Contains(t, config, line)
	}
	config = strings.NewReplacer(
		"address: 0.0.0.0:2319", "address: localhost:0",
		"storage-dir: /data/queues", "storage-dir: "+t.TempDir(),
		"storage-dir: /data/partitions", "storage-dir: "+t.TempDir(),
		"#auth-backend:\n#  driver: internal", "auth-backend:\n  driver: internal",
	).Replace(config)

	ctx := context.Background()
	d := startFromConfig(t, config)
	defer func() { require.NoError(t, d.Shutdown(ctx)) }()

	adminKey := createAdminKey(t, ctx, d)
	closeOpenDoor(t, ctx, d, adminKey)

	var list pb.QueuesListResponse
	require.NoError(t, clientWithKey(t, d, adminKey).QueuesList(ctx, &list, nil))

	err = clientWithKey(t, d, "").QueuesList(ctx, &list, nil)
	var duhErr duh.Error
	require.ErrorAs(t, err, &duhErr)
	assert.Equal(t, duh.CodeForbidden, duhErr.Code())
}

func TestConfigFileNoAuthServesAnyAuthorizationHeader(t *testing.T) {
	for _, test := range []struct {
		name string
		yaml string
	}{
		{name: "DriverUnset", yaml: "address: localhost:0\n"},
		{name: "DriverNone", yaml: "address: localhost:0\nauth-backend:\n  driver: none\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			d := startFromConfig(t, test.yaml)
			defer func() { require.NoError(t, d.Shutdown(ctx)) }()

			for _, header := range []struct {
				name  string
				value string
			}{
				{name: "Basic", value: "Basic c29tZS1jcmVkZW50aWFscw=="},
				{name: "LowercaseBearerUnknownKey", value: "bearer qtr-not-a-real-key"},
				{name: "BearerUnknownKey", value: "Bearer qtr-not-a-real-key"},
				{name: "NoScheme", value: "no-scheme-at-all"},
			} {
				t.Run(header.name, func(t *testing.T) {
					require.NoError(t, listQueuesWithHeader(t, ctx, d, header.value))
				})
			}
		})
	}
}

func startFromConfig(t *testing.T, config string) *daemon.Daemon {
	t.Helper()
	file, err := daemon.ReadConfig(strings.NewReader(config))
	require.NoError(t, err)

	var conf daemon.Config
	ctx := context.Background()
	require.NoError(t, daemon.ApplyConfigFile(ctx, &conf, file, io.Discard))

	d, err := daemon.NewDaemon(ctx, conf)
	require.NoError(t, err)
	return d
}

func clientWithKey(t *testing.T, d *daemon.Daemon, key string) *querator.Client {
	t.Helper()
	c, err := querator.NewClient(querator.ClientConfig{
		Endpoint: "http://" + d.Listener.Addr().String(),
		APIKey:   key,
	})
	require.NoError(t, err)
	return c
}

func createAdminKey(t *testing.T, ctx context.Context, d *daemon.Daemon) string {
	t.Helper()
	anonymous := clientWithKey(t, d, "")

	var user pb.UserCreateResponse
	require.NoError(t, anonymous.UsersCreate(ctx, &pb.UserCreateRequest{Username: "admin"}, &user))

	var binding pb.RoleBindingCreateResponse
	require.NoError(t, anonymous.RoleBindingsCreate(ctx, &pb.RoleBindingCreateRequest{
		Namespace: auth.SystemNamespace,
		UserId:    user.Id,
		RoleName:  auth.RoleAdmin,
	}, &binding))

	var key pb.APIKeyCreateResponse
	require.NoError(t, anonymous.APIKeysCreate(ctx, &pb.APIKeyCreateRequest{
		UserId: user.Id,
		Name:   "admin-key",
	}, &key))
	return key.Key
}

func closeOpenDoor(t *testing.T, ctx context.Context, d *daemon.Daemon, adminKey string) {
	t.Helper()
	require.NoError(t, clientWithKey(t, d, adminKey).RoleBindingsDelete(ctx, &pb.RoleBindingDeleteRequest{
		Namespace: auth.SystemNamespace,
		RoleName:  auth.RoleAdmin,
		UserId:    auth.AnonymousUserID,
	}))
}

func listQueuesWithHeader(t *testing.T, ctx context.Context, d *daemon.Daemon, header string) error {
	t.Helper()
	payload, err := proto.Marshal(&pb.QueuesListRequest{})
	require.NoError(t, err)

	r, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://"+d.Listener.Addr().String()+transport.RPCQueuesList, bytes.NewReader(payload))
	require.NoError(t, err)
	r.Header.Set("Content-Type", duh.ContentTypeProtoBuf)
	r.Header.Set("Authorization", header)

	var res v1.Reply
	return duh.HTTP1Client.Do(r, &res)
}
