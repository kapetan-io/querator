package service_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/duh-rpc/duh-go"
	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/internal/store"
	pb "github.com/kapetan-io/querator/proto"
	svc "github.com/kapetan-io/querator/service"
	"github.com/kapetan-io/querator/transport/auth"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestNamespaces(t *testing.T) {
	for _, tc := range []struct {
		Setup    NewStorageFunc
		TearDown func()
		Name     string
	}{
		{
			Name: "InMemory",
			Setup: func() store.Config {
				return setupMemoryStorage(store.Config{})
			},
			TearDown: func() {},
		},
		{
			Name: "BadgerDB",
			Setup: func() store.Config {
				badger := badgerTestSetup{Dir: t.TempDir()}
				t.Cleanup(func() {
					badger.Teardown()
				})
				return badger.Setup(store.BadgerConfig{})
			},
			TearDown: func() {},
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			testNamespaces(t, tc.Setup, tc.TearDown)
		})
	}
}

func testNamespaces(t *testing.T, setup NewStorageFunc, tearDown func()) {
	defer goleak.VerifyNone(t, goleakOptions...)

	testNamespaceAPIKeyTag(t, setup, tearDown)
	testNamespacesUpdate(t, setup, tearDown)

	t.Run("CRUD", func(t *testing.T) {
		d, c, ctx := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()

		t.Run("Create", func(t *testing.T) {
			nsName := random.String("ns-", 10)

			require.NoError(t, c.NamespacesCreate(ctx, &pb.NamespaceInfo{
				Name:        nsName,
				Description: "Test namespace",
			}))

			var list pb.NamespacesListResponse
			require.NoError(t, c.NamespacesList(ctx, &list, &querator.ListOptions{
				Pivot: nsName,
				Limit: 1,
			}))
			require.Equal(t, 1, len(list.Items))
			assert.Equal(t, nsName, list.Items[0].Name)
			assert.Equal(t, "Test namespace", list.Items[0].Description)
			assert.NotEmpty(t, list.Items[0].CreatedAt)
		})

		t.Run("Delete", func(t *testing.T) {
			nsName := random.String("ns-", 10)

			require.NoError(t, c.NamespacesCreate(ctx, &pb.NamespaceInfo{
				Name:        nsName,
				Description: "To be deleted",
			}))

			// Verify namespace exists
			var list pb.NamespacesListResponse
			require.NoError(t, c.NamespacesList(ctx, &list, &querator.ListOptions{
				Pivot: nsName,
				Limit: 1,
			}))
			require.Equal(t, 1, len(list.Items))
			assert.Equal(t, nsName, list.Items[0].Name)

			// Delete the namespace
			require.NoError(t, c.NamespacesDelete(ctx, &pb.NamespacesDeleteRequest{Name: nsName}))

			// Verify namespace is deleted
			var afterDelete pb.NamespacesListResponse
			require.NoError(t, c.NamespacesList(ctx, &afterDelete, &querator.ListOptions{
				Pivot: nsName,
				Limit: 1,
			}))
			// List should return empty or a different namespace
			if len(afterDelete.Items) > 0 {
				assert.NotEqual(t, nsName, afterDelete.Items[0].Name)
			}
		})
	})

	t.Run("List", func(t *testing.T) {
		d, c, ctx := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()

		// Create multiple namespaces
		const numNamespaces = 10
		for i := 0; i < numNamespaces; i++ {
			require.NoError(t, c.NamespacesCreate(ctx, &pb.NamespaceInfo{
				Name:        random.String("list-ns-", 10),
				Description: random.String("desc-", 20),
			}))
		}

		t.Run("ListAll", func(t *testing.T) {
			var list pb.NamespacesListResponse
			require.NoError(t, c.NamespacesList(ctx, &list, nil))
			assert.GreaterOrEqual(t, len(list.Items), numNamespaces)
		})

		t.Run("ListWithLimit", func(t *testing.T) {
			var list pb.NamespacesListResponse
			require.NoError(t, c.NamespacesList(ctx, &list, &querator.ListOptions{Limit: 5}))
			assert.Equal(t, 5, len(list.Items))
		})

		t.Run("ListWithPivot", func(t *testing.T) {
			// First get the list to know a namespace name
			var first pb.NamespacesListResponse
			require.NoError(t, c.NamespacesList(ctx, &first, &querator.ListOptions{Limit: 3}))
			require.GreaterOrEqual(t, len(first.Items), 3)

			// Use the second item as pivot
			pivot := first.Items[1].Name
			var pivoted pb.NamespacesListResponse
			require.NoError(t, c.NamespacesList(ctx, &pivoted, &querator.ListOptions{
				Pivot: pivot,
				Limit: 2,
			}))
			require.Equal(t, 2, len(pivoted.Items))
			assert.Equal(t, pivot, pivoted.Items[0].Name)
		})
	})

	t.Run("Errors", func(t *testing.T) {
		d, c, ctx := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()

		t.Run("NamespacesCreate", func(t *testing.T) {
			// Create a namespace first for the duplicate test
			existingNs := random.String("existing-", 10)
			require.NoError(t, c.NamespacesCreate(ctx, &pb.NamespaceInfo{
				Name: existingNs,
			}))

			for _, test := range []struct {
				Name string
				Req  *pb.NamespaceInfo
				Msg  string
				Code int
			}{
				{
					Name: "EmptyName",
					Req:  &pb.NamespaceInfo{Name: ""},
					Msg:  "namespace name is invalid; cannot be empty",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "ReservedPrefix",
					Req:  &pb.NamespaceInfo{Name: "_reserved"},
					Msg:  "namespace name is reserved;",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "AlreadyExists",
					Req:  &pb.NamespaceInfo{Name: existingNs},
					Msg:  "namespace already exists",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "NameTooLong",
					Req:  &pb.NamespaceInfo{Name: random.String("", 300)},
					Msg:  "namespace name is invalid; cannot be greater than '256' characters",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "WhitespaceOnly",
					Req:  &pb.NamespaceInfo{Name: "   "},
					Msg:  "namespace name is invalid; cannot be empty",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "ContainsWhitespace",
					Req:  &pb.NamespaceInfo{Name: "my namespace"},
					Msg:  "namespace name is invalid; 'my namespace' cannot contain whitespace",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "ContainsTilde",
					Req:  &pb.NamespaceInfo{Name: "my~namespace"},
					Msg:  "namespace name is invalid; 'my~namespace' cannot contain '~' character",
					Code: duh.CodeBadRequest,
				},
			} {
				t.Run(test.Name, func(t *testing.T) {
					err := c.NamespacesCreate(ctx, test.Req)
					require.Error(t, err)
					var e duh.Error
					require.True(t, errors.As(err, &e))
					assert.Contains(t, e.Message(), test.Msg)
					assert.Equal(t, test.Code, e.Code())
				})
			}
		})

		t.Run("NamespacesDelete", func(t *testing.T) {
			for _, test := range []struct {
				Name string
				Req  *pb.NamespacesDeleteRequest
				Msg  string
				Code int
			}{
				{
					Name: "NotFound",
					Req:  &pb.NamespacesDeleteRequest{Name: "nonexistent-namespace"},
					Msg:  "namespace does not exist",
					Code: duh.CodeRequestFailed,
				},
				{
					Name: "EmptyName",
					Req:  &pb.NamespacesDeleteRequest{Name: ""},
					Msg:  "namespace does not exist",
					Code: duh.CodeRequestFailed,
				},
				{
					Name: "SystemNamespace",
					Req:  &pb.NamespacesDeleteRequest{Name: "_system"},
					Msg:  "namespace name is reserved",
					Code: duh.CodeBadRequest,
				},
			} {
				t.Run(test.Name, func(t *testing.T) {
					err := c.NamespacesDelete(ctx, test.Req)
					require.Error(t, err)
					var e duh.Error
					require.True(t, errors.As(err, &e))
					assert.Contains(t, e.Message(), test.Msg)
					assert.Equal(t, test.Code, e.Code())
				})
			}
		})

		t.Run("NamespaceHasRoles", func(t *testing.T) {
			ns := random.String("ns-", 10)
			require.NoError(t, c.NamespacesCreate(ctx, &pb.NamespaceInfo{Name: ns}))

			// Create a role in the namespace
			var roleRes pb.RoleCreateResponse
			err := c.RolesCreate(ctx, &pb.RoleCreateRequest{
				Namespace:   ns,
				Name:        "test-role-" + random.String("", 5),
				Permissions: []string{auth.QueueList},
			}, &roleRes)
			require.NoError(t, err)

			// Attempt to delete the namespace that has roles
			err = c.NamespacesDelete(ctx, &pb.NamespacesDeleteRequest{Name: ns})
			require.Error(t, err)
			var duhErr duh.Error
			require.ErrorAs(t, err, &duhErr)
			assert.Contains(t, duhErr.Message(), "namespace has roles")
		})

		t.Run("NamespaceHasRoleBindings", func(t *testing.T) {
			ns := random.String("ns-", 10)
			require.NoError(t, c.NamespacesCreate(ctx, &pb.NamespaceInfo{Name: ns}))

			// Create a user
			var userRes pb.UserCreateResponse
			err := c.UsersCreate(ctx, &pb.UserCreateRequest{
				Username: "binding-user-" + random.String("", 5),
			}, &userRes)
			require.NoError(t, err)

			// Create a role in the namespace
			roleName := "binding-role-" + random.String("", 5)
			var roleRes pb.RoleCreateResponse
			err = c.RolesCreate(ctx, &pb.RoleCreateRequest{
				Namespace:   ns,
				Name:        roleName,
				Permissions: []string{auth.QueueList},
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

			// Attempt to delete the namespace that has role bindings
			err = c.NamespacesDelete(ctx, &pb.NamespacesDeleteRequest{Name: ns})
			require.Error(t, err)
			var duhErr duh.Error
			require.ErrorAs(t, err, &duhErr)
			assert.Contains(t, duhErr.Message(), "namespace has role bindings")
		})
	})

	t.Run("Queues", func(t *testing.T) {
		t.Run("QueuesList", func(t *testing.T) {
			d, c, ctx := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
			defer func() {
				d.Shutdown(t)
				tearDown()
			}()

			ns1 := random.String("ns1-", 10)
			ns2 := random.String("ns2-", 10)
			require.NoError(t, c.NamespacesCreate(ctx, &pb.NamespaceInfo{Name: ns1}))
			require.NoError(t, c.NamespacesCreate(ctx, &pb.NamespaceInfo{Name: ns2}))

			// Create queues in ns1
			require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
				QueueName:           random.String("q-ns1-", 10),
				Namespace:           ns1,
				LeaseTimeout:        "1m",
				ExpireTimeout:       "10m",
				RequestedPartitions: 1,
			}))
			require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
				QueueName:           random.String("q-ns1-", 10),
				Namespace:           ns1,
				LeaseTimeout:        "1m",
				ExpireTimeout:       "10m",
				RequestedPartitions: 1,
			}))

			// Create a queue in ns2
			require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
				QueueName:           random.String("q-ns2-", 10),
				Namespace:           ns2,
				LeaseTimeout:        "1m",
				ExpireTimeout:       "10m",
				RequestedPartitions: 1,
			}))

			// List queues scoped to ns1 — should only return ns1 queues
			var ns1List pb.QueuesListResponse
			require.NoError(t, c.QueuesList(ctx, &ns1List, &querator.ListOptions{Namespace: ns1}))
			require.Len(t, ns1List.Items, 2)
			for _, item := range ns1List.Items {
				assert.Equal(t, ns1, item.Namespace)
			}

			// List queues scoped to ns2 — should only return ns2 queues
			var ns2List pb.QueuesListResponse
			require.NoError(t, c.QueuesList(ctx, &ns2List, &querator.ListOptions{Namespace: ns2}))
			require.Len(t, ns2List.Items, 1)
			assert.Equal(t, ns2, ns2List.Items[0].Namespace)
		})

		t.Run("Errors", func(t *testing.T) {
			t.Run("QueuesCreate", func(t *testing.T) {
				d, c, ctx := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
				defer func() {
					d.Shutdown(t)
					tearDown()
				}()

				// Attempting to create a queue in a namespace that does not exist should fail
				err := c.QueuesCreate(ctx, &pb.QueueInfo{
					QueueName:           random.String("q-", 10),
					Namespace:           "nonexistent-ns-" + random.String("", 5),
					LeaseTimeout:        "1m",
					ExpireTimeout:       "10m",
					RequestedPartitions: 1,
				})
				require.Error(t, err)
				var e duh.Error
				require.ErrorAs(t, err, &e)
				assert.Contains(t, e.Message(), "namespace does not exist")
			})
		})
	})

	t.Run("ConcurrentOperations", func(t *testing.T) {
		d, c, ctx := newDaemon(t, clock.Minute, svc.Config{StorageConfig: setup()})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()

		const goroutines = 10
		var wg sync.WaitGroup
		wg.Add(goroutines)

		for i := 0; i < goroutines; i++ {
			go func(idx int) {
				defer wg.Done()
				ns := fmt.Sprintf("concurrent-ns-%d", idx)

				err := c.NamespacesCreate(ctx, &pb.NamespaceInfo{Name: ns})
				require.NoError(t, err)

				var listRes pb.NamespacesListResponse
				err = c.NamespacesList(ctx, &listRes, nil)
				require.NoError(t, err)

				err = c.NamespacesDelete(ctx, &pb.NamespacesDeleteRequest{Name: ns})
				require.NoError(t, err)
			}(i)
		}
		wg.Wait()
	})
}

func testNamespaceAPIKeyTag(t *testing.T, setup NewStorageFunc, tearDown func()) {
	t.Helper()
	t.Run("APIKeyTag", func(t *testing.T) {
		t.Run("CreateWithAPIKeyTag", func(t *testing.T) {
			d, c, ctx := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
			defer func() {
				d.Shutdown(t)
				tearDown()
			}()

			nsName := random.String("ns-", 10)
			require.NoError(t, c.NamespacesCreate(ctx, &pb.NamespaceInfo{
				Name:      nsName,
				ApiKeyTag: "dev",
			}))

			var list pb.NamespacesListResponse
			require.NoError(t, c.NamespacesList(ctx, &list, &querator.ListOptions{
				Pivot: nsName,
				Limit: 1,
			}))
			require.Equal(t, 1, len(list.Items))
			assert.Equal(t, nsName, list.Items[0].Name)
			assert.Equal(t, "dev", list.Items[0].ApiKeyTag)
		})

		t.Run("CreateWithEmptyAPIKeyTag", func(t *testing.T) {
			d, c, ctx := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
			defer func() {
				d.Shutdown(t)
				tearDown()
			}()

			nsName := random.String("ns-", 10)
			require.NoError(t, c.NamespacesCreate(ctx, &pb.NamespaceInfo{
				Name: nsName,
			}))

			var list pb.NamespacesListResponse
			require.NoError(t, c.NamespacesList(ctx, &list, &querator.ListOptions{
				Pivot: nsName,
				Limit: 1,
			}))
			require.Equal(t, 1, len(list.Items))
			assert.Equal(t, nsName, list.Items[0].Name)
			assert.Empty(t, list.Items[0].ApiKeyTag)
		})

		t.Run("APIKeyTagValidationErrors", func(t *testing.T) {
			d, c, ctx := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
			defer func() {
				d.Shutdown(t)
				tearDown()
			}()

			for _, test := range []struct {
				Name   string
				Tag    string
				Msg    string
				Code   int
			}{
				{
					Name: "UppercaseLetters",
					Tag:  "PROD",
					Msg:  "api_key_tag is invalid; must be lowercase alphanumeric only",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "ContainsSymbol",
					Tag:  "my-tag",
					Msg:  "api_key_tag is invalid; must be lowercase alphanumeric only",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "TooLong",
					Tag:  "thistagistoolong1",
					Msg:  "api_key_tag is invalid; cannot be greater than 16 characters",
					Code: duh.CodeBadRequest,
				},
			} {
				t.Run(test.Name, func(t *testing.T) {
					err := c.NamespacesCreate(ctx, &pb.NamespaceInfo{
						Name:      random.String("ns-", 10),
						ApiKeyTag: test.Tag,
					})
					require.Error(t, err)
					var e duh.Error
					require.ErrorAs(t, err, &e)
					assert.Contains(t, e.Message(), test.Msg)
					assert.Equal(t, test.Code, e.Code())
				})
			}
		})
	})
}

func testNamespacesUpdate(t *testing.T, setup NewStorageFunc, tearDown func()) {
	t.Helper()
	t.Run("NamespacesUpdate", func(t *testing.T) {
		t.Run("UpdateAPIKeyTag", func(t *testing.T) {
			d, c, ctx := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
			defer func() {
				d.Shutdown(t)
				tearDown()
			}()

			nsName := random.String("ns-", 10)
			require.NoError(t, c.NamespacesCreate(ctx, &pb.NamespaceInfo{
				Name: nsName,
			}))

			require.NoError(t, c.NamespacesUpdate(ctx, &pb.NamespaceInfo{
				Name:      nsName,
				ApiKeyTag: "stg",
			}))

			var list pb.NamespacesListResponse
			require.NoError(t, c.NamespacesList(ctx, &list, &querator.ListOptions{
				Pivot: nsName,
				Limit: 1,
			}))
			require.Equal(t, 1, len(list.Items))
			assert.Equal(t, "stg", list.Items[0].ApiKeyTag)
		})

		t.Run("UpdateClearsAPIKeyTag", func(t *testing.T) {
			d, c, ctx := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
			defer func() {
				d.Shutdown(t)
				tearDown()
			}()

			nsName := random.String("ns-", 10)
			require.NoError(t, c.NamespacesCreate(ctx, &pb.NamespaceInfo{
				Name:      nsName,
				ApiKeyTag: "prod",
			}))

			require.NoError(t, c.NamespacesUpdate(ctx, &pb.NamespaceInfo{
				Name: nsName,
			}))

			var list pb.NamespacesListResponse
			require.NoError(t, c.NamespacesList(ctx, &list, &querator.ListOptions{
				Pivot: nsName,
				Limit: 1,
			}))
			require.Equal(t, 1, len(list.Items))
			assert.Empty(t, list.Items[0].ApiKeyTag)
		})

		t.Run("UpdateNonExistentNamespace", func(t *testing.T) {
			d, c, ctx := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
			defer func() {
				d.Shutdown(t)
				tearDown()
			}()

			err := c.NamespacesUpdate(ctx, &pb.NamespaceInfo{
				Name:      "nonexistent-ns-" + random.String("", 5),
				ApiKeyTag: "stg",
			})
			require.Error(t, err)
			var e duh.Error
			require.ErrorAs(t, err, &e)
			assert.Contains(t, e.Message(), "namespace does not exist")
			assert.Equal(t, duh.CodeRequestFailed, e.Code())
		})

		t.Run("UpdateReservedNamespace", func(t *testing.T) {
			d, c, ctx := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
			defer func() {
				d.Shutdown(t)
				tearDown()
			}()

			err := c.NamespacesUpdate(ctx, &pb.NamespaceInfo{
				Name:      "_system",
				ApiKeyTag: "stg",
			})
			require.Error(t, err)
			var e duh.Error
			require.ErrorAs(t, err, &e)
			assert.Contains(t, e.Message(), "namespace name is reserved")
			assert.Equal(t, duh.CodeBadRequest, e.Code())
		})
	})
}
