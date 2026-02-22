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
					Msg:  "namespace name is reserved; names starting with '_' are reserved",
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
			} {
				t.Run(test.Name, func(t *testing.T) {
					err := c.NamespacesCreate(ctx, test.Req)
					require.Error(t, err)
					var e duh.Error
					require.True(t, errors.As(err, &e))
					assert.Equal(t, test.Msg, e.Message())
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
			} {
				t.Run(test.Name, func(t *testing.T) {
					err := c.NamespacesDelete(ctx, test.Req)
					require.Error(t, err)
					var e duh.Error
					require.True(t, errors.As(err, &e))
					assert.Equal(t, test.Msg, e.Message())
					assert.Equal(t, test.Code, e.Code())
				})
			}
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
