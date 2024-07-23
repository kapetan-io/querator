package querator_test

import (
	"github.com/kapetan-io/querator/internal/store"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestQueuesStorage(t *testing.T) {
	bdb := store.BoltDBTesting{Dir: t.TempDir()}

	for _, tc := range []struct {
		Setup    NewStorageFunc
		TearDown func()
		Name     string
	}{
		{
			Name: "BoltDB",
			Setup: func() store.Storage {
				return bdb.Setup(store.BoltOptions{})
			},
			TearDown: func() {
				bdb.Teardown()
			},
		},
		//{
		//	Name: "SurrealDB",
		//},
		//{
		//	Name: "PostgresSQL",
		//},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			testQueuesStorage(t, tc.Setup, tc.TearDown)
		})
	}
}

func testQueuesStorage(t *testing.T, newStore NewStorageFunc, tearDown func()) {
	_store := newStore()
	defer tearDown()
	t.Run("CRUDCompare", func(t *testing.T) {})

	t.Run("CRUD", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d, c, ctx := newDaemon(t, _store, 10*time.Second)
		defer d.Shutdown(t)

		t.Run("Create", func(t *testing.T) {
			require.NoError(t, c.QueueCreate(ctx, &pb.QueueInfo{QueueName: queueName}))
			// TODO: Validate CreatedAt
		})
		t.Run("Get", func(t *testing.T) {})
		t.Run("List", func(t *testing.T) {})
		t.Run("Update", func(t *testing.T) {})
		t.Run("Delete", func(t *testing.T) {})
	})

	t.Run("GetWithPivot", func(t *testing.T) {})
	t.Run("ListMoreThanAvailable", func(t *testing.T) {})
	t.Run("ListLessThanAvailable", func(t *testing.T) {})
	t.Run("ListWithPivot", func(t *testing.T) {})
	t.Run("ListIncludePivot", func(t *testing.T) {})
	t.Run("ListIterator", func(t *testing.T) {})
	t.Run("DeleteAlreadyDeletedIsOk", func(t *testing.T) {})
}
