package querator_test

import (
	"context"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/tackle/random"
	"os"
	"testing"
)

//func TestBrokenStorage(t *testing.T) {
//	var queueName = random.String("queue-", 10)
//	opts := &store.MockOptions{}
//	newStore := func() store.Storage {
//		return store.NewMockStorage(opts)
//	}
//
//	d, c, ctx := newDaemon(t, newStore, 10*time.Second)
//	defer d.Shutdown(t)
//
//	opts.Methods["Queue.Produce"] = func(args []any) error {
//		return errors.New("unknown storage error")
//	}
//
//	// TODO: QueueCreate should create a queue in storage
//	require.NoError(t, c.QueueCreate(ctx, &pb.QueueOptions{Name: queueName}))
//
//}

type NewStorageFunc func() store.Storage

func TestStorage(t *testing.T) {
	var dir string

	for _, tc := range []struct {
		Setup    NewStorageFunc
		TearDown func()
		Name     string
	}{
		{
			Name: "BoltDB",
			Setup: func() store.Storage {
				dir = random.String("test-data-", 10)
				if err := os.Mkdir(dir, 0777); err != nil {
					panic(err)
				}
				return store.NewBoltStorage(store.BoltOptions{
					StorageDir: dir,
				})
			},
			TearDown: func() {
				if err := os.RemoveAll(dir); err != nil {
					panic(err)
				}
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
			testStorage(t, tc.Setup, tc.TearDown)
		})
	}
}

func testStorage(t *testing.T, setup NewStorageFunc, tearDown func()) {
	s := setup()
	defer func() {
		_ = s.Close(context.Background())
		tearDown()
	}()

	defer func() { _ = s.Close(context.Background()) }()

	t.Run("SetAndListCompare", func(t *testing.T) {
		//ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		//defer cancel()
		//
		//queueName := random.String("queue-", 10)
		//set := store.QueueInfo{Name: queueName}

		// TODO: Use the client and start testing <--- NEXT
		// Set
		//require.NoError(t, q.Set(ctx, set))

		//var get store.QueueInfo
		//require.NoError(t, q.Get(ctx, queueName, &get))
		//
		//assert.Equal(t, set.Name, get.Name)
	})
}
