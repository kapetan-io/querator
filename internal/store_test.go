package internal_test

import (
	"context"
	"fmt"
	"github.com/kapetan-io/querator/internal"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestStorage(t *testing.T) {

	testCases := []struct {
		Name string
		New  func() (internal.Store, error)
	}{
		{
			Name: "BuntDB",
			New: func() (internal.Store, error) {
				return internal.NewBuntStore(internal.BuntOptions{})
			},
		},
		//{
		//	Name: "PostgresSQL",
		//},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			store, err := tc.New()
			require.NoError(t, err)
			testSuite(t, store)
		})
	}
}

func testSuite(t *testing.T, store internal.Store) {

	t.Run("Read/Write", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		var write []*internal.QueueItem
		for i := 0; i < 10; i++ {
			write = append(write, &internal.QueueItem{
				Body: []byte(fmt.Sprintf("item-%d", i)),
			})
		}
		err := store.WriteItems(ctx, write)
		require.NoError(t, err)

		var read []*internal.QueueItem
		err = store.ReadItems(ctx, read, "", 10)
		require.NoError(t, err)

		t.Logf("%+v", read)
	})
}
