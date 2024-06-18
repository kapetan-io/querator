package internal_test

import (
	"context"
	"fmt"
	"github.com/kapetan-io/querator/internal"
	"github.com/stretchr/testify/assert"
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

	t.Run("ReadAndWrite", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		messages := writeRandomMessages(t, ctx, store, 10_000)

		var read []*internal.QueueItem
		err := store.Read(ctx, &read, "", 10_000)
		require.NoError(t, err)

		assert.Equal(t, len(messages), len(read))
		for i := range messages {
			assert.NotEmpty(t, read[i].ID)
			assert.Equal(t, messages[i].IsReserved, read[i].IsReserved)
			assert.Equal(t, messages[i].Attempts, read[i].Attempts)
			assert.Equal(t, messages[i].Reference, read[i].Reference)
			assert.Equal(t, messages[i].Encoding, read[i].Encoding)
			assert.Equal(t, messages[i].Kind, read[i].Kind)
			assert.Equal(t, messages[i].Body, read[i].Body)
		}
	})

	t.Run("ReadPivot", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		messages := writeRandomMessages(t, ctx, store, 10_000)
		require.Len(t, messages, 10_000)

		id := messages[1000].ID
		var read []*internal.QueueItem
		err := store.Read(ctx, &read, id, 10)
		require.NoError(t, err)

		assert.Equal(t, 10, len(read))
		for i := range read {
			assert.NotEmpty(t, read[i].ID)
			assert.Equal(t, messages[i+1000].IsReserved, read[i].IsReserved)
			assert.Equal(t, messages[i+1000].Attempts, read[i].Attempts)
			assert.Equal(t, messages[i+1000].Reference, read[i].Reference)
			assert.Equal(t, messages[i+1000].Encoding, read[i].Encoding)
			assert.Equal(t, messages[i+1000].Kind, read[i].Kind)
			assert.Equal(t, messages[i+1000].Body, read[i].Body)
		}
	})

	t.Run("Delete", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		messages := writeRandomMessages(t, ctx, store, 10_000)
		require.Len(t, messages, 10_000)

		// Delete 1_000 messages
		err := store.Delete(ctx, messages[0:1_000])
		require.NoError(t, err)

		var read []*internal.QueueItem
		err = store.Read(ctx, &read, "", 10_000)
		require.NoError(t, err)
		assert.Len(t, read, 9_000)

		// Assert the items deleted do not exist
		for _, deleted := range messages[0:1_000] {
			for _, msg := range read {
				if msg.ID == deleted.ID {
					t.Fatalf("Found deleted message %s", deleted.ID)
				}
			}
		}
	})
}

func writeRandomMessages(t *testing.T, ctx context.Context, store internal.Store, count int) []*internal.QueueItem {
	t.Helper()

	var items []*internal.QueueItem
	for i := 0; i < count; i++ {
		items = append(items, &internal.QueueItem{
			Body: []byte(fmt.Sprintf("message-%d", i)),
		})
	}
	err := store.Write(ctx, items)
	require.NoError(t, err)
	return items
}
