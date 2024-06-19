package store_test

import (
	"context"
	"fmt"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestStorage(t *testing.T) {

	testCases := []struct {
		Name string
		New  func() (store.QueueStorage, error)
	}{
		{
			Name: "BuntDB",
			New: func() (store.QueueStorage, error) {
				return store.NewBuntStore(store.BuntOptions{})
			},
		},
		//{
		//	Name: "PostgresSQL",
		//},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			testSuite(t, tc.New)
		})
	}
}

type NewFunc func() (store.QueueStorage, error)

func testSuite(t *testing.T, newStore NewFunc) {

	t.Run("QueueItemCompare", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		s, err := newStore()
		defer func() { _ = s.Close(context.Background()) }()
		require.NoError(t, err)

		now := time.Now()
		var items []*store.QueueItem
		items = append(items, &store.QueueItem{
			IsReserved: true,
			ExpireAt:   now,
			Attempts:   5,
			Reference:  "rainbow@dash.com",
			Encoding:   "rainbows",
			Kind:       "20% cooler",
			Body: []byte("I mean... have I changed? Same sleek body. Same " +
				"flowing mane. Same spectacular hooves. Nope, I'm still awesome"),
		})
		items = append(items, &store.QueueItem{
			IsReserved: false,
			ExpireAt:   now.Add(1_000_000 * time.Minute),
			Attempts:   10_000,
			Reference:  "rarity@dash.com",
			Encoding:   "beauty",
			Kind:       "sparkles",
			Body:       []byte("Whining? I am not whining, I am complaining"),
		})

		err = s.Write(ctx, items)
		require.NoError(t, err)

		var reads []*store.QueueItem
		err = s.Read(ctx, &reads, "", 2)
		require.NoError(t, err)

		assert.Equal(t, len(reads), len(items))
		assert.True(t, reads[0].Compare(items[0]), "%+v != %+v", *reads[0], *items[0])
		assert.True(t, reads[1].Compare(items[1]), "%+v != %+v", *reads[1], *items[1])

		// Ensure all the fields are indeed the same
		assert.Equal(t, reads[0].ID, items[0].ID)
		assert.Equal(t, reads[0].IsReserved, items[0].IsReserved)
		assert.Equal(t, 0, reads[0].ExpireAt.Compare(items[0].ExpireAt))
		assert.Equal(t, reads[0].Attempts, items[0].Attempts)
		assert.Equal(t, reads[0].Reference, items[0].Reference)
		assert.Equal(t, reads[0].Encoding, items[0].Encoding)
		assert.Equal(t, reads[0].Kind, items[0].Kind)
		assert.Equal(t, reads[0].Body, items[0].Body)

		assert.Equal(t, reads[1].ID, items[1].ID)
		assert.Equal(t, reads[1].IsReserved, items[1].IsReserved)
		assert.Equal(t, 0, reads[1].ExpireAt.Compare(items[1].ExpireAt))
		assert.Equal(t, reads[1].Attempts, items[1].Attempts)
		assert.Equal(t, reads[1].Reference, items[1].Reference)
		assert.Equal(t, reads[1].Encoding, items[1].Encoding)
		assert.Equal(t, reads[1].Kind, items[1].Kind)
		assert.Equal(t, reads[1].Body, items[1].Body)

		cmp := &store.QueueItem{
			IsReserved: false,
			ExpireAt:   now.Add(1_000_000 * time.Minute),
			Attempts:   100_000,
			Reference:  "discord@dash.com",
			Encoding:   "Lord of Chaos",
			Kind:       "Captain Good guy",
			Body:       []byte("Make sense? Oh, what fun is there in making sense?"),
		}

		assert.True(t, cmp.Compare(&store.QueueItem{
			IsReserved: false,
			ExpireAt:   now.Add(1_000_000 * time.Minute),
			Attempts:   100_000,
			Reference:  "discord@dash.com",
			Encoding:   "Lord of Chaos",
			Kind:       "Captain Good guy",
			Body:       []byte("Make sense? Oh, what fun is there in making sense?"),
		}))
		cpy := *cmp
		cpy.IsReserved = true
		assert.False(t, cmp.Compare(&cpy))
		cpy = *cmp
		cpy.ExpireAt = time.Now()
		assert.False(t, cmp.Compare(&cpy))
		cpy = *cmp
		cpy.Attempts = 2
		assert.False(t, cmp.Compare(&cpy))
		cpy = *cmp
		cpy.Reference = ""
		assert.False(t, cmp.Compare(&cpy))
		cpy = *cmp
		cpy.Encoding = ""
		assert.False(t, cmp.Compare(&cpy))
		cpy = *cmp
		cpy.Kind = ""
		assert.False(t, cmp.Compare(&cpy))
		cpy = *cmp
		cpy.Body = []byte("Debugging is like being the detective in a crime movie where you are also the murderer")
		assert.False(t, cmp.Compare(&cpy))

		t.Run("ReadAndWrite", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			s, err := newStore()
			defer func() { _ = s.Close(context.Background()) }()
			require.NoError(t, err)

			items := writeRandomItems(t, ctx, s, 10_000)

			var read []*store.QueueItem
			err = s.Read(ctx, &read, "", 10_000)
			require.NoError(t, err)

			assert.Equal(t, len(items), len(read))
			for i := range items {
				assert.NotEmpty(t, read[i].ID)
				assert.Equal(t, items[i].IsReserved, read[i].IsReserved)
				assert.Equal(t, items[i].Attempts, read[i].Attempts)
				assert.Equal(t, items[i].Reference, read[i].Reference)
				assert.Equal(t, items[i].Encoding, read[i].Encoding)
				assert.Equal(t, items[i].Kind, read[i].Kind)
				assert.Equal(t, items[i].Body, read[i].Body)
			}

			// Ensure if we ask for more than is available, we only get what is in the db.
			var more []*store.QueueItem
			err = s.Read(ctx, &more, "", 20_000)
			require.NoError(t, err)
			assert.Equal(t, 10_000, len(more))
			assert.True(t, items[0].Compare(more[0]),
				"%+v != %+v", *items[0], *more[0])
			assert.True(t, items[10_000-1].Compare(more[len(more)-1]),
				"%+v != %+v", *items[10_000-1], *more[len(more)-1])

			// Ensure if we limit the read, we get only the amount requested
			var limit []*store.QueueItem
			err = s.Read(ctx, &limit, "", 1_000)
			require.NoError(t, err)
			assert.Equal(t, 1_000, len(limit))
			assert.True(t, items[0].Compare(limit[0]),
				"%+v != %+v", *items[0], *limit[0])
			assert.True(t, items[1_000-1].Compare(limit[len(limit)-1]),
				"%+v != %+v", *items[1_000-1], *limit[len(limit)-1])
		})

		t.Run("ReadPivot", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			s, err := newStore()
			defer func() { _ = s.Close(context.Background()) }()
			require.NoError(t, err)

			items := writeRandomItems(t, ctx, s, 10_000)
			require.Len(t, items, 10_000)

			id := items[1000].ID
			var read []*store.QueueItem
			err = s.Read(ctx, &read, id, 10)
			require.NoError(t, err)

			assert.Equal(t, 10, len(read))
			for i := range read {
				assert.NotEmpty(t, read[i].ID)
				assert.Equal(t, items[i+1000].IsReserved, read[i].IsReserved)
				assert.Equal(t, items[i+1000].Attempts, read[i].Attempts)
				assert.Equal(t, items[i+1000].Reference, read[i].Reference)
				assert.Equal(t, items[i+1000].Encoding, read[i].Encoding)
				assert.Equal(t, items[i+1000].Kind, read[i].Kind)
				assert.Equal(t, items[i+1000].Body, read[i].Body)
			}
		})

		t.Run("Delete", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			s, err := newStore()
			defer func() { _ = s.Close(context.Background()) }()
			require.NoError(t, err)

			items := writeRandomItems(t, ctx, s, 10_000)
			require.Len(t, items, 10_000)

			// Delete 1_000 items
			err = s.Delete(ctx, items[0:1_000])
			require.NoError(t, err)

			var read []*store.QueueItem
			err = s.Read(ctx, &read, "", 10_000)
			require.NoError(t, err)
			assert.Equal(t, 9_000, len(read))

			// Assert the items deleted do not exist
			for _, deleted := range items[0:1_000] {
				for _, item := range read {
					if item.ID == deleted.ID {
						t.Fatalf("Found deleted message %s", deleted.ID)
					}
				}
			}
		})
	})
}

func writeRandomItems(t *testing.T, ctx context.Context, s store.QueueStorage, count int) []*store.QueueItem {
	t.Helper()

	var items []*store.QueueItem
	for i := 0; i < count; i++ {
		items = append(items, &store.QueueItem{
			Body: []byte(fmt.Sprintf("message-%d", i)),
		})
	}
	err := s.Write(ctx, items)
	require.NoError(t, err)
	return items
}
