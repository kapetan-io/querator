package store_test

import (
	"context"
	"fmt"
	"github.com/kapetan-io/querator/store"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

func TestQueue(t *testing.T) {

	testCases := []struct {
		Name string
		New  func() (store.Queue, error)
	}{
		{
			Name: "BuntDB",
			New: func() (store.Queue, error) {
				return store.NewBuntStorage(store.BuntOptions{}).NewQueue(store.QueueOptions{Name: "test-queue"})
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

type NewFunc func() (store.Queue, error)

func testSuite(t *testing.T, newStore NewFunc) {

	t.Run("QueueItemCompare", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		s, err := newStore()
		defer func() { _ = s.Close(context.Background()) }()
		require.NoError(t, err)

		now := time.Now().UTC()
		var items []*store.Item
		items = append(items, &store.Item{
			IsReserved:      true,
			DeadDeadline:    now.Add(100_000 * time.Minute),
			ReserveDeadline: now.Add(3_000 * time.Minute),
			Attempts:        5,
			Reference:       "rainbow@dash.com",
			Encoding:        "rainbows",
			Kind:            "20% cooler",
			Body: []byte("I mean... have I changed? Same sleek body. Same " +
				"flowing mane. Same spectacular hooves. Nope, I'm still awesome"),
		})
		items = append(items, &store.Item{
			IsReserved:      false,
			DeadDeadline:    now.Add(1_000_000 * time.Minute),
			ReserveDeadline: now.Add(3_000 * time.Minute),
			Attempts:        10_000,
			Reference:       "rarity@dash.com",
			Encoding:        "beauty",
			Kind:            "sparkles",
			Body:            []byte("Whining? I am not whining, I am complaining"),
		})

		err = s.Write(ctx, items)
		require.NoError(t, err)

		var reads []*store.Item
		err = s.Read(ctx, &reads, "", 2)
		require.NoError(t, err)

		assert.Equal(t, len(reads), len(items))
		assert.True(t, reads[0].Compare(items[0]), "%+v != %+v", *reads[0], *items[0])
		assert.True(t, reads[1].Compare(items[1]), "%+v != %+v", *reads[1], *items[1])

		// Ensure all the fields are indeed the same
		assert.Equal(t, reads[0].ID, items[0].ID)
		assert.Equal(t, reads[0].IsReserved, items[0].IsReserved)
		assert.Equal(t, 0, reads[0].DeadDeadline.Compare(items[0].DeadDeadline))
		assert.Equal(t, 0, reads[0].ReserveDeadline.Compare(items[0].ReserveDeadline))
		assert.Equal(t, reads[0].Attempts, items[0].Attempts)
		assert.Equal(t, reads[0].Reference, items[0].Reference)
		assert.Equal(t, reads[0].Encoding, items[0].Encoding)
		assert.Equal(t, reads[0].Kind, items[0].Kind)
		assert.Equal(t, reads[0].Body, items[0].Body)

		assert.Equal(t, reads[1].ID, items[1].ID)
		assert.Equal(t, reads[1].IsReserved, items[1].IsReserved)
		assert.Equal(t, 0, reads[1].DeadDeadline.Compare(items[1].DeadDeadline))
		assert.Equal(t, 0, reads[1].ReserveDeadline.Compare(items[1].ReserveDeadline))
		assert.Equal(t, reads[1].Attempts, items[1].Attempts)
		assert.Equal(t, reads[1].Reference, items[1].Reference)
		assert.Equal(t, reads[1].Encoding, items[1].Encoding)
		assert.Equal(t, reads[1].Kind, items[1].Kind)
		assert.Equal(t, reads[1].Body, items[1].Body)

		cmp := &store.Item{
			IsReserved:      false,
			DeadDeadline:    now.Add(1_000_000 * time.Minute),
			ReserveDeadline: now.Add(2_000 * time.Minute),
			Attempts:        100_000,
			Reference:       "discord@dash.com",
			Encoding:        "Lord of Chaos",
			Kind:            "Captain Good guy",
			Body:            []byte("Make sense? Oh, what fun is there in making sense?"),
		}

		assert.True(t, cmp.Compare(&store.Item{
			IsReserved:      false,
			DeadDeadline:    now.Add(1_000_000 * time.Minute),
			ReserveDeadline: now.Add(2_000 * time.Minute),
			Attempts:        100_000,
			Reference:       "discord@dash.com",
			Encoding:        "Lord of Chaos",
			Kind:            "Captain Good guy",
			Body:            []byte("Make sense? Oh, what fun is there in making sense?"),
		}))
		cpy := *cmp
		cpy.IsReserved = true
		assert.False(t, cmp.Compare(&cpy))
		cpy = *cmp
		cpy.DeadDeadline = time.Now().UTC()
		assert.False(t, cmp.Compare(&cpy))
		cpy = *cmp
		cpy.ReserveDeadline = time.Now().UTC()
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

			var read []*store.Item
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
			var more []*store.Item
			err = s.Read(ctx, &more, "", 20_000)
			require.NoError(t, err)
			assert.Equal(t, 10_000, len(more))
			assert.True(t, items[0].Compare(more[0]),
				"%+v != %+v", *items[0], *more[0])
			assert.True(t, items[10_000-1].Compare(more[len(more)-1]),
				"%+v != %+v", *items[10_000-1], *more[len(more)-1])

			// Ensure if we limit the read, we get only the amount requested
			var limit []*store.Item
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
			var read []*store.Item
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

			// The read includes the pivot
			item := read[9]
			read = read[:0]
			err = s.Read(ctx, &read, item.ID, 1)
			require.NoError(t, err)

			require.Equal(t, 1, len(read))
			assert.Equal(t, item.ID, read[0].ID)
			assert.True(t, read[0].Compare(item), "%+v != %+v", *read[0], item)

			// Pivot through more pages and ensure the pivot allows us to page through items
			read = read[0:]
			err = s.Read(ctx, &read, item.ID, 10)
			require.NoError(t, err)

			item = read[9]
			read = read[:0]
			err = s.Read(ctx, &read, item.ID, 10)
			require.NoError(t, err)

			// The last item on the last page, should match the items written
			assert.True(t, items[1026].Compare(read[9]),
				"%+v != %+v", *items[1026], *read[9])
		})

		t.Run("Reserve", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			s, err := newStore()
			defer func() { _ = s.Close(context.Background()) }()
			require.NoError(t, err)

			items := writeRandomItems(t, ctx, s, 10_000)
			require.Len(t, items, 10_000)

			// Reserve 10 items
			var reserved []*store.Item
			expire := time.Now().UTC().Add(2_000 * time.Minute)

			err = s.Reserve(ctx, &reserved, store.ReserveOptions{ReserveDeadline: expire, Limit: 10})
			require.NoError(t, err)
			assert.Equal(t, 10, len(reserved))

			// Ensure the items reserved are marked as reserved in the database
			var read []*store.Item
			err = s.Read(ctx, &read, "", 10_000)
			require.NoError(t, err)

			for i := range reserved {
				assert.Equal(t, read[i].ID, reserved[i].ID)
				assert.Equal(t, true, read[i].IsReserved)
				assert.True(t, expire.Equal(read[i].ReserveDeadline))
			}

			// Reserve some more items
			var secondReserve []*store.Item
			err = s.Reserve(ctx, &secondReserve, store.ReserveOptions{ReserveDeadline: expire, Limit: 10})
			require.NoError(t, err)

			read = read[:0]
			err = s.Read(ctx, &read, "", 10_000)
			require.NoError(t, err)

			var combined []*store.Item
			combined = append(combined, reserved...)
			combined = append(combined, secondReserve...)

			err = s.Read(ctx, &read, "", 10_000)
			require.NoError(t, err)
			assert.NotEqual(t, reserved[0].ID, secondReserve[0].ID)
			assert.Equal(t, combined[0].ID, read[0].ID)
			require.Equal(t, 20, len(combined))
			require.Equal(t, 20_000, len(read))

			// Ensure all the items reserved are marked as reserved in the database
			for i := range combined {
				assert.Equal(t, read[i].ID, combined[i].ID)
				assert.Equal(t, true, read[i].IsReserved)
				assert.True(t, expire.Equal(read[i].ReserveDeadline))
			}
		})

		t.Run("Stats", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			s, err := newStore()
			defer func() { _ = s.Close(context.Background()) }()
			require.NoError(t, err)

			items := writeRandomItems(t, ctx, s, 10_000)
			require.Len(t, items, 10_000)

			// Reserve 100 items
			var reserved []*store.Item
			expire := time.Now().UTC().Add(time.Minute)

			err = s.Reserve(ctx, &reserved, store.ReserveOptions{ReserveDeadline: expire, Limit: 100})
			require.NoError(t, err)
			assert.Equal(t, 100, len(reserved))

			var stats store.Stats
			require.NoError(t, s.Stats(ctx, &stats))

			// Ensure stats are accurate
			assert.Equal(t, 10_000, stats.Total)
			assert.Equal(t, 100, stats.TotalReserved)
			assert.True(t, stats.AverageReservedAge < time.Minute)
			assert.NotEmpty(t, stats.AverageAge)

			t.Logf("total: %d average-age: %s reserved %d average-reserved: %s",
				stats.Total, stats.AverageAge, stats.TotalReserved, stats.AverageReservedAge)
		})

		t.Run("Get One", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			s, err := newStore()
			defer func() { _ = s.Close(context.Background()) }()
			require.NoError(t, err)

			items := writeRandomItems(t, ctx, s, 10_000)
			require.Len(t, items, 10_000)

			// Ensure Read() can fetch one item using pivot, and the item returned is the pivot
			item := items[1000]
			var read []*store.Item
			err = s.Read(ctx, &read, item.ID, 1)
			require.NoError(t, err)

			require.Equal(t, 1, len(read))
			assert.Equal(t, item.ID, read[0].ID)
			assert.True(t, read[0].Compare(item), "%+v != %+v", *read[0], item)
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
			err = s.Delete(ctx, store.CollectIDs(items[0:1_000]))
			require.NoError(t, err)

			var read []*store.Item
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

func writeRandomItems(t *testing.T, ctx context.Context, s store.Queue, count int) []*store.Item {
	t.Helper()
	expire := time.Now().UTC().Add(random.Duration(time.Minute))

	var items []*store.Item
	for i := 0; i < count; i++ {
		items = append(items, &store.Item{
			DeadDeadline: expire,
			Attempts:     rand.Intn(10),
			Reference:    random.String("ref-", 10),
			Encoding:     random.String("enc-", 10),
			Kind:         random.String("kind-", 10),
			Body:         []byte(fmt.Sprintf("message-%d", i)),
		})
	}
	err := s.Write(ctx, items)
	require.NoError(t, err)
	return items
}
