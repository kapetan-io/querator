package store_test

import (
	"context"
	"fmt"
	"github.com/kapetan-io/querator/store"
	"github.com/kapetan-io/querator/transport"
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
		var items []*transport.Item
		items = append(items, &transport.Item{
			IsReserved:      true,
			DeadDeadline:    now.Add(100_000 * time.Minute),
			ReserveDeadline: now.Add(3_000 * time.Minute),
			Attempts:        5,
			Reference:       "rainbow@dash.com",
			Encoding:        "rainbows",
			Kind:            "20% cooler",
			Payload: []byte("I mean... have I changed? Same sleek body. Same " +
				"flowing mane. Same spectacular hooves. Nope, I'm still awesome"),
		})
		items = append(items, &transport.Item{
			IsReserved:      false,
			DeadDeadline:    now.Add(1_000_000 * time.Minute),
			ReserveDeadline: now.Add(3_000 * time.Minute),
			Attempts:        10_000,
			Reference:       "rarity@dash.com",
			Encoding:        "beauty",
			Kind:            "sparkles",
			Payload:         []byte("Whining? I am not whining, I am complaining"),
		})

		err = s.Write(ctx, items)
		require.NoError(t, err)

		var reads []*transport.Item
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
		assert.Equal(t, reads[0].Payload, items[0].Payload)

		assert.Equal(t, reads[1].ID, items[1].ID)
		assert.Equal(t, reads[1].IsReserved, items[1].IsReserved)
		assert.Equal(t, 0, reads[1].DeadDeadline.Compare(items[1].DeadDeadline))
		assert.Equal(t, 0, reads[1].ReserveDeadline.Compare(items[1].ReserveDeadline))
		assert.Equal(t, reads[1].Attempts, items[1].Attempts)
		assert.Equal(t, reads[1].Reference, items[1].Reference)
		assert.Equal(t, reads[1].Encoding, items[1].Encoding)
		assert.Equal(t, reads[1].Kind, items[1].Kind)
		assert.Equal(t, reads[1].Payload, items[1].Payload)

		cmp := &transport.Item{
			IsReserved:      false,
			DeadDeadline:    now.Add(1_000_000 * time.Minute),
			ReserveDeadline: now.Add(2_000 * time.Minute),
			Attempts:        100_000,
			Reference:       "discord@dash.com",
			Encoding:        "Lord of Chaos",
			Kind:            "Captain Good guy",
			Payload:         []byte("Make sense? Oh, what fun is there in making sense?"),
		}

		assert.True(t, cmp.Compare(&transport.Item{
			IsReserved:      false,
			DeadDeadline:    now.Add(1_000_000 * time.Minute),
			ReserveDeadline: now.Add(2_000 * time.Minute),
			Attempts:        100_000,
			Reference:       "discord@dash.com",
			Encoding:        "Lord of Chaos",
			Kind:            "Captain Good guy",
			Payload:         []byte("Make sense? Oh, what fun is there in making sense?"),
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
		cpy.Payload = []byte("Debugging is like being the detective in a crime movie where you are also the murderer")
		assert.False(t, cmp.Compare(&cpy))
	})

	t.Run("ReadAndWrite", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		s, err := newStore()
		defer func() { _ = s.Close(context.Background()) }()
		require.NoError(t, err)

		items := writeRandomItems(t, ctx, s, 10_000)

		var read []*transport.Item
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
			assert.Equal(t, items[i].Payload, read[i].Payload)
		}

		// Ensure if we ask for more than is available, we only get what is in the db.
		t.Run("AskForMoreThanIsAvailable", func(t *testing.T) {
			var more []*transport.Item
			err = s.Read(ctx, &more, "", 20_000)
			require.NoError(t, err)
			assert.Equal(t, 10_000, len(more))
			assert.True(t, items[0].Compare(more[0]),
				"%+v != %+v", *items[0], *more[0])
			assert.True(t, items[10_000-1].Compare(more[len(more)-1]),
				"%+v != %+v", *items[10_000-1], *more[len(more)-1])

			// Ensure if we limit the read, we get only the amount requested
			var limit []*transport.Item
			err = s.Read(ctx, &limit, "", 1_000)
			require.NoError(t, err)
			assert.Equal(t, 1_000, len(limit))
			assert.True(t, items[0].Compare(limit[0]),
				"%+v != %+v", *items[0], *limit[0])
			assert.True(t, items[1_000-1].Compare(limit[len(limit)-1]),
				"%+v != %+v", *items[1_000-1], *limit[len(limit)-1])
		})

		t.Run("AskForLessThanIsAvailable", func(t *testing.T) {
			var less []*transport.Item
			require.NoError(t, s.Read(ctx, &less, "", 10))
			assert.Equal(t, 10, len(less))
		})
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
		var read []*transport.Item
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
			assert.Equal(t, items[i+1000].Payload, read[i].Payload)
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

	t.Run("Produce", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		s, err := newStore()
		defer func() { _ = s.Close(context.Background()) }()
		require.NoError(t, err)

		expire := time.Now().UTC().Add(random.Duration(time.Minute))
		var items []*transport.Item
		for i := 0; i < 10; i++ {
			items = append(items, &transport.Item{
				DeadDeadline: expire,
				Attempts:     rand.Intn(10),
				Reference:    random.String("ref-", 10),
				Encoding:     random.String("enc-", 10),
				Kind:         random.String("kind-", 10),
				Payload:      []byte(fmt.Sprintf("message-%d", i)),
			})
		}

		batch := store.Batch[transport.ProduceRequest]{
			Requests: []transport.ProduceRequest{{Items: items}},
		}

		// Produce the items
		require.NoError(t, s.Produce(ctx, batch))

		// Ensure the items produced are in the database
		var read []*transport.Item
		err = s.Read(ctx, &read, "", 10_000)
		require.NoError(t, err)
		require.Len(t, items, 10)

		assert.Equal(t, len(items), len(read))
		for i := range items {
			assert.NotEmpty(t, read[i].ID)
			assert.Equal(t, items[i].IsReserved, read[i].IsReserved)
			assert.Equal(t, items[i].Attempts, read[i].Attempts)
			assert.Equal(t, items[i].Reference, read[i].Reference)
			assert.Equal(t, items[i].Encoding, read[i].Encoding)
			assert.Equal(t, items[i].Kind, read[i].Kind)
			assert.Equal(t, items[i].Payload, read[i].Payload)
		}
	})

	t.Run("Reserve", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		s, err := newStore()
		defer func() { _ = s.Close(context.Background()) }()
		require.NoError(t, err)

		items := writeRandomItems(t, ctx, s, 10_000)
		require.Len(t, items, 10_000)

		expire := time.Now().UTC().Add(2_000 * time.Minute)
		var reserved, secondReserve, lastReserved []*transport.Item
		var read []*transport.Item

		// Reserve 10 items in one request
		t.Run("TenItems", func(t *testing.T) {
			batch := store.Batch[transport.ReserveRequest]{
				Requests:       []transport.ReserveRequest{{NumRequested: 10}},
				TotalRequested: 10,
			}

			require.NoError(t, s.Reserve(ctx, batch, store.ReserveOptions{ReserveDeadline: expire}))
			reserved = batch.Requests[0].Items
			require.Equal(t, 10, len(reserved))

			// Ensure the items reserved are marked as reserved in the database
			err = s.Read(ctx, &read, "", 10_000)
			require.NoError(t, err)

			for i := range reserved {
				assert.Equal(t, read[i].ID, reserved[i].ID)
				assert.Equal(t, true, read[i].IsReserved)
				assert.True(t, expire.Equal(read[i].ReserveDeadline))
			}
		})

		// Reserve 10 more items
		t.Run("AnotherTenItems", func(t *testing.T) {
			batch := store.Batch[transport.ReserveRequest]{
				Requests:       []transport.ReserveRequest{{NumRequested: 10}},
				TotalRequested: 10,
			}

			require.NoError(t, s.Reserve(ctx, batch, store.ReserveOptions{ReserveDeadline: expire}))
			secondReserve = batch.Requests[0].Items

			read = read[:0]
			err = s.Read(ctx, &read, "", 10_000)
			require.NoError(t, err)

			var combined []*transport.Item
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
			lastReserved = read[len(combined):]
		})

		t.Run("DistributeNumRequested", func(t *testing.T) {
			// Ensure our requests are of different requested reservations
			batch := store.Batch[transport.ReserveRequest]{
				Requests: []transport.ReserveRequest{
					{NumRequested: 5},
					{NumRequested: 10},
					{NumRequested: 20},
				},
				TotalRequested: 35,
			}

			require.NoError(t, s.Reserve(ctx, batch, store.ReserveOptions{ReserveDeadline: expire}))

			assert.Equal(t, 5, batch.Requests[0].NumRequested)
			assert.Equal(t, 5, len(batch.Requests[0].Items))
			assert.Equal(t, 10, batch.Requests[1].NumRequested)
			assert.Equal(t, 10, len(batch.Requests[1].Items))
			assert.Equal(t, 20, batch.Requests[2].NumRequested)
			assert.Equal(t, 20, len(batch.Requests[2].Items))

			// Ensure all the items are reserved
			read = read[:0]
			require.NoError(t, s.Read(ctx, &read, lastReserved[0].ID, 10_000))
			require.Equal(t, 9_980, len(read))

			for _, item := range read[0:35] {
				// Ensure the item is reserved
				require.Equal(t, true, item.IsReserved)
				// Find the reserved item in the batch request
				if !findInBatch(t, batch, item.ID) {
					t.Fatalf("unable to find item '%s' in any batch reserve request", item.ID)
				}
			}
		})

		t.Run("NotEnoughItemsDistribution", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			s, err := newStore()
			defer func() { _ = s.Close(context.Background()) }()
			require.NoError(t, err)

			items := writeRandomItems(t, ctx, s, 23)
			require.Len(t, items, 23)

			expire := time.Now().UTC().Add(2_000 * time.Minute)

			batch := store.Batch[transport.ReserveRequest]{
				Requests: []transport.ReserveRequest{
					{NumRequested: 20},
					{NumRequested: 6},
					{NumRequested: 1},
				},
				TotalRequested: 27,
			}

			// Reserve() should fairly distribute items across all requests
			require.NoError(t, s.Reserve(ctx, batch, store.ReserveOptions{ReserveDeadline: expire}))
			assert.Equal(t, 20, batch.Requests[0].NumRequested)
			assert.Equal(t, 16, len(batch.Requests[0].Items))
			assert.Equal(t, 6, batch.Requests[1].NumRequested)
			assert.Equal(t, 6, len(batch.Requests[1].Items))
			assert.Equal(t, 1, batch.Requests[2].NumRequested)
			assert.Equal(t, 1, len(batch.Requests[2].Items))
		})

		t.Run("NoItemsToDistribute", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			s, err := newStore()
			defer func() { _ = s.Close(context.Background()) }()
			require.NoError(t, err)

			expire := time.Now().UTC().Add(2_000 * time.Minute)
			batch := store.Batch[transport.ReserveRequest]{
				Requests: []transport.ReserveRequest{
					{NumRequested: 20},
					{NumRequested: 6},
					{NumRequested: 1},
				},
				TotalRequested: 27,
			}

			// Reserve() should return no items and without error
			require.NoError(t, s.Reserve(ctx, batch, store.ReserveOptions{ReserveDeadline: expire}))
			assert.Equal(t, 20, batch.Requests[0].NumRequested)
			assert.Equal(t, 0, len(batch.Requests[0].Items))
			assert.Equal(t, 6, batch.Requests[1].NumRequested)
			assert.Equal(t, 0, len(batch.Requests[1].Items))
			assert.Equal(t, 1, batch.Requests[2].NumRequested)
			assert.Equal(t, 0, len(batch.Requests[2].Items))
		})
	})

	t.Run("Complete", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		s, err := newStore()
		defer func() { _ = s.Close(context.Background()) }()
		require.NoError(t, err)

		expire := time.Now().UTC().Add(random.Duration(time.Minute))
		var items []*transport.Item
		for i := 0; i < 10; i++ {
			items = append(items, &transport.Item{
				DeadDeadline: expire,
				Attempts:     rand.Intn(10),
				Reference:    random.String("ref-", 10),
				Encoding:     random.String("enc-", 10),
				Kind:         random.String("kind-", 10),
				Payload:      []byte(fmt.Sprintf("message-%d", i)),
			})
		}

		// Produce the items
		require.NoError(t, s.Produce(ctx, store.Batch[transport.ProduceRequest]{
			Requests: []transport.ProduceRequest{{Items: items}},
		}))

		// Reserve the items
		reserve := store.Batch[transport.ReserveRequest]{
			Requests:       []transport.ReserveRequest{{NumRequested: 9}},
			TotalRequested: 9,
		}

		require.NoError(t, s.Reserve(ctx, reserve, store.ReserveOptions{ReserveDeadline: expire}))

		ids := store.CollectIDs(reserve.Requests[0].Items)

		complete := store.Batch[transport.CompleteRequest]{
			Requests: []transport.CompleteRequest{{Ids: ids}},
		}

		var read []*transport.Item
		t.Run("Success", func(t *testing.T) {
			// Complete the items
			require.NoError(t, s.Complete(ctx, complete))
			require.NoError(t, complete.Requests[0].Err)

			// Ensure the items completed are not in the database
			require.NoError(t, s.Read(ctx, &read, "", 10))
			assert.Equal(t, 1, len(read))
		})

		t.Run("NotReserved", func(t *testing.T) {
			// Attempt to complete an item that has not been reserved
			complete = store.Batch[transport.CompleteRequest]{
				Requests: []transport.CompleteRequest{{Ids: []string{read[0].ID}}},
			}

			require.NoError(t, s.Complete(ctx, complete))
			require.Error(t, complete.Requests[0].Err)
			assert.Contains(t, complete.Requests[0].Err.Error(), "item(s) cannot be completed")
			assert.Contains(t, complete.Requests[0].Err.Error(), "is not marked as reserved")
		})

		read = read[:0]
		t.Run("InvalidID", func(t *testing.T) {
			require.NoError(t, s.Read(ctx, &read, "", 10))
			assert.Equal(t, 1, len(read))

			complete = store.Batch[transport.CompleteRequest]{
				Requests: []transport.CompleteRequest{
					{Ids: []string{"invalid-id"}},
					{Ids: []string{"another-invalid-id"}},
				},
			}

			require.NoError(t, s.Complete(ctx, complete))
			require.Error(t, complete.Requests[0].Err)
			assert.Equal(t, "invalid storage id; 'invalid-id': expected format <queue_name>~<storage_id>",
				complete.Requests[0].Err.Error())
			require.Error(t, complete.Requests[1].Err)
			assert.Equal(t, "invalid storage id; 'another-invalid-id': expected format <queue_name>~<storage_id>",
				complete.Requests[1].Err.Error())
		})
	})

	// TODO: Fix Stats, AverageAge and AverageReservedAge is wrong after item changes
	//t.Run("Stats", func(t *testing.T) {
	//	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	//	defer cancel()
	//
	//	s, err := newStore()
	//	defer func() { _ = s.Close(context.Background()) }()
	//	require.NoError(t, err)
	//
	//	items := writeRandomItems(t, ctx, s, 10_000)
	//	require.Len(t, items, 10_000)
	//
	//	// Reserve 100 items
	//	expire := time.Now().UTC().Add(2_000 * time.Minute)
	//	batch := store.Batch[transport.ItemBatch]{
	//		Requests: []*transport.ItemBatch{{}},
	//		TotalRequested:   100,
	//	}
	//
	//	err = s.Reserve(ctx, batch, store.ReserveOptions{ReserveDeadline: expire})
	//	require.NoError(t, err)
	//	assert.Equal(t, 100, len(batch.Requests[0].Items))
	//
	//	var stats store.Stats
	//	require.NoError(t, s.Stats(ctx, &stats))
	//
	//	// Ensure stats are accurate
	//	assert.Equal(t, 10_000, stats.Total)
	//	assert.Equal(t, 100, stats.TotalReserved)
	//	assert.True(t, stats.AverageReservedAge < time.Minute)
	//	assert.NotEmpty(t, stats.AverageAge)
	//
	//	t.Logf("total: %d average-age: %s reserved %d average-reserved: %s",
	//		stats.Total, stats.AverageAge, stats.TotalReserved, stats.AverageReservedAge)
	//})

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
		var read []*transport.Item
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

		var read []*transport.Item
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
}

func writeRandomItems(t *testing.T, ctx context.Context, s store.Queue, count int) []*transport.Item {
	t.Helper()
	expire := time.Now().UTC().Add(random.Duration(time.Minute))

	var items []*transport.Item
	for i := 0; i < count; i++ {
		items = append(items, &transport.Item{
			DeadDeadline: expire,
			Attempts:     rand.Intn(10),
			Reference:    random.String("ref-", 10),
			Encoding:     random.String("enc-", 10),
			Kind:         random.String("kind-", 10),
			Payload:      []byte(fmt.Sprintf("message-%d", i)),
		})
	}
	err := s.Write(ctx, items)
	require.NoError(t, err)
	return items
}

func findInBatch(t *testing.T, batch store.Batch[transport.ReserveRequest], id string) bool {
	t.Helper()

	for _, item := range batch.Requests {
		for _, idItem := range item.Items {
			if idItem.ID == id {
				return true
			}
		}
	}
	return false
}
