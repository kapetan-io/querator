package store_test

import (
	"context"
	"fmt"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/rand"
	"os"
	"testing"
	"time"
)

type NewStorageFunc func() store.Storage

func TestQueue(t *testing.T) {
	var dir string

	for _, tc := range []struct {
		Setup    NewStorageFunc
		TearDown func()
		Name     string
	}{
		{
			Name: "BuntDB",
			Setup: func() store.Storage {
				return store.NewBuntStorage(store.BuntOptions{})
			},
			TearDown: func() {},
		},
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
		//	Name: "PostgresSQL",
		//},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			testQueue(t, tc.Setup, tc.TearDown)
		})
	}
}

func testQueue(t *testing.T, setup NewStorageFunc, tearDown func()) {
	s := setup()
	defer func() {
		_ = s.Close(context.Background())
		tearDown()
	}()

	t.Run("QueueItemCompare", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		q, err := s.NewQueue(store.QueueInfo{Name: random.String("queue-", 10)})
		defer func() { _ = q.Close(context.Background()) }()
		require.NoError(t, err)

		now := time.Now().UTC()
		var items []*types.Item
		items = append(items, &types.Item{
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
		items = append(items, &types.Item{
			IsReserved:      false,
			DeadDeadline:    now.Add(1_000_000 * time.Minute),
			ReserveDeadline: now.Add(3_000 * time.Minute),
			Attempts:        10_000,
			Reference:       "rarity@dash.com",
			Encoding:        "beauty",
			Kind:            "sparkles",
			Payload:         []byte("Whining? I am not whining, I am complaining"),
		})

		err = q.Add(ctx, items)
		require.NoError(t, err)

		var reads []*types.Item
		err = q.List(ctx, &reads, types.ListOptions{Limit: 2})
		require.NoError(t, err)

		require.Equal(t, len(items), len(reads))
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

		cmp := &types.Item{
			IsReserved:      false,
			DeadDeadline:    now.Add(1_000_000 * time.Minute),
			ReserveDeadline: now.Add(2_000 * time.Minute),
			CreatedAt:       now,
			Attempts:        100_000,
			Reference:       "discord@dash.com",
			Encoding:        "Lord of Chaos",
			Kind:            "Captain Good guy",
			Payload:         []byte("Make sense? Oh, what fun is there in making sense?"),
		}

		assert.True(t, cmp.Compare(&types.Item{
			IsReserved:      false,
			DeadDeadline:    now.Add(1_000_000 * time.Minute),
			ReserveDeadline: now.Add(2_000 * time.Minute),
			CreatedAt:       now,
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

	t.Run("AddAndList", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		q, err := s.NewQueue(store.QueueInfo{Name: random.String("queue-", 10)})
		defer func() { _ = q.Close(context.Background()) }()
		require.NoError(t, err)

		items := writeRandomItems(t, ctx, q, 10_000)

		var read []*types.Item
		err = q.List(ctx, &read, types.ListOptions{Limit: 10_000})
		require.NoError(t, err)

		assert.Equal(t, len(items), len(read))
		for i := range items {
			assert.NotEmpty(t, read[i].ID)
			assert.NotEmpty(t, read[i].CreatedAt)
			assert.Equal(t, items[i].IsReserved, read[i].IsReserved)
			assert.Equal(t, items[i].Attempts, read[i].Attempts)
			assert.Equal(t, items[i].Reference, read[i].Reference)
			assert.Equal(t, items[i].Encoding, read[i].Encoding)
			assert.Equal(t, items[i].Kind, read[i].Kind)
			assert.Equal(t, items[i].Payload, read[i].Payload)
		}

		// Ensure if we ask for more than is available, we only get what is in the db.
		t.Run("AskForMoreThanIsAvailable", func(t *testing.T) {
			var more []*types.Item
			err = q.List(ctx, &more, types.ListOptions{Limit: 20_000})
			require.NoError(t, err)
			assert.Equal(t, 10_000, len(more))
			assert.True(t, items[0].Compare(more[0]),
				"%+v != %+v", *items[0], *more[0])
			assert.True(t, items[10_000-1].Compare(more[len(more)-1]),
				"%+v != %+v", *items[10_000-1], *more[len(more)-1])

			// Ensure if we limit the read, we get only the amount requested
			var limit []*types.Item
			err = q.List(ctx, &limit, types.ListOptions{Limit: 1_000})
			require.NoError(t, err)
			assert.Equal(t, 1_000, len(limit))
			assert.True(t, items[0].Compare(limit[0]),
				"%+v != %+v", *items[0], *limit[0])
			assert.True(t, items[1_000-1].Compare(limit[len(limit)-1]),
				"%+v != %+v", *items[1_000-1], *limit[len(limit)-1])
		})

		t.Run("AskForLessThanIsAvailable", func(t *testing.T) {
			var less []*types.Item
			require.NoError(t, q.List(ctx, &less, types.ListOptions{Limit: 10}))
			assert.Equal(t, 10, len(less))
		})
	})

	t.Run("ReadPivot", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		q, err := s.NewQueue(store.QueueInfo{Name: random.String("queue-", 10)})
		defer func() { _ = q.Close(context.Background()) }()
		require.NoError(t, err)

		items := writeRandomItems(t, ctx, q, 10_000)
		require.Len(t, items, 10_000)

		id := items[1000].ID
		var read []*types.Item
		err = q.List(ctx, &read, types.ListOptions{Pivot: id, Limit: 10})
		require.NoError(t, err)

		assert.Equal(t, 10, len(read))
		for i := range read {
			assert.NotEmpty(t, read[i].ID)
			assert.Equal(t, items[i+1000].ID, read[i].ID)
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
		err = q.List(ctx, &read, types.ListOptions{Pivot: item.ID, Limit: 1})
		require.NoError(t, err)

		require.Equal(t, 1, len(read))
		assert.Equal(t, item.ID, read[0].ID)
		assert.True(t, read[0].Compare(item), "%+v != %+v", *read[0], item)

		// Pivot through more pages and ensure the pivot allows us to page through items
		read = read[0:]
		err = q.List(ctx, &read, types.ListOptions{Pivot: item.ID, Limit: 10})
		require.NoError(t, err)

		item = read[9]
		read = read[:0]
		err = q.List(ctx, &read, types.ListOptions{Pivot: item.ID, Limit: 10})
		require.NoError(t, err)

		// The last item on the last page, should match the items written
		assert.True(t, items[1026].Compare(read[9]),
			"%+v != %+v", *items[1026], *read[9])
	})

	t.Run("Produce", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		q, err := s.NewQueue(store.QueueInfo{Name: random.String("queue-", 10)})
		defer func() { _ = q.Close(context.Background()) }()
		require.NoError(t, err)

		expire := time.Now().UTC().Add(random.Duration(time.Minute))
		var items []*types.Item
		for i := 0; i < 10; i++ {
			items = append(items, &types.Item{
				DeadDeadline: expire,
				Attempts:     rand.Intn(10),
				Reference:    random.String("ref-", 10),
				Encoding:     random.String("enc-", 10),
				Kind:         random.String("kind-", 10),
				Payload:      []byte(fmt.Sprintf("message-%d", i)),
			})
		}

		batch := types.Batch[types.ProduceRequest]{
			Requests: []*types.ProduceRequest{{Items: items}},
		}

		// Produce the items
		require.NoError(t, q.Produce(ctx, batch))

		// Ensure the items produced are in the database
		var read []*types.Item
		err = q.List(ctx, &read, types.ListOptions{Limit: 10_000})
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

		q, err := s.NewQueue(store.QueueInfo{Name: random.String("queue-", 10)})
		defer func() { _ = q.Close(context.Background()) }()
		require.NoError(t, err)

		items := writeRandomItems(t, ctx, q, 10_000)
		require.Len(t, items, 10_000)

		expire := time.Now().UTC().Add(2_000 * time.Minute)
		var reserved, secondReserve, lastReserved []*types.Item
		var read []*types.Item

		// Reserve 10 items in one request
		t.Run("TenItems", func(t *testing.T) {
			batch := types.ReserveBatch{
				Requests: []*types.ReserveRequest{{NumRequested: 10}},
				Total:    10,
			}

			require.NoError(t, q.Reserve(ctx, batch, store.ReserveOptions{ReserveDeadline: expire}))
			reserved = batch.Requests[0].Items
			require.Equal(t, 10, len(reserved))

			// Ensure the items reserved are marked as reserved in the database
			err = q.List(ctx, &read, types.ListOptions{Limit: 10_000})
			require.NoError(t, err)

			for i := range reserved {
				assert.Equal(t, read[i].ID, reserved[i].ID)
				assert.Equal(t, true, read[i].IsReserved)
				assert.True(t, expire.Equal(read[i].ReserveDeadline))
			}
		})

		// Reserve 10 more items
		t.Run("AnotherTenItems", func(t *testing.T) {
			batch := types.ReserveBatch{
				Requests: []*types.ReserveRequest{{NumRequested: 10}},
				Total:    10,
			}

			require.NoError(t, q.Reserve(ctx, batch, store.ReserveOptions{ReserveDeadline: expire}))
			secondReserve = batch.Requests[0].Items

			read = read[:0]
			err = q.List(ctx, &read, types.ListOptions{Limit: 10_000})
			require.NoError(t, err)

			var combined []*types.Item
			combined = append(combined, reserved...)
			combined = append(combined, secondReserve...)

			err = q.List(ctx, &read, types.ListOptions{Limit: 10_000})
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
			batch := types.ReserveBatch{
				Requests: []*types.ReserveRequest{
					{NumRequested: 5},
					{NumRequested: 10},
					{NumRequested: 20},
				},
				Total: 35,
			}

			require.NoError(t, q.Reserve(ctx, batch, store.ReserveOptions{ReserveDeadline: expire}))

			assert.Equal(t, 5, batch.Requests[0].NumRequested)
			assert.Equal(t, 5, len(batch.Requests[0].Items))
			assert.Equal(t, 10, batch.Requests[1].NumRequested)
			assert.Equal(t, 10, len(batch.Requests[1].Items))
			assert.Equal(t, 20, batch.Requests[2].NumRequested)
			assert.Equal(t, 20, len(batch.Requests[2].Items))

			// Ensure all the items are reserved
			read = read[:0]
			require.NoError(t, q.List(ctx, &read, types.ListOptions{Pivot: lastReserved[0].ID, Limit: 10_000}))
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

			q, err := s.NewQueue(store.QueueInfo{Name: random.String("queue-", 10)})
			defer func() { _ = q.Close(context.Background()) }()
			require.NoError(t, err)

			items := writeRandomItems(t, ctx, q, 23)
			require.Len(t, items, 23)

			expire := time.Now().UTC().Add(2_000 * time.Minute)

			batch := types.ReserveBatch{
				Requests: []*types.ReserveRequest{
					{NumRequested: 20},
					{NumRequested: 6},
					{NumRequested: 1},
				},
				Total: 27,
			}

			// Reserve() should fairly distribute items across all requests
			require.NoError(t, q.Reserve(ctx, batch, store.ReserveOptions{ReserveDeadline: expire}))
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

			q, err := s.NewQueue(store.QueueInfo{Name: random.String("queue-", 10)})
			defer func() { _ = q.Close(context.Background()) }()
			require.NoError(t, err)

			expire := time.Now().UTC().Add(2_000 * time.Minute)
			batch := types.ReserveBatch{
				Requests: []*types.ReserveRequest{
					{NumRequested: 20},
					{NumRequested: 6},
					{NumRequested: 1},
				},
				Total: 27,
			}

			// Reserve() should return no items and without error
			require.NoError(t, q.Reserve(ctx, batch, store.ReserveOptions{ReserveDeadline: expire}))
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

		q, err := s.NewQueue(store.QueueInfo{Name: random.String("queue-", 10)})
		defer func() { _ = q.Close(context.Background()) }()
		require.NoError(t, err)

		expire := time.Now().UTC().Add(random.Duration(time.Minute))
		var items []*types.Item
		for i := 0; i < 10; i++ {
			items = append(items, &types.Item{
				DeadDeadline: expire,
				Attempts:     rand.Intn(10),
				Reference:    random.String("ref-", 10),
				Encoding:     random.String("enc-", 10),
				Kind:         random.String("kind-", 10),
				Payload:      []byte(fmt.Sprintf("message-%d", i)),
			})
		}

		// Produce the items
		require.NoError(t, q.Produce(ctx, types.Batch[types.ProduceRequest]{
			Requests: []*types.ProduceRequest{{Items: items}},
		}))

		// Reserve the items
		reserve := types.ReserveBatch{
			Requests: []*types.ReserveRequest{{NumRequested: 9}},
			Total:    9,
		}

		require.NoError(t, q.Reserve(ctx, reserve, store.ReserveOptions{ReserveDeadline: expire}))

		ids := store.CollectIDs(reserve.Requests[0].Items)

		complete := types.Batch[types.CompleteRequest]{
			Requests: []*types.CompleteRequest{{Ids: ids}},
		}

		var read []*types.Item
		t.Run("Success", func(t *testing.T) {
			// Complete the items
			require.NoError(t, q.Complete(ctx, complete))
			require.NoError(t, complete.Requests[0].Err)

			// Ensure the items completed are not in the database
			require.NoError(t, q.List(ctx, &read, types.ListOptions{Limit: 10}))
			assert.Equal(t, 1, len(read))
		})

		t.Run("NotReserved", func(t *testing.T) {
			// Attempt to complete an item that has not been reserved
			complete = types.Batch[types.CompleteRequest]{
				Requests: []*types.CompleteRequest{{Ids: []string{read[0].ID}}},
			}

			require.NoError(t, q.Complete(ctx, complete))
			require.Error(t, complete.Requests[0].Err)
			assert.Contains(t, complete.Requests[0].Err.Error(), "item(s) cannot be completed")
			assert.Contains(t, complete.Requests[0].Err.Error(), "is not marked as reserved")
		})

		read = read[:0]
		t.Run("InvalidID", func(t *testing.T) {
			require.NoError(t, q.List(ctx, &read, types.ListOptions{Limit: 10}))
			assert.Equal(t, 1, len(read))

			complete = types.Batch[types.CompleteRequest]{
				Requests: []*types.CompleteRequest{
					{Ids: []string{"invalid-id"}},
					{Ids: []string{"another-invalid-id"}},
				},
			}

			require.NoError(t, q.Complete(ctx, complete))
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
	//	s, err := setup()
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
	//		Total:   100,
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

		q, err := s.NewQueue(store.QueueInfo{Name: random.String("queue-", 10)})
		defer func() { _ = q.Close(context.Background()) }()
		require.NoError(t, err)

		items := writeRandomItems(t, ctx, q, 10_000)
		require.Len(t, items, 10_000)

		// Ensure List() can fetch one item using pivot, and the item returned is the pivot
		item := items[1000]
		var read []*types.Item
		err = q.List(ctx, &read, types.ListOptions{Pivot: item.ID, Limit: 1})
		require.NoError(t, err)

		require.Equal(t, 1, len(read))
		assert.Equal(t, item.ID, read[0].ID)
		assert.True(t, read[0].Compare(item), "%+v != %+v", *read[0], item)
	})

	t.Run("Delete", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		q, err := s.NewQueue(store.QueueInfo{Name: random.String("queue-", 10)})
		defer func() { _ = q.Close(context.Background()) }()
		require.NoError(t, err)

		items := writeRandomItems(t, ctx, q, 10_000)
		require.Len(t, items, 10_000)

		// Delete 1_000 items
		err = q.Delete(ctx, store.CollectIDs(items[0:1_000]))
		require.NoError(t, err)

		var read []*types.Item
		err = q.List(ctx, &read, types.ListOptions{Limit: 10_000})
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

		// Delete 1_000 items that are already deleted
		err = q.Delete(ctx, store.CollectIDs(items[0:1_000]))
		require.NoError(t, err)
	})

}

func TestQueueStore(t *testing.T) {
	var dir string

	for _, tc := range []struct {
		Setup    NewStorageFunc
		TearDown func()
		Name     string
	}{
		{
			Name: "BuntDB",
			Setup: func() store.Storage {
				return store.NewBuntStorage(store.BuntOptions{})
			},
			TearDown: func() {},
		},
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
			testQueueStore(t, tc.Setup, tc.TearDown)
		})
	}
}

func testQueueStore(t *testing.T, setup NewStorageFunc, tearDown func()) {
	s := setup()
	defer func() {
		_ = s.Close(context.Background())
		tearDown()
	}()

	defer func() { _ = s.Close(context.Background()) }()

	t.Run("Set", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		q, err := s.NewQueueStore(store.QueueStoreOptions{})
		defer func() { _ = q.Close(context.Background()) }()
		require.NoError(t, err)

		queueName := random.String("queue-", 10)
		set := store.QueueInfo{Name: queueName}

		// Set
		require.NoError(t, q.Set(ctx, set))

		var get store.QueueInfo
		require.NoError(t, q.Get(ctx, queueName, &get))

		assert.Equal(t, set.Name, get.Name)
	})
}

func writeRandomItems(t *testing.T, ctx context.Context, s store.Queue, count int) []*types.Item {
	t.Helper()
	expire := time.Now().UTC().Add(random.Duration(time.Minute))

	var items []*types.Item
	for i := 0; i < count; i++ {
		items = append(items, &types.Item{
			DeadDeadline: expire,
			Attempts:     rand.Intn(10),
			Reference:    random.String("ref-", 10),
			Encoding:     random.String("enc-", 10),
			Kind:         random.String("kind-", 10),
			Payload:      []byte(fmt.Sprintf("message-%d", i)),
		})
	}
	err := s.Add(ctx, items)
	require.NoError(t, err)
	return items
}

func findInBatch(t *testing.T, batch types.ReserveBatch, id string) bool {
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
