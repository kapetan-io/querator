package service_test

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/duh-rpc/duh-go"
	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	pb "github.com/kapetan-io/querator/proto"
	svc "github.com/kapetan-io/querator/service"
	"github.com/kapetan-io/querator/transport/auth"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestStorageBackends(t *testing.T) {
	badgerdb := badgerTestSetup{Dir: t.TempDir()}
	postgres := postgresTestSetup{}
	mongo := mongoTestSetup{}

	for _, tc := range []struct {
		Setup    NewStorageFunc
		TearDown func()
		Name     string
		Lifetime storageLifetime
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
				return badgerdb.Setup(store.BadgerConfig{})
			},
			TearDown: func() {
				badgerdb.Teardown()
			},
			Lifetime: outlivesDaemon,
		},
		{
			Name: "PostgreSQL",
			Setup: func() store.Config {
				return postgres.Setup(store.PostgresConfig{})
			},
			TearDown: func() {
				postgres.Teardown()
			},
			Lifetime: outlivesDaemon,
		},
		{
			Name: "MongoDB",
			Setup: func() store.Config {
				return mongo.Setup(store.MongoConfig{})
			},
			TearDown: func() {
				mongo.Teardown()
			},
			Lifetime: outlivesDaemon,
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			testStorageBackends(t, tc.Setup, tc.TearDown, tc.Lifetime)
		})
	}
}

// withExtraBackend adds a memory partition backend to conf, so a test has two backends to place
// partitions on. A non-nil fault wraps only the added backend.
func withExtraBackend(conf store.Config, name string, affinity float64, fault *partitionFault) store.Config {
	extra := store.PartitionStorage{
		PartitionStore: store.NewMemoryPartitionStore(conf, log),
		Name:           name,
		Affinity:       affinity,
	}
	if fault != nil {
		extra = fault.Wrap(store.Config{PartitionStorage: []store.PartitionStorage{extra}}).PartitionStorage[0]
	}
	conf.PartitionStorage = append(conf.PartitionStorage, extra)
	return conf
}

func findBackend(t *testing.T, list *pb.StorageBackendsListResponse, name string) *pb.StorageBackend {
	t.Helper()
	for _, b := range list.Backends {
		if b.Backend == name {
			return b
		}
	}
	require.FailNowf(t, "backend not listed", "'%s' is not in storage/backends.list", name)
	return nil
}

func listBackends(t *testing.T, td *testDaemon) *pb.StorageBackendsListResponse {
	t.Helper()
	var list pb.StorageBackendsListResponse
	require.NoError(t, td.MustClient().StorageBackendsList(td.Context(), &list))
	return &list
}

func testStorageBackends(t *testing.T, setup NewStorageFunc, tearDown func(), lifetime storageLifetime) {
	t.Run("ListsConfiguredBackendWithItsPartitions", func(t *testing.T) {
		conf := setup()
		defer tearDown()
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()
		name := conf.PartitionStorage[0].Name

		backend := findBackend(t, listBackends(t, d), name)
		assert.Equal(t, 1.0, backend.GetAffinity())
		assert.True(t, backend.GetConfigured())
		assert.False(t, backend.GetSafeToStop())
		assert.Empty(t, backend.Partitions)
		require.NotNil(t, backend.Pending)
		assert.Empty(t, backend.Pending.Create)
		assert.Empty(t, backend.Pending.Drain)

		queueName := random.String("queue-", 10)
		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			QueueName:           queueName,
			LeaseTimeout:        "1m0s",
			ExpireTimeout:       "24h0m0s",
			RequestedPartitions: 2,
		})
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items:          produceRandomItems(3),
		}))

		backend = findBackend(t, listBackends(t, d), name)
		require.Len(t, backend.Partitions, 2)
		var unleased int32
		for i, p := range backend.Partitions {
			assert.Equal(t, queueName, p.QueueName)
			assert.Equal(t, int32(i), p.GetPartition())
			assert.Equal(t, "available", p.State)
			assert.Equal(t, int32(0), p.GetFailures())
			require.NotNil(t, p.Unleased)
			require.NotNil(t, p.Leased)
			require.NotNil(t, p.Scheduled)
			unleased += p.GetUnleased()
		}
		assert.Equal(t, int32(3), unleased)
		assert.False(t, backend.GetSafeToStop())
	})

	t.Run("ScheduledCountDropsWhenScheduledItemIsDeleted", func(t *testing.T) {
		conf := setup()
		defer tearDown()
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		createOnePartitionQueue(t, ctx, c, queueName)
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items: []*pb.QueueProduceItem{{
				Bytes:     []byte("later"),
				EnqueueAt: timestamppb.New(clock.Now().UTC().Add(clock.Hour)),
			}},
		}))
		partition := findBackend(t, listBackends(t, d), conf.PartitionStorage[0].Name).Partitions[0]
		require.Equal(t, int32(1), partition.GetScheduled())

		var scheduled pb.StorageItemsListResponse
		require.NoError(t, c.StorageScheduledList(ctx, queueName, 0, &scheduled, nil))
		require.Len(t, scheduled.Items, 1)
		require.NoError(t, c.StorageItemsDelete(ctx, &pb.StorageItemsDeleteRequest{
			QueueName: queueName,
			Ids:       []string{scheduled.Items[0].Id},
		}))

		partition = findBackend(t, listBackends(t, d), conf.PartitionStorage[0].Name).Partitions[0]
		assert.Equal(t, int32(0), partition.GetScheduled())
	})

	t.Run("ListCountsLeasedAndScheduledItems", func(t *testing.T) {
		conf := setup()
		defer tearDown()
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		createOnePartitionQueue(t, ctx, c, queueName)
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items:          produceRandomItems(3),
		}))
		var lease pb.QueueLeaseResponse
		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			QueueName:      queueName,
			ClientId:       random.String("client-", 10),
			BatchSize:      1,
			RequestTimeout: "5s",
		}, &lease))
		require.Len(t, lease.Items, 1)

		partition := findBackend(t, listBackends(t, d), conf.PartitionStorage[0].Name).Partitions[0]
		assert.Equal(t, int32(2), partition.GetUnleased())
		assert.Equal(t, int32(1), partition.GetLeased())
		assert.Equal(t, int32(0), partition.GetScheduled())

		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items: []*pb.QueueProduceItem{{
				Bytes:     []byte("later"),
				EnqueueAt: timestamppb.New(clock.Now().UTC().Add(clock.Hour)),
			}},
		}))

		partition = findBackend(t, listBackends(t, d), conf.PartitionStorage[0].Name).Partitions[0]
		assert.Equal(t, int32(1), partition.GetLeased())
		assert.Equal(t, int32(1), partition.GetScheduled())
	})

	t.Run("ListCountsScheduledRetry", func(t *testing.T) {
		conf := setup()
		defer tearDown()
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		createOnePartitionQueue(t, ctx, c, queueName)
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items:          produceRandomItems(1),
		}))
		var lease pb.QueueLeaseResponse
		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			QueueName:      queueName,
			ClientId:       random.String("client-", 10),
			BatchSize:      1,
			RequestTimeout: "5s",
		}, &lease))
		require.Len(t, lease.Items, 1)

		require.NoError(t, c.QueueRetry(ctx, &pb.QueueRetryRequest{
			QueueName: queueName,
			Partition: lease.Partition,
			Items: []*pb.QueueRetryItem{
				{Id: lease.Items[0].Id, RetryAt: timestamppb.New(clock.Now().UTC().Add(clock.Hour))},
			},
		}))

		partition := findBackend(t, listBackends(t, d), conf.PartitionStorage[0].Name).Partitions[0]
		assert.Equal(t, int32(0), partition.GetLeased())
		assert.Equal(t, int32(1), partition.GetScheduled())
	})

	t.Run("ScheduledCountDropsWhenItemComesDue", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		conf := setup()
		defer tearDown()
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf, Clock: now})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()
		name := conf.PartitionStorage[0].Name

		queueName := random.String("queue-", 10)
		createOnePartitionQueue(t, ctx, c, queueName)
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items: []*pb.QueueProduceItem{{
				Bytes:     []byte("soon"),
				EnqueueAt: timestamppb.New(now.Now().UTC().Add(clock.Minute)),
			}},
		}))
		assert.Equal(t, int32(1), findBackend(t, listBackends(t, d), name).Partitions[0].GetScheduled())

		now.Advance(2 * clock.Minute)
		// Only scheduled is checked; unleased counts the due item twice on some backends (QUE-6).
		require.Eventually(t, func() bool {
			return findBackend(t, listBackends(t, d), name).Partitions[0].GetScheduled() == 0
		}, 5*clock.Second, 50*clock.Millisecond)
	})

	t.Run("FirstSeenBackendListsConfiguredAffinity", func(t *testing.T) {
		conf := withExtraBackend(setup(), "memory-extra", 2.5, nil)
		defer tearDown()
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
		defer d.Shutdown(t)

		list := listBackends(t, d)
		assert.Equal(t, 2.5, findBackend(t, list, "memory-extra").GetAffinity())
		assert.True(t, findBackend(t, list, "memory-extra").GetConfigured())
		assert.Equal(t, 1.0, findBackend(t, list, conf.PartitionStorage[0].Name).GetAffinity())
	})

	t.Run("PendingListsRebalanceAfterAffinityChange", func(t *testing.T) {
		conf := withExtraBackend(setup(), "memory-extra", 1, nil)
		defer tearDown()
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()
		name := conf.PartitionStorage[0].Name

		queueName := random.String("queue-", 10)
		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			QueueName:           queueName,
			LeaseTimeout:        "1m0s",
			ExpireTimeout:       "24h0m0s",
			RequestedPartitions: 2,
		})
		var info pb.QueueInfo
		require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: queueName}, &info))
		var onExtra int32 = -1
		for _, p := range info.Partitions {
			if p.StorageName == "memory-extra" {
				onExtra = p.Partition
			}
		}
		require.NotEqual(t, int32(-1), onExtra)

		list := listBackends(t, d)
		for _, b := range list.Backends {
			assert.Empty(t, b.Pending.Create)
			assert.Empty(t, b.Pending.Drain)
		}

		require.NoError(t, c.StorageBackendsUpdate(ctx, &pb.StorageBackendsUpdateRequest{
			Backend:  "memory-extra",
			Affinity: new(0.0),
		}))

		list = listBackends(t, d)
		extra := findBackend(t, list, "memory-extra")
		assert.Empty(t, extra.Pending.Create)
		require.Len(t, extra.Pending.Drain, 1)
		assert.Equal(t, queueName, extra.Pending.Drain[0].QueueName)
		assert.Equal(t, []int32{onExtra}, extra.Pending.Drain[0].Partitions)

		main := findBackend(t, list, name)
		assert.Empty(t, main.Pending.Drain)
		require.Len(t, main.Pending.Create, 1)
		assert.Equal(t, queueName, main.Pending.Create[0].QueueName)
		assert.Equal(t, []int32{2}, main.Pending.Create[0].Partitions)
	})

	t.Run("UpdateWithoutAffinityKeepsSavedAffinity", func(t *testing.T) {
		conf := setup()
		defer tearDown()
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()
		name := conf.PartitionStorage[0].Name

		require.NoError(t, c.StorageBackendsUpdate(ctx, &pb.StorageBackendsUpdateRequest{
			Backend:  name,
			Affinity: new(0.5),
		}))
		assert.Equal(t, 0.5, findBackend(t, listBackends(t, d), name).GetAffinity())

		require.NoError(t, c.StorageBackendsUpdate(ctx, &pb.StorageBackendsUpdateRequest{Backend: name}))
		assert.Equal(t, 0.5, findBackend(t, listBackends(t, d), name).GetAffinity())
	})

	t.Run("ZeroAffinityPlacesNoNewPartitions", func(t *testing.T) {
		conf := withExtraBackend(setup(), "memory-extra", 1, nil)
		defer tearDown()
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()

		extra := findBackend(t, listBackends(t, d), "memory-extra")
		assert.Empty(t, extra.Partitions)
		assert.False(t, extra.GetSafeToStop())

		require.NoError(t, c.StorageBackendsUpdate(ctx, &pb.StorageBackendsUpdateRequest{
			Backend:  "memory-extra",
			Affinity: new(0.0),
		}))

		queueName := random.String("queue-", 10)
		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			QueueName:           queueName,
			LeaseTimeout:        "1m0s",
			ExpireTimeout:       "24h0m0s",
			RequestedPartitions: 4,
		})

		var info pb.QueueInfo
		require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: queueName}, &info))
		require.Len(t, info.Partitions, 4)
		for _, p := range info.Partitions {
			assert.Equal(t, conf.PartitionStorage[0].Name, p.StorageName)
		}

		extra = findBackend(t, listBackends(t, d), "memory-extra")
		assert.Equal(t, 0.0, extra.GetAffinity())
		assert.Empty(t, extra.Partitions)
		assert.True(t, extra.GetSafeToStop())
	})

	t.Run("ListRepliesWhileBackendIsDown", func(t *testing.T) {
		fault := &partitionFault{}
		conf := withExtraBackend(setup(), "memory-down", 1, fault)
		defer tearDown()
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			QueueName:           queueName,
			LeaseTimeout:        "1m0s",
			ExpireTimeout:       "24h0m0s",
			RequestedPartitions: 2,
		})

		fault.statsFailing.Store(true)
		require.NoError(t, c.QueueReload(ctx, &pb.QueueReloadRequest{QueueName: queueName}))

		list := listBackends(t, d)
		down := findBackend(t, list, "memory-down")
		require.Len(t, down.Partitions, 1)
		assert.Greater(t, down.Partitions[0].GetFailures(), int32(0))
		assert.Nil(t, down.Partitions[0].Unleased)
		assert.Nil(t, down.Partitions[0].Leased)
		assert.Nil(t, down.Partitions[0].Scheduled)

		up := findBackend(t, list, conf.PartitionStorage[0].Name)
		require.Len(t, up.Partitions, 1)
		assert.Equal(t, int32(0), up.Partitions[0].GetFailures())
		assert.NotNil(t, up.Partitions[0].Unleased)

		// A backend at affinity 0 that still holds a partition is not safe to stop.
		require.NoError(t, c.StorageBackendsUpdate(ctx, &pb.StorageBackendsUpdateRequest{
			Backend:  "memory-down",
			Affinity: new(0.0),
		}))
		assert.False(t, findBackend(t, listBackends(t, d), "memory-down").GetSafeToStop())
	})

	t.Run("ListShowsPartitionOnBackendWithNoRecord", func(t *testing.T) {
		conf := setup()
		defer tearDown()
		queueName := random.String("queue-", 10)
		require.NoError(t, conf.Queues.Add(context.Background(), types.QueueInfo{
			Name:                queueName,
			LeaseTimeout:        time.Minute,
			ExpireTimeout:       24 * time.Hour,
			RequestedPartitions: 1,
			NextPartition:       1,
			PartitionInfo:       []types.PartitionInfo{{StorageName: "gone", PartitionNum: 0}},
		}))
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
		defer d.Shutdown(t)

		gone := findBackend(t, listBackends(t, d), "gone")
		assert.False(t, gone.GetConfigured())
		require.Len(t, gone.Partitions, 1)
		assert.Equal(t, queueName, gone.Partitions[0].QueueName)
		assert.False(t, gone.GetSafeToStop())
		assert.Nil(t, gone.Affinity)
		assert.Equal(t, int32(1), gone.Partitions[0].GetFailures())
		assert.Nil(t, gone.Partitions[0].Unleased)

		// A rebalance would drain the partition and create its replacement on the recorded backend
		require.NotNil(t, gone.Pending)
		assert.Empty(t, gone.Pending.Create)
		require.Len(t, gone.Pending.Drain, 1)
		assert.Equal(t, queueName, gone.Pending.Drain[0].QueueName)
		assert.Equal(t, []int32{0}, gone.Pending.Drain[0].Partitions)
		recorded := findBackend(t, listBackends(t, d), conf.PartitionStorage[0].Name)
		assert.NotNil(t, recorded.Affinity)
		require.Len(t, recorded.Pending.Create, 1)
		assert.Equal(t, queueName, recorded.Pending.Create[0].QueueName)
		assert.Equal(t, []int32{1}, recorded.Pending.Create[0].Partitions)

		var resp pb.QueuesRebalanceResponse
		require.NoError(t, d.MustClient().QueuesRebalance(d.Context(), &pb.QueuesRebalanceRequest{}, &resp))
		assert.Equal(t, int32(1), resp.GetPartitionsDraining())
		assert.Equal(t, int32(1), resp.GetPartitionsCreated())
		gone = findBackend(t, listBackends(t, d), "gone")
		require.Len(t, gone.Partitions, 1)
		assert.Equal(t, "draining", gone.Partitions[0].State)
		assert.Nil(t, gone.Affinity)
		assert.False(t, gone.GetSafeToStop())
		assert.Empty(t, gone.Pending.Drain)
		recorded = findBackend(t, listBackends(t, d), conf.PartitionStorage[0].Name)
		require.Len(t, recorded.Partitions, 1)
		assert.Equal(t, int32(1), recorded.Partitions[0].GetPartition())
	})

	t.Run("ListOrdersUnrecordedBackendsByNameAfterRecordedOnes", func(t *testing.T) {
		conf := setup()
		defer tearDown()
		queueName := random.String("queue-", 10)
		require.NoError(t, conf.Queues.Add(context.Background(), types.QueueInfo{
			Name:                queueName,
			LeaseTimeout:        time.Minute,
			ExpireTimeout:       24 * time.Hour,
			RequestedPartitions: 2,
			NextPartition:       2,
			PartitionInfo: []types.PartitionInfo{
				{StorageName: "gone-b", PartitionNum: 0},
				{StorageName: "gone-a", PartitionNum: 1},
			},
		}))
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
		defer d.Shutdown(t)

		var names []string
		for _, b := range listBackends(t, d).Backends {
			names = append(names, b.Backend)
		}
		assert.Equal(t, []string{conf.PartitionStorage[0].Name, "gone-a", "gone-b"}, names)
	})

	t.Run("Errors", func(t *testing.T) {
		t.Run("StorageBackendsUpdate", func(t *testing.T) {
			for _, test := range []struct {
				name     string
				backend  string
				affinity *float64
				wantCode int
				wantErr  string
			}{
				{
					name:     "EmptyBackend",
					backend:  "",
					affinity: new(1.0),
					wantCode: duh.CodeBadRequest,
					wantErr:  "backend is invalid; cannot be empty",
				},
				{
					name:     "UnknownBackend",
					backend:  "no-such-backend",
					affinity: new(1.0),
					wantCode: duh.CodeBadRequest,
					wantErr:  "backend is invalid; 'no-such-backend' is not a known storage backend",
				},
				{
					name:     "UnknownBackendWithoutAffinity",
					backend:  "no-such-backend",
					wantCode: duh.CodeBadRequest,
					wantErr:  "backend is invalid; 'no-such-backend' is not a known storage backend",
				},
				{
					name:     "NegativeAffinity",
					affinity: new(-1.0),
					wantCode: duh.CodeBadRequest,
					wantErr:  "affinity cannot be negative, got -1",
				},
				{
					name:     "InfiniteAffinity",
					affinity: new(math.Inf(1)),
					wantCode: duh.CodeBadRequest,
					wantErr:  "affinity is invalid",
				},
				{
					name:     "NaNAffinity",
					affinity: new(math.NaN()),
					wantCode: duh.CodeBadRequest,
					wantErr:  "affinity is invalid",
				},
				{
					name:     "AffinityOverflowsTheSum",
					affinity: new(math.MaxFloat64),
					wantCode: duh.CodeBadRequest,
					wantErr:  "affinity is invalid",
				},
			} {
				t.Run(test.name, func(t *testing.T) {
					conf := setup()
					defer tearDown()
					d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
					defer d.Shutdown(t)
					c, ctx := d.MustClient(), d.Context()
					name := conf.PartitionStorage[0].Name

					backend := test.backend
					if test.name != "EmptyBackend" && backend == "" {
						backend = name
					}
					err := c.StorageBackendsUpdate(ctx, &pb.StorageBackendsUpdateRequest{
						Backend:  backend,
						Affinity: test.affinity,
					})
					var duhErr duh.Error
					require.True(t, errors.As(err, &duhErr))
					assert.Equal(t, test.wantCode, duhErr.Code())
					assert.Contains(t, duhErr.Message(), test.wantErr)

					// Nothing was saved
					list := listBackends(t, d)
					require.Len(t, list.Backends, 1)
					assert.Equal(t, name, list.Backends[0].Backend)
					assert.Equal(t, 1.0, list.Backends[0].GetAffinity())
				})
			}
		})
	})

	// Only storage that outlives the daemon can show what a restart finds.
	if lifetime == outlivesDaemon {
		t.Run("FirstSeenAffinityOutlivesConfigChange", func(t *testing.T) {
			conf := setup()
			defer tearDown()
			first := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
			name := conf.PartitionStorage[0].Name
			assert.Equal(t, 1.0, findBackend(t, listBackends(t, first), name).GetAffinity())
			first.Shutdown(t)

			conf.PartitionStorage[0].Affinity = 3
			second := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
			defer second.Shutdown(t)
			assert.Equal(t, 1.0, findBackend(t, listBackends(t, second), name).GetAffinity())
		})

		t.Run("RefusedRebalancePlanLeavesUnrecordedEntryWithoutPending", func(t *testing.T) {
			conf := withExtraBackend(setup(), "memory-removed", 1, nil)
			defer tearDown()
			queueName := random.String("queue-", 10)
			require.NoError(t, conf.Queues.Add(context.Background(), types.QueueInfo{
				Name:                queueName,
				LeaseTimeout:        time.Minute,
				ExpireTimeout:       24 * time.Hour,
				RequestedPartitions: 1,
				NextPartition:       1,
				PartitionInfo:       []types.PartitionInfo{{StorageName: "gone", PartitionNum: 0}},
			}))
			first := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
			first.Shutdown(t)

			// A recorded backend with affinity above 0 that the config no longer names makes
			// the rebalance refuse, so the list has no plan to show for any backend
			conf.PartitionStorage = conf.PartitionStorage[:1]
			second := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
			defer second.Shutdown(t)

			gone := findBackend(t, listBackends(t, second), "gone")
			require.Len(t, gone.Partitions, 1)
			assert.Empty(t, gone.Pending.Create)
			assert.Empty(t, gone.Pending.Drain)
			assert.False(t, gone.GetSafeToStop())
		})

		t.Run("PartitionOnBackendMissingFromConfigLoads", func(t *testing.T) {
			conf := withExtraBackend(setup(), "memory-removed", 1, nil)
			defer tearDown()
			first := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
			c, ctx := first.MustClient(), first.Context()
			queueName := random.String("queue-", 10)
			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				QueueName:           queueName,
				LeaseTimeout:        "1m0s",
				ExpireTimeout:       "24h0m0s",
				RequestedPartitions: 2,
			})
			var info pb.QueueInfo
			require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: queueName}, &info))
			var removed int32 = -1
			for _, p := range info.Partitions {
				if p.StorageName == "memory-removed" {
					removed = p.Partition
				}
			}
			require.NotEqual(t, int32(-1), removed)
			first.Shutdown(t)

			conf.PartitionStorage = conf.PartitionStorage[:1]
			second := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
			defer second.Shutdown(t)
			c, ctx = second.MustClient(), second.Context()

			var stats pb.QueueStatsResponse
			require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats))
			require.Len(t, stats.LogicalQueues, 1)
			var found bool
			for _, p := range stats.LogicalQueues[0].Partitions {
				if p.Partition == removed {
					found = true
					assert.Greater(t, p.Failures, int32(0))
					continue
				}
				assert.Equal(t, int32(0), p.Failures)
			}
			assert.True(t, found)

			// Items still go to the partition whose backend is configured
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "5s",
				Items:          produceRandomItems(1),
			}))

			missing := findBackend(t, listBackends(t, second), "memory-removed")
			assert.False(t, missing.GetConfigured())
			require.Len(t, missing.Partitions, 1)
			assert.Greater(t, missing.Partitions[0].GetFailures(), int32(0))
			assert.Nil(t, missing.Partitions[0].Unleased)
		})
	}
}

func TestStorageBackendsPermissions(t *testing.T) {
	d := newDaemonWithAuth(t, func() store.Config { return setupMemoryStorage(store.Config{}) })
	defer d.Shutdown(t)
	ctx := d.Context()
	admin := newClientWithAPIKey(t, d, d.AdminKey)

	for _, test := range []struct {
		name    string
		granted []string
		call    func(c *querator.Client) error
	}{
		{
			name:    "ListWithoutBackendList",
			granted: []string{auth.QueueList, auth.BackendUpdate},
			call: func(c *querator.Client) error {
				return c.StorageBackendsList(ctx, &pb.StorageBackendsListResponse{})
			},
		},
		{
			name:    "UpdateWithoutBackendUpdate",
			granted: []string{auth.QueueList, auth.BackendList},
			call: func(c *querator.Client) error {
				return c.StorageBackendsUpdate(ctx, &pb.StorageBackendsUpdateRequest{
					Backend:  "memory-0",
					Affinity: new(2.0),
				})
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			limited := newClientWithAPIKey(t, d, CreateUserWithLimitedPermissions(t, ctx, admin, test.granted))
			err := test.call(limited)
			var duhErr duh.Error
			require.ErrorAs(t, err, &duhErr)
			assert.Equal(t, duh.CodeForbidden, duhErr.Code())

			var list pb.StorageBackendsListResponse
			require.NoError(t, admin.StorageBackendsList(ctx, &list))
			assert.Equal(t, 1.0, findBackend(t, &list, "memory-0").GetAffinity())
		})
	}

	t.Run("GrantedBothPermissions", func(t *testing.T) {
		granted := newClientWithAPIKey(t, d, CreateUserWithLimitedPermissions(t, ctx, admin,
			[]string{auth.BackendList, auth.BackendUpdate}))
		require.NoError(t, granted.StorageBackendsUpdate(ctx, &pb.StorageBackendsUpdateRequest{
			Backend:  "memory-0",
			Affinity: new(2.0),
		}))
		var list pb.StorageBackendsListResponse
		require.NoError(t, granted.StorageBackendsList(ctx, &list))
		assert.Equal(t, 2.0, findBackend(t, &list, "memory-0").GetAffinity())
	})
}
