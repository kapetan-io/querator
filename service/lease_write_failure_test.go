package service_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/duh-rpc/duh-go"
	"github.com/jackc/pgx/v5"
	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/internal/store"
	pb "github.com/kapetan-io/querator/proto"
	svc "github.com/kapetan-io/querator/service"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	mongoopts "go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/goleak"
)

// leaseFault makes the database refuse to save a lease of the item with Reference "B",
// so a lease write fails the way a real storage fault does, and lets it through again.
// Close releases the test's own connection to the database.
type leaseFault struct {
	Refuse func(t *testing.T)
	Allow  func(t *testing.T)
	Close  func(t *testing.T)
}

// TestLeaseWriteFailure holds lease tests that need the database itself to refuse the
// lease write, a fault the client cannot cause. They run against PostgreSQL and MongoDB,
// where a test can add a rule to the table or collection.
func TestLeaseWriteFailure(t *testing.T) {
	defer goleak.VerifyNone(t, goleakOptions...)

	t.Run("PostgreSQL", func(t *testing.T) {
		t.Run("LeaseSavesNothing", func(t *testing.T) {
			postgres := postgresTestSetup{}
			d := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: postgres.Setup(store.PostgresConfig{})})
			defer func() {
				d.Shutdown(t)
				postgres.Teardown()
			}()
			c, ctx := d.MustClient(), d.Context()
			queueName := produceLeaseFaultItems(t, ctx, c)
			fault := postgresLeaseFault(t, ctx, postgres.dsn)
			defer fault.Close(t)

			// The update of B fails, and the transaction takes A's update back with it, so the
			// lease saves nothing and the request times out empty.
			fault.Refuse(t)
			var lease pb.QueueLeaseResponse
			err := c.QueueLease(ctx, &pb.QueueLeaseRequest{
				ClientId:       random.String("client-", 10),
				RequestTimeout: "1s",
				QueueName:      queueName,
				BatchSize:      2,
			}, &lease)
			var e duh.Error
			require.True(t, errors.As(err, &e))
			assert.Equal(t, querator.MsgRequestTimeout, e.Message())
			assert.Empty(t, lease.Items)
			assert.False(t, isLeased(t, ctx, c, queueName, "A"))
			assert.False(t, isLeased(t, ctx, c, queueName, "B"))
			assert.Equal(t, leaseCounts{UnLeased: 2, TotalLeased: 0}, partitionLeaseCounts(t, ctx, c, queueName))

			fault.Allow(t)
			assert.Equal(t, []string{"A", "B"}, leaseAndCompleteReferences(t, ctx, c, queueName))
		})

		t.Run("LaterPassAnswersWaitingLease", func(t *testing.T) {
			postgres := postgresTestSetup{}
			d := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: postgres.Setup(store.PostgresConfig{})})
			defer func() {
				d.Shutdown(t)
				postgres.Teardown()
			}()
			c, ctx := d.MustClient(), d.Context()
			queueName := produceLeaseFaultItems(t, ctx, c)
			fault := postgresLeaseFault(t, ctx, postgres.dsn)
			defer fault.Close(t)

			fault.Refuse(t)
			var lease pb.QueueLeaseResponse
			var leaseErr error
			backgroundCall{
				Call: func() {
					leaseErr = c.QueueLease(ctx, &pb.QueueLeaseRequest{
						ClientId:       random.String("client-", 10),
						RequestTimeout: "10s",
						QueueName:      queueName,
						BatchSize:      2,
					}, &lease)
				},
				While: func() {
					// The failed pass leaves the request waiting with nothing saved.
					require.NoError(t, untilLeaseClientWaiting(t, c, queueName, 1))
					assert.False(t, isLeased(t, ctx, c, queueName, "A"))
					assert.False(t, isLeased(t, ctx, c, queueName, "B"))

					// Resuming runs another pass that tries the waiting request again and fails
					// again, and neither pass moves the counts.
					require.NoError(t, d.Service().PauseQueue(ctx, queueName))
					require.NoError(t, d.Service().ResumeQueue(ctx, queueName))
					require.NoError(t, untilLeaseClientWaiting(t, c, queueName, 1))
					assert.Equal(t, leaseCounts{UnLeased: 2, TotalLeased: 0}, partitionLeaseCounts(t, ctx, c, queueName))

					// A produce runs a later pass, which answers the waiting request.
					fault.Allow(t)
					require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
						QueueName:      queueName,
						RequestTimeout: "5s",
						Items:          []*pb.QueueProduceItem{{Reference: "C", Bytes: []byte("c")}},
					}))
				},
			}.run()
			require.NoError(t, leaseErr)

			var references []string
			for _, item := range lease.Items {
				references = append(references, item.Reference)
				assert.True(t, isLeased(t, ctx, c, queueName, itemReference(item.Reference)))
			}
			assert.Equal(t, []string{"A", "B"}, references)
			assert.False(t, isLeased(t, ctx, c, queueName, "C"))
		})

		t.Run("CommitFails", func(t *testing.T) {
			postgres := postgresTestSetup{}
			d := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: postgres.Setup(store.PostgresConfig{})})
			defer func() {
				d.Shutdown(t)
				postgres.Teardown()
			}()
			c, ctx := d.MustClient(), d.Context()
			queueName := produceLeaseFaultItems(t, ctx, c)

			conn, err := pgx.Connect(ctx, postgres.dsn)
			require.NoError(t, err)
			defer func() { _ = conn.Close(ctx) }()
			var table string
			require.NoError(t, conn.QueryRow(ctx,
				`SELECT tablename FROM pg_tables WHERE tablename LIKE 'items\_%'`).Scan(&table))
			name := pgx.Identifier{table}.Sanitize()

			// A deferred constraint trigger lets every UPDATE through and refuses the lease of
			// B only when the transaction commits, so the commit is the write that fails.
			_, err = conn.Exec(ctx, `CREATE FUNCTION refuse_lease_b() RETURNS trigger AS $$
				BEGIN
					IF NEW.is_leased AND NEW.reference = 'B' THEN
						RAISE EXCEPTION 'lease of B refused';
					END IF;
					RETURN NEW;
				END $$ LANGUAGE plpgsql`)
			require.NoError(t, err)
			_, err = conn.Exec(ctx, `CREATE CONSTRAINT TRIGGER refuse_lease_b AFTER UPDATE ON `+name+
				` DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION refuse_lease_b()`)
			require.NoError(t, err)

			var lease pb.QueueLeaseResponse
			err = c.QueueLease(ctx, &pb.QueueLeaseRequest{
				ClientId:       random.String("client-", 10),
				RequestTimeout: "1s",
				QueueName:      queueName,
				BatchSize:      2,
			}, &lease)
			var e duh.Error
			require.True(t, errors.As(err, &e))
			assert.Equal(t, querator.MsgRequestTimeout, e.Message())
			assert.Empty(t, lease.Items)
			assert.False(t, isLeased(t, ctx, c, queueName, "A"))
			assert.False(t, isLeased(t, ctx, c, queueName, "B"))

			assert.Equal(t, leaseCounts{UnLeased: 2, TotalLeased: 0}, partitionLeaseCounts(t, ctx, c, queueName))

			_, err = conn.Exec(ctx, `DROP TRIGGER refuse_lease_b ON `+name)
			require.NoError(t, err)
			assert.Equal(t, []string{"A", "B"}, leaseAndCompleteReferences(t, ctx, c, queueName))
		})

		t.Run("SeveralWaitingLeases", func(t *testing.T) {
			postgres := postgresTestSetup{}
			d := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: postgres.Setup(store.PostgresConfig{})})
			defer func() {
				d.Shutdown(t)
				postgres.Teardown()
			}()
			c, ctx := d.MustClient(), d.Context()
			queueName := produceLeaseFaultItems(t, ctx, c)
			fault := postgresLeaseFault(t, ctx, postgres.dsn)
			defer fault.Close(t)

			fault.Refuse(t)
			require.NoError(t, d.Service().PauseQueue(ctx, queueName))
			leases := make([]pb.QueueLeaseResponse, 2)
			errs := make([]error, 2)
			backgroundCall{
				Call: func() {
					concurrently(2, func(i int) {
						errs[i] = c.QueueLease(ctx, &pb.QueueLeaseRequest{
							ClientId:       random.String("client-", 10),
							RequestTimeout: "10s",
							QueueName:      queueName,
							BatchSize:      1,
						}, &leases[i])
					})
				},
				While: func() {
					require.NoError(t, untilLeaseClientWaiting(t, c, queueName, 2))
					// Resuming runs one pass that places A on one request and B on the other.
					// The transaction saves neither, so both requests keep waiting.
					require.NoError(t, d.Service().ResumeQueue(ctx, queueName))
					require.NoError(t, untilLeaseClientWaiting(t, c, queueName, 2))
					assert.False(t, isLeased(t, ctx, c, queueName, "A"))
					assert.False(t, isLeased(t, ctx, c, queueName, "B"))

					fault.Allow(t)
					require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
						QueueName:      queueName,
						RequestTimeout: "5s",
						Items:          []*pb.QueueProduceItem{{Reference: "C", Bytes: []byte("c")}},
					}))
				},
			}.run()

			var references []string
			for i := range leases {
				require.NoError(t, errs[i])
				require.Len(t, leases[i].Items, 1)
				references = append(references, leases[i].Items[0].Reference)
				assert.True(t, isLeased(t, ctx, c, queueName, itemReference(leases[i].Items[0].Reference)))
			}
			slices.Sort(references)
			assert.Equal(t, []string{"A", "B"}, references)
		})
	})

	t.Run("MongoDB", func(t *testing.T) {
		t.Run("LeaseHandsOutSavedItems", func(t *testing.T) {
			mongoSetup := mongoTestSetup{}
			d := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: mongoSetup.Setup(store.MongoConfig{})})
			defer func() {
				d.Shutdown(t)
				mongoSetup.Teardown()
			}()
			c, ctx := d.MustClient(), d.Context()
			queueName := produceLeaseFaultItems(t, ctx, c)
			fault := mongoLeaseFault(t, ctx, mongoSetup.dbName)
			defer fault.Close(t)

			// The bulk write is unordered, so A's update applies while B's is refused. The
			// consumer gets A, the one item storage shows as leased.
			fault.Refuse(t)
			assert.Equal(t, []string{"A"}, leaseAndCompleteReferences(t, ctx, c, queueName))
			assert.False(t, isLeased(t, ctx, c, queueName, "B"))

			fault.Allow(t)
			assert.Equal(t, []string{"B"}, leaseAndCompleteReferences(t, ctx, c, queueName))
		})

		t.Run("SeveralWaitingLeases", func(t *testing.T) {
			mongoSetup := mongoTestSetup{}
			d := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: mongoSetup.Setup(store.MongoConfig{})})
			defer func() {
				d.Shutdown(t)
				mongoSetup.Teardown()
			}()
			c, ctx := d.MustClient(), d.Context()
			queueName := produceLeaseFaultItems(t, ctx, c)
			fault := mongoLeaseFault(t, ctx, mongoSetup.dbName)
			defer fault.Close(t)

			fault.Refuse(t)
			require.NoError(t, d.Service().PauseQueue(ctx, queueName))
			leases := make([]pb.QueueLeaseResponse, 2)
			errs := make([]error, 2)
			backgroundCall{
				Call: func() {
					concurrently(2, func(i int) {
						errs[i] = c.QueueLease(ctx, &pb.QueueLeaseRequest{
							ClientId:       random.String("client-", 10),
							RequestTimeout: "10s",
							QueueName:      queueName,
							BatchSize:      1,
						}, &leases[i])
					})
				},
				While: func() {
					require.NoError(t, untilLeaseClientWaiting(t, c, queueName, 2))
					// Resuming runs one pass that places A on one request and B on the other.
					// A's update applies and B's is refused, so only B's request keeps waiting.
					require.NoError(t, d.Service().ResumeQueue(ctx, queueName))
					require.NoError(t, untilLeaseClientWaiting(t, c, queueName, 1))
					assert.True(t, isLeased(t, ctx, c, queueName, "A"))
					assert.False(t, isLeased(t, ctx, c, queueName, "B"))

					// Resuming again runs another pass that fails B's lease again. The counts show
					// only A as leased.
					require.NoError(t, d.Service().PauseQueue(ctx, queueName))
					require.NoError(t, d.Service().ResumeQueue(ctx, queueName))
					require.NoError(t, untilLeaseClientWaiting(t, c, queueName, 1))
					assert.Equal(t, leaseCounts{UnLeased: 1, TotalLeased: 1}, partitionLeaseCounts(t, ctx, c, queueName))

					fault.Allow(t)
					require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
						QueueName:      queueName,
						RequestTimeout: "5s",
						Items:          []*pb.QueueProduceItem{{Reference: "C", Bytes: []byte("c")}},
					}))
				},
			}.run()

			var references []string
			for i := range leases {
				require.NoError(t, errs[i])
				require.Len(t, leases[i].Items, 1)
				references = append(references, leases[i].Items[0].Reference)
				assert.True(t, isLeased(t, ctx, c, queueName, itemReference(leases[i].Items[0].Reference)))
			}
			slices.Sort(references)
			assert.Equal(t, []string{"A", "B"}, references)
			assert.False(t, isLeased(t, ctx, c, queueName, "C"))
		})
	})
}

// postgresLeaseFault finds the table of the only partition in the database and refuses a
// lease of B with a CHECK constraint on it. The table must already exist.
func postgresLeaseFault(t *testing.T, ctx context.Context, dsn string) leaseFault {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)

	var table string
	require.NoError(t, conn.QueryRow(ctx,
		`SELECT tablename FROM pg_tables WHERE tablename LIKE 'items\_%'`).Scan(&table))
	name := pgx.Identifier{table}.Sanitize()

	return leaseFault{
		Refuse: func(t *testing.T) {
			_, err := conn.Exec(ctx, `ALTER TABLE `+name+
				` ADD CONSTRAINT refuse_lease_b CHECK (NOT is_leased OR reference <> 'B') NOT VALID`)
			require.NoError(t, err)
		},
		Allow: func(t *testing.T) {
			_, err := conn.Exec(ctx, `ALTER TABLE `+name+` DROP CONSTRAINT refuse_lease_b`)
			require.NoError(t, err)
		},
		Close: func(t *testing.T) { require.NoError(t, conn.Close(ctx)) },
	}
}

// mongoLeaseFault finds the collection of the only partition in the database and refuses a
// lease of B with a collection validator. The collection must already exist.
func mongoLeaseFault(t *testing.T, ctx context.Context, dbName string) leaseFault {
	t.Helper()
	container, err := getSharedMongoContainer()
	require.NoError(t, err)
	client, err := mongo.Connect(ctx, mongoopts.Client().ApplyURI(container.uri))
	require.NoError(t, err)

	db := client.Database(dbName)
	names, err := db.ListCollectionNames(ctx, bson.M{"name": bson.M{"$regex": "^items_"}})
	require.NoError(t, err)
	require.Len(t, names, 1)

	return leaseFault{
		Refuse: func(t *testing.T) {
			require.NoError(t, db.RunCommand(ctx, bson.D{
				{Key: "collMod", Value: names[0]},
				{Key: "validator", Value: bson.M{"$or": bson.A{
					bson.M{"is_leased": false},
					bson.M{"reference": bson.M{"$ne": "B"}},
				}}},
				{Key: "validationLevel", Value: "strict"},
				{Key: "validationAction", Value: "error"},
			}).Err())
		},
		Allow: func(t *testing.T) {
			require.NoError(t, db.RunCommand(ctx, bson.D{
				{Key: "collMod", Value: names[0]},
				{Key: "validator", Value: bson.M{}},
			}).Err())
		},
		Close: func(t *testing.T) { require.NoError(t, client.Disconnect(ctx)) },
	}
}

// produceLeaseFaultItems creates a single partition queue holding items A and B.
func produceLeaseFaultItems(t *testing.T, ctx context.Context, c *querator.Client) string {
	t.Helper()
	queueName := random.String("queue-", 10)
	createQueueAndWait(t, ctx, c, &pb.QueueInfo{
		LeaseTimeout:        LeaseTimeout,
		ExpireTimeout:       ExpireTimeout,
		QueueName:           queueName,
		RequestedPartitions: 1,
	})
	require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
		QueueName:      queueName,
		RequestTimeout: "5s",
		Items: []*pb.QueueProduceItem{
			{Reference: "A", Bytes: []byte("a")},
			{Reference: "B", Bytes: []byte("b")},
		},
	}))
	return queueName
}

// leaseAndCompleteReferences leases up to two items, checks storage shows each as leased,
// completes them and returns their references. A complete is refused for an item that is
// not leased, so it fails when the consumer was handed an item storage never leased.
func leaseAndCompleteReferences(t *testing.T, ctx context.Context, c *querator.Client, queueName string) []string {
	t.Helper()
	var lease pb.QueueLeaseResponse
	require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
		ClientId:       random.String("client-", 10),
		RequestTimeout: "3s",
		QueueName:      queueName,
		BatchSize:      2,
	}, &lease))

	var references, ids []string
	for _, item := range lease.Items {
		references = append(references, item.Reference)
		ids = append(ids, item.Id)
		assert.True(t, isLeased(t, ctx, c, queueName, itemReference(item.Reference)))
	}
	require.NoError(t, c.QueueComplete(ctx, &pb.QueueCompleteRequest{
		QueueName:      queueName,
		RequestTimeout: "5s",
		Partition:      lease.Partition,
		Ids:            ids,
	}))
	return references
}

// leaseCounts is the partition's in-memory count of un-leased and leased items, as queue stats
// report it.
type leaseCounts struct {
	UnLeased    int32
	TotalLeased int32
}

// partitionLeaseCounts returns the lease counts of the queue's only partition.
func partitionLeaseCounts(t *testing.T, ctx context.Context, c *querator.Client, queueName string) leaseCounts {
	t.Helper()
	var stats pb.QueueStatsResponse
	require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats))
	require.Len(t, stats.LogicalQueues, 1)
	require.Len(t, stats.LogicalQueues[0].Partitions, 1)
	p := stats.LogicalQueues[0].Partitions[0]
	return leaseCounts{UnLeased: p.UnLeased, TotalLeased: p.TotalLeased}
}
