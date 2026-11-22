package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	pb "github.com/kapetan-io/querator/proto"
	svc "github.com/kapetan-io/querator/service"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	mongodriver "go.mongodb.org/mongo-driver/mongo"
	mongoopts "go.mongodb.org/mongo-driver/mongo/options"
)

// A queue stored before negative timeouts were refused must stay updatable through the API.
// The InMemory and BadgerDB records are seeded in internal/store, where their storage is reachable.
func TestQueuesUpdateOnStoredNegativeLease(t *testing.T) {
	postgres := postgresTestSetup{}
	mongo := mongoTestSetup{}

	for _, tc := range []struct {
		Setup    NewStorageFunc
		Seed     func(t *testing.T, ctx context.Context, name string, lease time.Duration)
		TearDown func()
		Name     string
	}{
		{
			Name: "PostgreSQL",
			Setup: func() store.Config {
				return withMemoryAuth(postgres.Setup(store.PostgresConfig{}))
			},
			Seed: func(t *testing.T, ctx context.Context, name string, lease time.Duration) {
				conn, err := pgx.Connect(ctx, postgres.dsn)
				require.NoError(t, err)
				defer func() { _ = conn.Close(ctx) }()
				_, err = conn.Exec(ctx, "UPDATE queues SET lease_timeout_ns = $1 WHERE name = $2",
					lease.Nanoseconds(), name)
				require.NoError(t, err)
			},
			TearDown: postgres.Teardown,
		},
		{
			Name: "MongoDB",
			Setup: func() store.Config {
				return withMemoryAuth(mongo.Setup(store.MongoConfig{}))
			},
			Seed: func(t *testing.T, ctx context.Context, name string, lease time.Duration) {
				container, err := getSharedMongoContainer()
				require.NoError(t, err)
				client, err := mongodriver.Connect(ctx, mongoopts.Client().ApplyURI(container.uri))
				require.NoError(t, err)
				defer func() { _ = client.Disconnect(ctx) }()
				_, err = client.Database(mongo.dbName).Collection("queues").UpdateOne(ctx,
					bson.M{"_id": name}, bson.M{"$set": bson.M{"lease_timeout_ns": lease.Nanoseconds()}})
				require.NoError(t, err)
			},
			TearDown: mongo.Teardown,
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			conf := tc.Setup()
			d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
			defer func() {
				d.Shutdown(t)
				tc.TearDown()
			}()
			c, ctx := d.MustClient(), d.Context()

			seed := func(t *testing.T) string {
				name := random.String("queue-", 10)
				require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
					QueueName:           name,
					LeaseTimeout:        "1m",
					ExpireTimeout:       "10m",
					RequestedPartitions: 1,
				}))
				tc.Seed(t, ctx, name, -5*time.Minute)
				return name
			}

			t.Run("ReferenceOnly", func(t *testing.T) {
				name := seed(t)
				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
					QueueName: name,
					Reference: new("owner"),
				}))

				var stored types.QueueInfo
				require.NoError(t, conf.Queues.Get(ctx, name, &stored))
				assert.Equal(t, "owner", stored.Reference)
				assert.Equal(t, -5*time.Minute, stored.LeaseTimeout)
			})

			t.Run("PositiveLeaseRepairs", func(t *testing.T) {
				name := seed(t)
				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
					QueueName:    name,
					LeaseTimeout: "2m",
				}))

				var stored types.QueueInfo
				require.NoError(t, conf.Queues.Get(ctx, name, &stored))
				assert.Equal(t, 2*time.Minute, stored.LeaseTimeout)
			})

			t.Run("DifferentNegativeRefused", func(t *testing.T) {
				name := seed(t)
				require.Error(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
					QueueName:    name,
					LeaseTimeout: "-6m",
				}))

				var stored types.QueueInfo
				require.NoError(t, conf.Queues.Get(ctx, name, &stored))
				assert.Equal(t, -5*time.Minute, stored.LeaseTimeout)
			})
		})
	}
}
