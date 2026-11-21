package service_test

import (
	"context"
	"errors"
	"io"
	"math"
	"testing"
	"time"

	"github.com/duh-rpc/duh-go"
	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/daemon"
	pb "github.com/kapetan-io/querator/proto"
	svc "github.com/kapetan-io/querator/service"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestQueuePartitionLimit keeps partitions on the in-memory backend only. Badger opens a separate
// database for every partition, measured at about 85 MB of heap each.
func TestQueuePartitionLimit(t *testing.T) {
	t.Run("CreateAtLimit", func(t *testing.T) {
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setupMemoryStorage(svc.Config{}.StorageConfig)})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()

		name := random.String("queue-", 10)
		require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
			QueueName:           name,
			LeaseTimeout:        "1m",
			ExpireTimeout:       "10m",
			RequestedPartitions: 1_024,
		}))

		var info pb.QueueInfo
		require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: name}, &info))
		assert.Equal(t, int32(1_024), info.RequestedPartitions)
		assert.Len(t, info.Partitions, 1_024)
	})

	t.Run("RejectedCreateLeavesNoQueue", func(t *testing.T) {
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setupMemoryStorage(svc.Config{}.StorageConfig)})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()

		// Placing math.MaxInt32 partitions would exhaust memory, so this passes only if the
		// limit is checked before placement
		err := c.QueuesCreate(ctx, &pb.QueueInfo{
			QueueName:           random.String("queue-", 10),
			LeaseTimeout:        "1m",
			ExpireTimeout:       "10m",
			RequestedPartitions: math.MaxInt32,
		})
		var e duh.Error
		require.True(t, errors.As(err, &e))
		assert.Equal(t, duh.CodeBadRequest, e.Code())
		assert.Equal(t, "partitions is invalid; cannot be greater than 1024", e.Message())

		var list pb.QueuesListResponse
		require.NoError(t, c.QueuesList(ctx, &list, &querator.ListOptions{}))
		assert.Empty(t, list.Items)
	})

	// A config file that lists its partitions skips placement, which is how a queue with more
	// partitions than the create limit exists in storage. Queue records go to Badger so they
	// survive the restart below, while partitions stay in memory.
	t.Run("StoredQueueAboveLimitKeepsWorking", func(t *testing.T) {
		const count = 1_500
		name := random.String("queue-", 10)
		partitions := make([]daemon.Partition, count)
		for i := range partitions {
			partitions[i] = daemon.Partition{Partition: i, StorageName: "mem-00"}
		}

		conf := &daemon.Config{}
		require.NoError(t, daemon.ApplyConfigFile(context.Background(), conf, daemon.File{
			PartitionStorage: []daemon.PartitionStorage{
				{Name: "mem-00", Memory: &daemon.MemoryConfig{}, Affinity: 1},
			},
			QueueStorage: daemon.QueueStorage{Badger: &daemon.BadgerConfig{StorageDir: t.TempDir()}},
			Queues: []daemon.Queue{{
				Name:                name,
				LeaseTimeout:        time.Minute,
				ExpireTimeout:       10 * time.Minute,
				RequestedPartitions: count,
				Partitions:          partitions,
			}},
		}, io.Discard))

		func() {
			d := newDaemon(t, 10*clock.Second, conf.Service)
			defer d.Shutdown(t)
			c, ctx := d.MustClient(), d.Context()

			var info pb.QueueInfo
			require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: name}, &info))
			assert.Len(t, info.Partitions, count)

			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      name,
				RequestTimeout: "1m",
				Items:          produceRandomItems(10),
			}))
			var lease pb.QueueLeaseResponse
			require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
				ClientId:       random.String("client-", 10),
				RequestTimeout: "5s",
				QueueName:      name,
				BatchSize:      1,
			}, &lease))
			assert.Len(t, lease.Items, 1)

			require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
				QueueName: name,
				Reference: new("updated"),
			}))
		}()

		// The restart proves the queue reloads from storage with the update applied.
		d := newDaemon(t, 10*clock.Second, conf.Service)
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()

		var info pb.QueueInfo
		require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: name}, &info))
		assert.Equal(t, "updated", info.Reference)
		assert.Equal(t, int32(count), info.RequestedPartitions)
		assert.Len(t, info.Partitions, count)
	})
}
