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

	t.Run("ConfigFileListingAboveLimitFailsStart", func(t *testing.T) {
		listed := func(count int) []daemon.Partition {
			partitions := make([]daemon.Partition, count)
			for i := range partitions {
				partitions[i] = daemon.Partition{Partition: i, StorageName: "mem-00"}
			}
			return partitions
		}
		file := daemon.File{
			PartitionStorage: []daemon.PartitionStorage{
				{Name: "mem-00", Memory: &daemon.MemoryConfig{}, Affinity: 1},
			},
			QueueStorage: daemon.QueueStorage{Badger: &daemon.BadgerConfig{StorageDir: t.TempDir()}},
			Queues: []daemon.Queue{{
				Name:                "queue-listed",
				LeaseTimeout:        time.Minute,
				ExpireTimeout:       10 * time.Minute,
				RequestedPartitions: 2,
				Partitions:          listed(2),
			}},
		}

		// The first start saves the queue, so the second start finds it in the queue store
		first := &daemon.Config{}
		require.NoError(t, daemon.ApplyConfigFile(context.Background(), first, file, io.Discard))
		require.NoError(t, first.Service.StorageConfig.Queues.Close(context.Background()))

		file.Queues[0].RequestedPartitions = 1_025
		file.Queues[0].Partitions = listed(1_025)
		second := &daemon.Config{}
		err := daemon.ApplyConfigFile(context.Background(), second, file, io.Discard)
		require.ErrorContains(t, err, "invalid queue 'queue-listed'; lists 1025 partitions, cannot be more than 1024")
		require.NoError(t, second.Service.StorageConfig.Queues.Close(context.Background()))
	})
}
