package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/duh-rpc/duh-go"
	"github.com/kapetan-io/querator/daemon"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	pb "github.com/kapetan-io/querator/proto"
	svc "github.com/kapetan-io/querator/service"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// memoryBackends returns memory storage whose partition backends are replaced by the given
// name and affinity pairs.
func memoryBackends(backends ...store.PartitionStorage) store.Config {
	conf := setupMemoryStorage(store.Config{})
	conf.PartitionStorage = nil
	for _, b := range backends {
		b.PartitionStore = store.NewMemoryPartitionStore(conf, log)
		conf.PartitionStorage = append(conf.PartitionStorage, b)
	}
	return conf
}

func TestPlacement(t *testing.T) {
	t.Run("DuplicateBackendNamesRefuseStartup", func(t *testing.T) {
		conf := memoryBackends(
			store.PartitionStorage{Name: "memory-0", Affinity: 1},
			store.PartitionStorage{Name: "memory-0", Affinity: 1},
		)

		ctx, cancel := context.WithTimeout(context.Background(), 10*clock.Second)
		defer cancel()
		_, err := svc.New(ctx, svc.Config{StorageConfig: conf, Log: log})
		require.Error(t, err)
		assert.ErrorContains(t, err, "memory-0")
		assert.ErrorContains(t, err, "duplicate")
	})

	t.Run("NoBackendAboveZeroRefusesQueueCreate", func(t *testing.T) {
		for _, test := range []struct {
			name     string
			affinity float64
		}{
			{name: "ZeroAffinity", affinity: 0},
			{name: "NegativeAffinity", affinity: -1},
		} {
			t.Run(test.name, func(t *testing.T) {
				conf := memoryBackends(store.PartitionStorage{Name: "memory-0", Affinity: test.affinity})
				d, c, ctx := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
				defer d.Shutdown(t)

				name := random.String("queue-", 10)
				err := c.QueuesCreate(ctx, &pb.QueueInfo{
					QueueName:           name,
					LeaseTimeout:        "1m",
					ExpireTimeout:       "10m",
					RequestedPartitions: 4,
				})
				require.Error(t, err)
				var duhErr duh.Error
				require.True(t, errors.As(err, &duhErr))
				assert.Equal(t, duh.CodeRequestFailed, duhErr.Code())
				assert.Contains(t, duhErr.Message(), "no storage backend can take partitions")

				// No queue was created
				var list pb.QueuesListResponse
				require.NoError(t, c.QueuesList(ctx, &list, nil))
				assert.Empty(t, list.Items)

				// Querator is still serving requests
				err = c.QueueProduce(ctx, &pb.QueueProduceRequest{
					QueueName:      name,
					RequestTimeout: "1s",
					Items:          []*pb.QueueProduceItem{{Bytes: []byte("x")}},
				})
				require.Error(t, err)
				assert.ErrorContains(t, err, "does not exist")
			})
		}
	})

	t.Run("QueueWithoutPartitionsReturnsErrors", func(t *testing.T) {
		conf := setupMemoryStorage(store.Config{})
		name := random.String("queue-", 10)
		// A queue stored before placement was refused, with no partitions.
		require.NoError(t, conf.Queues.Add(context.Background(), types.QueueInfo{
			Name:                name,
			LeaseTimeout:        clock.Minute,
			ExpireTimeout:       10 * clock.Minute,
			RequestedPartitions: 4,
		}))
		d, c, ctx := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
		defer d.Shutdown(t)

		err := c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      name,
			RequestTimeout: "1s",
			Items:          []*pb.QueueProduceItem{{Bytes: []byte("x")}},
		})
		require.Error(t, err)
		assert.ErrorContains(t, err, "no available partitions")

		err = c.QueueLease(ctx, &pb.QueueLeaseRequest{
			QueueName:      name,
			ClientId:       "client-1",
			BatchSize:      1,
			RequestTimeout: "1s",
		}, &pb.QueueLeaseResponse{})
		require.Error(t, err)
		assert.ErrorContains(t, err, "no available partitions")

		err = c.QueueComplete(ctx, &pb.QueueCompleteRequest{
			QueueName:      name,
			Partition:      0,
			RequestTimeout: "1s",
			Ids:            []string{"id-1"},
		})
		var duhErr duh.Error
		require.True(t, errors.As(err, &duhErr))
		assert.Equal(t, duh.CodeBadRequest, duhErr.Code())
		assert.Contains(t, duhErr.Message(), "partition is invalid")

		err = c.QueueRetry(ctx, &pb.QueueRetryRequest{
			QueueName: name,
			Partition: 0,
			Items:     []*pb.QueueRetryItem{{Id: "id-1"}},
		})
		require.True(t, errors.As(err, &duhErr))
		assert.Equal(t, duh.CodeBadRequest, duhErr.Code())
		assert.Contains(t, duhErr.Message(), "partition is invalid")

		// Process is alive and the queue can still be inspected and removed
		var info pb.QueueInfo
		require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: name}, &info))
		require.NoError(t, c.QueuesDelete(ctx, &pb.QueuesDeleteRequest{QueueName: name}))
	})

	t.Run("ConfigFileQueueWithoutPartitionsAndNoBackendAboveZero", func(t *testing.T) {
		file := daemon.File{
			PartitionStorage: []daemon.PartitionStorage{
				{Name: "mem-00", Memory: &daemon.MemoryConfig{}},
			},
			QueueStorage: daemon.QueueStorage{Memory: &daemon.MemoryConfig{}},
			Queues:       []daemon.Queue{{Name: "queue-1", RequestedPartitions: 2}},
		}
		err := daemon.ApplyConfigFile(context.Background(), &daemon.Config{}, file, nil)
		require.Error(t, err)
		assert.ErrorContains(t, err, "queue-1")
		assert.ErrorContains(t, err, "no storage backend can take partitions")
	})

	t.Run("DuplicateBackendNamesInConfigFileRefuseStartup", func(t *testing.T) {
		file := daemon.File{
			PartitionStorage: []daemon.PartitionStorage{
				{Name: "mem-00", Memory: &daemon.MemoryConfig{}, Affinity: 1},
				{Name: "mem-00", Memory: &daemon.MemoryConfig{}, Affinity: 1},
			},
		}
		conf := &daemon.Config{}
		require.NoError(t, daemon.ApplyConfigFile(context.Background(), conf, file, nil))
		ctx, cancel := context.WithTimeout(context.Background(), 10*clock.Second)
		defer cancel()
		_, err := daemon.NewDaemon(ctx, *conf)
		require.Error(t, err)
		assert.ErrorContains(t, err, "mem-00")
	})

	t.Run("RestartKeepsStoredConfigQueueOnAffinityZeroBackends", func(t *testing.T) {
		dir := t.TempDir()
		file := daemon.File{
			PartitionStorage: []daemon.PartitionStorage{
				{Name: "badger-0", Badger: &daemon.BadgerConfig{StorageDir: dir}},
			},
			QueueStorage: daemon.QueueStorage{Badger: &daemon.BadgerConfig{StorageDir: dir}},
		}

		// A queue stored while a backend could still place partitions
		first := &daemon.Config{}
		require.NoError(t, daemon.ApplyConfigFile(context.Background(), first, file, nil))
		require.NoError(t, first.Service.StorageConfig.Queues.Add(context.Background(), types.QueueInfo{
			Name:                "queue-1",
			LeaseTimeout:        clock.Minute,
			ExpireTimeout:       10 * clock.Minute,
			RequestedPartitions: 2,
		}))
		require.NoError(t, first.Service.StorageConfig.Queues.Close(context.Background()))

		// Restart with the queue listed without partitions and every backend at affinity 0
		file.Queues = []daemon.Queue{{Name: "queue-1", RequestedPartitions: 2}}
		second := &daemon.Config{}
		require.NoError(t, daemon.ApplyConfigFile(context.Background(), second, file, nil))
		require.NoError(t, second.Service.StorageConfig.Queues.Close(context.Background()))
	})
}
