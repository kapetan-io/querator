package daemon_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"runtime"
	"testing"

	"github.com/kapetan-io/querator/daemon"
	"github.com/kapetan-io/querator/internal/store"
	pb "github.com/kapetan-io/querator/proto"
	svc "github.com/kapetan-io/querator/service"
	"github.com/kapetan-io/tackle/clock"
	"github.com/stretchr/testify/require"
)

func BenchmarkProduce(b *testing.B) {
	b.Logf("Current Operating System has '%d' CPUs\n", runtime.NumCPU())

	log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))

	for _, tc := range []struct {
		Setup    func() store.Config
		TearDown func()
		Name     string
	}{
		{
			Name: "InMemory",
			Setup: func() store.Config {
				return store.Config{
					Queues: store.NewMemoryQueues(log),
					PartitionStorage: []store.PartitionStorage{
						{
							PartitionStore: store.NewMemoryPartitionStore(store.Config{}, log),
							Name:           "memory-0",
							Affinity:       1,
						},
					},
				}
			},
			TearDown: func() {},
		},
	} {
		b.Run(tc.Name, func(b *testing.B) {
			items := generateBenchProduceItems(1_000)
			mask := len(items) - 1

			d, err := daemon.NewDaemon(context.Background(), daemon.Config{
				Service: svc.ServiceConfig{
					StorageConfig: tc.Setup(),
					Log:           log,
				},
			})
			require.NoError(b, err)
			defer func() {
				_ = d.Shutdown(context.Background())
			}()
			s := d.Service()
			require.NoError(b, s.QueuesCreate(context.Background(), &pb.QueueInfo{
				QueueName:           "bench-queue",
				ExpireTimeout:       "24h0m0s",
				LeaseTimeout:        "1m0s",
				RequestedPartitions: 1,
			}))

			for _, p := range []int{1, 8, 24, 32} {
				b.Run(fmt.Sprintf("Produce_%d", p), func(b *testing.B) {
					runtime.GOMAXPROCS(p)
					start := clock.Now()
					b.ResetTimer()

					b.RunParallel(func(p *testing.PB) {
						index := int(rand.Uint32() & uint32(mask))

						for p.Next() {
							err := s.QueueProduce(context.Background(), &pb.QueueProduceRequest{
								Items:          items[index&mask : index+1&mask],
								QueueName:      "bench-queue",
								RequestTimeout: "1m",
							})
							if err != nil {
								b.Error(err)
								return
							}
						}

					})
					opsPerSec := float64(b.N) / clock.Since(start).Seconds()
					b.ReportMetric(opsPerSec, "ops/s")
				})
			}
		})
	}
}

func generateBenchProduceItems(size int) []*pb.QueueProduceItem {
	items := make([]*pb.QueueProduceItem, 0, size)
	for i := 0; i < size; i++ {
		items = append(items, &pb.QueueProduceItem{
			Bytes:     []byte(fmt.Sprintf("%d-payload-%d", i, i)),
			Reference: fmt.Sprintf("ref-%d", i),
			Encoding:  "bytes",
			Kind:      "benchmark",
		})
	}
	return items
}
