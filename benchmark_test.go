package querator_test

import (
	"context"
	"fmt"
	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/daemon"
	"github.com/kapetan-io/querator/internal/store"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/require"
	"math/rand"
	"runtime"
	"testing"
)

func BenchmarkProduce(b *testing.B) {
	fmt.Printf("Current Operating System has '%d' CPUs\n", runtime.NumCPU())
	//bdb := boltTestSetup{Dir: b.TempDir()}

	for _, tc := range []struct {
		Setup    NewStorageFunc
		TearDown func()
		Name     string
	}{
		{
			Name: "InMemory",
			Setup: func(cp *clock.Provider) store.StorageConfig {
				return setupMemoryStorage(store.StorageConfig{Clock: cp})
			},
			TearDown: func() {},
		},
			
		//{
		//	Name: "BoltDB",
		//	Setup: func(cp *clock.Provider) store.StorageConfig {
		//		return bdb.Setup(store.BoltConfig{Clock: cp})
		//	},
		//	TearDown: func() {
		//		bdb.Teardown()
		//	},
		//},
		//{
		//	Name: "SurrealDB",
		//},
		//{
		//	Name: "PostgresSQL",
		//},
	} {
		b.Run(tc.Name, func(b *testing.B) {
			items := generateProduceItems(1_000)
			mask := len(items) - 1

			d, err := daemon.NewDaemon(context.Background(), daemon.Config{
				ServiceConfig: querator.ServiceConfig{
					StorageConfig: tc.Setup(clock.NewProvider()),
					Logger:        log,
				},
			})
			require.NoError(b, err)
			defer func() {
				_ = d.Shutdown(context.Background())
			}()
			s := d.Service()
			require.NoError(b, s.QueuesCreate(context.Background(), &pb.QueueInfo{
				QueueName:      "bench-queue",
				DeadTimeout:    "24h0m0s",
				ReserveTimeout: "1m0s",
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

		b.Run(tc.Name, func(b *testing.B) {
			d, err := daemon.NewDaemon(context.Background(), daemon.Config{
				ServiceConfig: querator.ServiceConfig{
					StorageConfig: tc.Setup(clock.NewProvider()),
					Logger:        log,
				},
			})
			require.NoError(b, err)
			defer func() {
				fmt.Printf("Shutdown\n")
				_ = d.Shutdown(context.Background())
			}()
			s := d.Service()

			b.Run("QueuesCreate", func(b *testing.B) {
				start := clock.Now()
				b.ResetTimer()

				for n := 0; n < b.N; n++ {
					timeOuts := random.Slice(validTimeouts)
					info := pb.QueueInfo{
						QueueName:      random.String("queue-", 10),
						DeadQueue:      random.String("dead-", 10),
						Reference:      random.String("ref-", 10),
						MaxAttempts:    int32(rand.Intn(100)),
						ReserveTimeout: timeOuts.Reserve,
						DeadTimeout:    timeOuts.Dead,
					}

					err = s.QueuesCreate(context.Background(), &info)
					if err != nil {
						fmt.Printf("Error creating queue: %s\n", err)
						b.Error(err)
						return
					}
				}

				opsPerSec := float64(b.N) / clock.Since(start).Seconds()
				b.ReportMetric(opsPerSec, "ops/s")
			})
		})
	}
}

func generateProduceItems(size int) []*pb.QueueProduceItem {
	items := make([]*pb.QueueProduceItem, 0, size)
	for i := 0; i < size; i++ {
		items = append(items, &pb.QueueProduceItem{
			Bytes:     []byte(fmt.Sprintf("%d-%s", i, random.String("payload-", 256))),
			Reference: random.String("ref-", 10),
			Encoding:  random.String("enc-", 10),
			Kind:      random.String("kind-", 10),
		})
	}
	return items
}
