package bench_test

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
	"log/slog"
	"math/rand"
	"os"
	"runtime"
	"testing"
)

// var log = slog.New(slog.NewTextHandler(io.Discard, nil))
var log = slog.New(slog.NewTextHandler(os.Stdout, nil))

type NewStorageFunc func() store.Storage

func BenchmarkProduce(b *testing.B) {
	fmt.Printf("Current Operating System has '%d' CPUs\n", runtime.NumCPU())
	bdb := store.BoltDBTesting{Dir: b.TempDir()}

	testCases := []struct {
		Setup    NewStorageFunc
		TearDown func()
		Name     string
	}{
		{
			Name: "BoltDB",
			Setup: func() store.Storage {
				return bdb.Setup(store.BoltConfig{})
			},
			TearDown: func() {
				bdb.Teardown()
			},
		},
		{
			Name: "InMemory",
			Setup: func() store.Storage {
				return store.NewMemoryBackend(store.MemoryBackendConfig{})
			},
			TearDown: func() {

			},
		},
		//{
		//	Name: "PostgresSQL",
		//},
	}

	for _, tc := range testCases {
		b.Run(tc.Name, func(b *testing.B) {
			items := generateProduceItems(1_000)
			mask := len(items) - 1

			d, err := daemon.NewDaemon(context.Background(), daemon.Config{
				ServiceConfig: querator.ServiceConfig{
					Storage: tc.Setup(),
					Logger:  log,
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
	}
}

type Pair struct {
	Reserve string
	Dead    string
}

var validTimeouts = []Pair{
	{
		Reserve: "15s",
		Dead:    "1m0s",
	},
	{
		Reserve: "1m0s",
		Dead:    "10m0s",
	},
	{
		Reserve: "10m0s",
		Dead:    "24h0m0s",
	},
	{
		Reserve: "30m0s",
		Dead:    "1h0m0s",
	},
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

func BenchmarkQueuesCreate(b *testing.B) {
	fmt.Printf("Current Operating System has '%d' CPUs\n", runtime.NumCPU())
	bdb := store.BoltDBTesting{Dir: b.TempDir()}
	//bdb := store.BoltDBTesting{Dir: "/tmp/querator"}

	testCases := []struct {
		Setup    NewStorageFunc
		TearDown func()
		Name     string
	}{
		{
			Name: "BoltDB",
			Setup: func() store.Storage {
				return bdb.Setup(store.BoltConfig{})
			},
			TearDown: func() {
				bdb.Teardown()
			},
		},
		{
			Name: "InMemory",
			Setup: func() store.Storage {
				return store.NewMemoryBackend(store.MemoryBackendConfig{})
			},
			TearDown: func() {},
		},
		//{
		//	Name: "PostgresSQL",
		//},
	}

	for _, tc := range testCases {
		b.Run(tc.Name, func(b *testing.B) {
			//items := generateQueueInfo(100_000)

			d, err := daemon.NewDaemon(context.Background(), daemon.Config{
				ServiceConfig: querator.ServiceConfig{
					Storage: tc.Setup(),
					Logger:  log,
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
