package bench_test

import (
	"context"
	"fmt"
	"github.com/kapetan-io/querator/daemon"
	"github.com/kapetan-io/querator/internal/store"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/require"
	"io"
	"log/slog"
	"math/rand"
	"runtime"
	"testing"
	"time"
)

var log = slog.New(slog.NewTextHandler(io.Discard, nil))

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
				return bdb.Setup(store.BoltOptions{})
			},
			TearDown: func() {
				bdb.Teardown()
			},
		},
		{
			Name: "MemoryDB",
			Setup: func() store.Storage {
				return store.NewMemoryStorage()
			},
			TearDown: func() {},
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
				Store:  tc.Setup(),
				Logger: log,
			})
			require.NoError(b, err)
			defer func() {
				_ = d.Shutdown(context.Background())
			}()
			s := d.Service()
			require.NoError(b, s.QueuesCreate(context.Background(), &pb.QueueInfo{QueueName: "bench-queue"}))

			for _, p := range []int{1, 8, 24, 32} {
				b.Run(fmt.Sprintf("Produce_%d", p), func(b *testing.B) {
					runtime.GOMAXPROCS(p)
					start := time.Now()
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
					opsPerSec := float64(b.N) / time.Since(start).Seconds()
					b.ReportMetric(opsPerSec, "ops/s")
				})
			}
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
