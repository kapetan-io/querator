package daemon_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/kapetan-io/querator/daemon"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/stretchr/testify/require"
)

func BenchmarkProduceOperations(b *testing.B) {
	tests := []struct {
		payloadSize PayloadSize
		batchSize   int
	}{
		{Payload100B, 1},
		{Payload100B, 10},
		{Payload100B, 100},
		{Payload1KB, 1},
		{Payload1KB, 10},
		{Payload1KB, 100},
		{Payload10KB, 1},
		{Payload10KB, 10},
	}

	for _, test := range tests {
		name := fmt.Sprintf("payload=%d/batch=%d", test.payloadSize, test.batchSize)
		b.Run(name, func(b *testing.B) {
			ctx := context.Background()
			d, err := daemon.NewDaemon(ctx, daemon.Config{
				ListenAddress: "localhost:0",
			})
			require.NoError(b, err)
			defer func() { _ = d.Shutdown(ctx) }()

			client := d.MustClient()

			err = client.QueuesCreate(ctx, &pb.QueueInfo{
				RequestedPartitions: 1,
				LeaseTimeout:        "1m",
				ExpireTimeout:       "24h",
				QueueName:           "benchmark",
			})
			require.NoError(b, err)

			items := GenerateProduceItems(test.batchSize, test.payloadSize, benchmarkSeed)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := client.QueueProduce(ctx, &pb.QueueProduceRequest{
					QueueName:      "benchmark",
					RequestTimeout: "30s",
					Items:          items,
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkLeaseOperations(b *testing.B) {
	tests := []struct {
		payloadSize PayloadSize
		batchSize   int
		queueDepth  int
	}{
		{Payload100B, 1, 1000},
		{Payload100B, 10, 1000},
		{Payload100B, 100, 1000},
		{Payload1KB, 1, 1000},
		{Payload1KB, 10, 1000},
		{Payload1KB, 100, 1000},
		{Payload10KB, 1, 500},
		{Payload10KB, 10, 500},
	}

	for _, test := range tests {
		name := fmt.Sprintf("payload=%d/batch=%d", test.payloadSize, test.batchSize)
		b.Run(name, func(b *testing.B) {
			ctx := context.Background()
			d, err := daemon.NewDaemon(ctx, daemon.Config{
				ListenAddress: "localhost:0",
			})
			require.NoError(b, err)
			defer func() { _ = d.Shutdown(ctx) }()

			client := d.MustClient()

			err = client.QueuesCreate(ctx, &pb.QueueInfo{
				RequestedPartitions: 1,
				LeaseTimeout:        "1m",
				ExpireTimeout:       "24h",
				QueueName:           "benchmark",
			})
			require.NoError(b, err)

			// Pre-populate queue with items
			maxBatchSize := 100
			if int(test.payloadSize)*maxBatchSize > 900000 {
				maxBatchSize = 900000 / int(test.payloadSize)
				if maxBatchSize < 1 {
					maxBatchSize = 1
				}
			}

			totalNeeded := b.N * test.batchSize
			if totalNeeded < test.queueDepth {
				totalNeeded = test.queueDepth
			}

			batches := (totalNeeded + maxBatchSize - 1) / maxBatchSize
			for i := 0; i < batches; i++ {
				remaining := totalNeeded - (i * maxBatchSize)
				if remaining > maxBatchSize {
					remaining = maxBatchSize
				}
				items := GenerateProduceItems(remaining, test.payloadSize, benchmarkSeed+i)
				err = client.QueueProduce(ctx, &pb.QueueProduceRequest{
					QueueName:      "benchmark",
					RequestTimeout: "30s",
					Items:          items,
				})
				require.NoError(b, err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var resp pb.QueueLeaseResponse
				err := client.QueueLease(ctx, &pb.QueueLeaseRequest{
					QueueName:      "benchmark",
					ClientId:       "bench-client",
					RequestTimeout: "5s",
					BatchSize:      int32(test.batchSize),
				}, &resp)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkCompleteOperations(b *testing.B) {
	batchSizes := []int{1, 10, 100}

	for _, batchSize := range batchSizes {
		name := fmt.Sprintf("batch=%d", batchSize)
		b.Run(name, func(b *testing.B) {
			ctx := context.Background()
			d, err := daemon.NewDaemon(ctx, daemon.Config{
				ListenAddress: "localhost:0",
			})
			require.NoError(b, err)
			defer func() { _ = d.Shutdown(ctx) }()

			client := d.MustClient()

			err = client.QueuesCreate(ctx, &pb.QueueInfo{
				RequestedPartitions: 1,
				LeaseTimeout:        "1m",
				ExpireTimeout:       "24h",
				QueueName:           "benchmark",
			})
			require.NoError(b, err)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				// Produce items
				items := GenerateProduceItems(batchSize, Payload1KB, benchmarkSeed+i)
				err := client.QueueProduce(ctx, &pb.QueueProduceRequest{
					QueueName:      "benchmark",
					RequestTimeout: "30s",
					Items:          items,
				})
				require.NoError(b, err)

				// Lease items
				var leaseResp pb.QueueLeaseResponse
				err = client.QueueLease(ctx, &pb.QueueLeaseRequest{
					QueueName:      "benchmark",
					ClientId:       "bench-client",
					RequestTimeout: "30s",
					BatchSize:      int32(batchSize),
				}, &leaseResp)
				require.NoError(b, err)

				ids := make([]string, len(leaseResp.Items))
				for j, item := range leaseResp.Items {
					ids[j] = item.Id
				}
				b.StartTimer()

				// Complete items (this is what we're benchmarking)
				err = client.QueueComplete(ctx, &pb.QueueCompleteRequest{
					QueueName:      leaseResp.QueueName,
					Partition:      leaseResp.Partition,
					RequestTimeout: "30s",
					Ids:            ids,
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkFullWorkflow(b *testing.B) {
	configs := []struct {
		payloadSize PayloadSize
		batchSize   int
	}{
		{Payload1KB, 10},
		{Payload1KB, 100},
		{Payload10KB, 10},
	}

	for _, config := range configs {
		name := fmt.Sprintf("payload=%d/batch=%d", config.payloadSize, config.batchSize)
		b.Run(name, func(b *testing.B) {
			ctx := context.Background()
			d, err := daemon.NewDaemon(ctx, daemon.Config{
				ListenAddress: "localhost:0",
			})
			require.NoError(b, err)
			defer func() { _ = d.Shutdown(ctx) }()

			client := d.MustClient()

			err = client.QueuesCreate(ctx, &pb.QueueInfo{
				RequestedPartitions: 1,
				LeaseTimeout:        "1m",
				ExpireTimeout:       "24h",
				QueueName:           "benchmark",
			})
			require.NoError(b, err)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Produce
				items := GenerateProduceItems(config.batchSize, config.payloadSize, benchmarkSeed+i)
				err := client.QueueProduce(ctx, &pb.QueueProduceRequest{
					QueueName:      "benchmark",
					RequestTimeout: "30s",
					Items:          items,
				})
				if err != nil {
					b.Fatal(err)
				}

				// Lease
				var leaseResp pb.QueueLeaseResponse
				err = client.QueueLease(ctx, &pb.QueueLeaseRequest{
					QueueName:      "benchmark",
					ClientId:       "bench-client",
					RequestTimeout: "30s",
					BatchSize:      int32(config.batchSize),
				}, &leaseResp)
				if err != nil {
					b.Fatal(err)
				}

				// Complete
				ids := make([]string, len(leaseResp.Items))
				for j, item := range leaseResp.Items {
					ids[j] = item.Id
				}
				err = client.QueueComplete(ctx, &pb.QueueCompleteRequest{
					QueueName:      leaseResp.QueueName,
					Partition:      leaseResp.Partition,
					RequestTimeout: "30s",
					Ids:            ids,
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkThroughput(b *testing.B) {
	configs := []struct {
		payloadSize PayloadSize
		batchSize   int
	}{
		{Payload1KB, 10},
		{Payload1KB, 100},
		{Payload1KB, 1000},
		{Payload10KB, 10},
		{Payload10KB, 100},
	}

	for _, config := range configs {
		name := fmt.Sprintf("payload=%d/batch=%d", config.payloadSize, config.batchSize)
		b.Run(name, func(b *testing.B) {
			ctx := context.Background()
			d, err := daemon.NewDaemon(ctx, daemon.Config{
				ListenAddress: "localhost:0",
			})
			require.NoError(b, err)
			defer func() { _ = d.Shutdown(ctx) }()

			client := d.MustClient()

			err = client.QueuesCreate(ctx, &pb.QueueInfo{
				RequestedPartitions: 1,
				LeaseTimeout:        "1m",
				ExpireTimeout:       "24h",
				QueueName:           "benchmark",
			})
			require.NoError(b, err)

			totalItems := 0
			start := time.Now()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				items := GenerateProduceItems(config.batchSize, config.payloadSize, benchmarkSeed+i)

				err := client.QueueProduce(ctx, &pb.QueueProduceRequest{
					QueueName:      "benchmark",
					RequestTimeout: "30s",
					Items:          items,
				})
				if err != nil {
					b.Fatal(err)
				}

				var leaseResp pb.QueueLeaseResponse
				err = client.QueueLease(ctx, &pb.QueueLeaseRequest{
					QueueName:      "benchmark",
					ClientId:       "throughput-bench",
					RequestTimeout: "30s",
					BatchSize:      int32(config.batchSize),
				}, &leaseResp)
				if err != nil {
					b.Fatal(err)
				}

				ids := make([]string, len(leaseResp.Items))
				for j, item := range leaseResp.Items {
					ids[j] = item.Id
				}

				err = client.QueueComplete(ctx, &pb.QueueCompleteRequest{
					QueueName:      leaseResp.QueueName,
					Partition:      leaseResp.Partition,
					RequestTimeout: "30s",
					Ids:            ids,
				})
				if err != nil {
					b.Fatal(err)
				}

				totalItems += config.batchSize
			}
			b.StopTimer()

			elapsed := time.Since(start)
			b.ReportMetric(float64(totalItems)/elapsed.Seconds(), "items/s")
		})
	}
}
