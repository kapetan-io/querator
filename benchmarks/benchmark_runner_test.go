package daemon_test

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/kapetan-io/querator/daemon"
	pb "github.com/kapetan-io/querator/proto"
)

type BenchmarkRunner struct {
	results []BenchmarkMetrics
}

func NewBenchmarkRunner() *BenchmarkRunner {
	return &BenchmarkRunner{
		results: make([]BenchmarkMetrics, 0),
	}
}

func (r *BenchmarkRunner) RunProduceBenchmarks() error {
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
		metrics, err := r.runProduceBenchmark(int(test.payloadSize), test.batchSize, 1, 0, 100)
		if err != nil {
			return err
		}
		r.results = append(r.results, metrics)
	}

	return nil
}

func (r *BenchmarkRunner) runProduceBenchmark(payloadSize, batchSize, concurrency, queueDepth, iterations int) (BenchmarkMetrics, error) {
	ctx := context.Background()
	d, err := daemon.NewDaemon(ctx, daemon.Config{})
	if err != nil {
		return BenchmarkMetrics{}, err
	}
	defer func() { _ = d.Shutdown(ctx) }()

	client := d.MustClient()

	err = client.QueuesCreate(ctx, &pb.QueueInfo{
		RequestedPartitions: 1,
		LeaseTimeout:        "1m",
		ExpireTimeout:       "24h",
		QueueName:           "benchmark",
	})
	if err != nil {
		return BenchmarkMetrics{}, err
	}

	items := GenerateProduceItems(batchSize, PayloadSize(payloadSize), benchmarkSeed)

	collector := NewMetricsCollector()

	for i := 0; i < iterations; i++ {
		start := time.Now()
		err := client.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      "benchmark",
			RequestTimeout: "30s",
			Items:          items,
		})
		if err != nil {
			return BenchmarkMetrics{}, err
		}
		collector.RecordLatency(time.Since(start))
	}

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	return collector.Finalize(OpProduce, payloadSize, batchSize, concurrency, queueDepth, iterations, iterations*batchSize, memStats), nil
}

func (r *BenchmarkRunner) RunLeaseBenchmarks() error {
	tests := []struct {
		payloadSize PayloadSize
		batchSize   int
		queueDepth  int
	}{
		{Payload100B, 1, 100},
		{Payload100B, 10, 100},
		{Payload100B, 100, 100},
		{Payload1KB, 1, 100},
		{Payload1KB, 10, 100},
		{Payload1KB, 100, 100},
		{Payload10KB, 1, 50},
		{Payload10KB, 10, 50},
	}

	for _, test := range tests {
		metrics, err := r.runLeaseBenchmark(int(test.payloadSize), test.batchSize, 1, test.queueDepth, 50)
		if err != nil {
			return err
		}
		r.results = append(r.results, metrics)
	}

	return nil
}

func (r *BenchmarkRunner) runLeaseBenchmark(payloadSize, batchSize, concurrency, queueDepth, iterations int) (BenchmarkMetrics, error) {
	ctx := context.Background()
	d, err := daemon.NewDaemon(ctx, daemon.Config{})
	if err != nil {
		return BenchmarkMetrics{}, err
	}
	defer func() { _ = d.Shutdown(ctx) }()

	client := d.MustClient()

	err = client.QueuesCreate(ctx, &pb.QueueInfo{
		RequestedPartitions: 1,
		LeaseTimeout:        "1m",
		ExpireTimeout:       "24h",
		QueueName:           "benchmark",
	})
	if err != nil {
		return BenchmarkMetrics{}, err
	}

	maxBatchSize := 100
	if payloadSize*maxBatchSize > 900000 {
		maxBatchSize = 900000 / payloadSize
		if maxBatchSize < 1 {
			maxBatchSize = 1
		}
	}

	batches := (queueDepth + maxBatchSize - 1) / maxBatchSize
	totalProduced := 0
	for i := 0; i < batches; i++ {
		remaining := queueDepth - (i * maxBatchSize)
		if remaining > maxBatchSize {
			remaining = maxBatchSize
		}

		items := GenerateProduceItems(remaining, PayloadSize(payloadSize), benchmarkSeed+i)
		err = client.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      "benchmark",
			RequestTimeout: "30s",
			Items:          items,
		})
		if err != nil {
			return BenchmarkMetrics{}, fmt.Errorf("failed to produce batch %d/%d (%d items): %w", i+1, batches, len(items), err)
		}
		totalProduced += len(items)
	}

	if totalProduced != queueDepth {
		return BenchmarkMetrics{}, fmt.Errorf("produced %d items but expected %d", totalProduced, queueDepth)
	}

	collector := NewMetricsCollector()

	actualIterations := queueDepth / batchSize
	if actualIterations < 1 {
		actualIterations = 1
	}
	if actualIterations > iterations {
		actualIterations = iterations
	}

	for i := 0; i < actualIterations; i++ {
		start := time.Now()
		var resp pb.QueueLeaseResponse
		err := client.QueueLease(ctx, &pb.QueueLeaseRequest{
			QueueName:      "benchmark",
			ClientId:       "bench-client",
			RequestTimeout: "5s",
			BatchSize:      int32(batchSize),
		}, &resp)
		if err != nil {
			return BenchmarkMetrics{}, fmt.Errorf("lease iteration %d/%d failed: %w", i+1, actualIterations, err)
		}
		collector.RecordLatency(time.Since(start))
	}

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	return collector.Finalize(OpLease, payloadSize, batchSize, concurrency, queueDepth, actualIterations, actualIterations*batchSize, memStats), nil
}

func (r *BenchmarkRunner) RunCompleteBenchmarks() error {
	batchSizes := []int{1, 10, 100}

	for _, batchSize := range batchSizes {
		metrics, err := r.runCompleteBenchmark(1024, batchSize, 1, 0, 100)
		if err != nil {
			return err
		}
		r.results = append(r.results, metrics)
	}

	return nil
}

func (r *BenchmarkRunner) runCompleteBenchmark(payloadSize, batchSize, concurrency, queueDepth, iterations int) (BenchmarkMetrics, error) {
	ctx := context.Background()
	d, err := daemon.NewDaemon(ctx, daemon.Config{})
	if err != nil {
		return BenchmarkMetrics{}, err
	}
	defer func() { _ = d.Shutdown(ctx) }()

	client := d.MustClient()

	err = client.QueuesCreate(ctx, &pb.QueueInfo{
		RequestedPartitions: 1,
		LeaseTimeout:        "1m",
		ExpireTimeout:       "24h",
		QueueName:           "benchmark",
	})
	if err != nil {
		return BenchmarkMetrics{}, err
	}

	collector := NewMetricsCollector()

	for i := 0; i < iterations; i++ {
		items := GenerateProduceItems(batchSize, PayloadSize(payloadSize), benchmarkSeed+i)

		err := client.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      "benchmark",
			RequestTimeout: "30s",
			Items:          items,
		})
		if err != nil {
			return BenchmarkMetrics{}, err
		}

		var leaseResp pb.QueueLeaseResponse
		err = client.QueueLease(ctx, &pb.QueueLeaseRequest{
			QueueName:      "benchmark",
			ClientId:       "bench-client",
			RequestTimeout: "30s",
			BatchSize:      int32(batchSize),
		}, &leaseResp)
		if err != nil {
			return BenchmarkMetrics{}, err
		}

		ids := make([]string, len(leaseResp.Items))
		for j, item := range leaseResp.Items {
			ids[j] = item.Id
		}

		start := time.Now()
		err = client.QueueComplete(ctx, &pb.QueueCompleteRequest{
			QueueName:      leaseResp.QueueName,
			Partition:      leaseResp.Partition,
			RequestTimeout: "30s",
			Ids:            ids,
		})
		if err != nil {
			return BenchmarkMetrics{}, err
		}
		collector.RecordLatency(time.Since(start))
	}

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	return collector.Finalize(OpComplete, payloadSize, batchSize, concurrency, queueDepth, iterations, iterations*batchSize, memStats), nil
}

func (r *BenchmarkRunner) SaveResults(outputDir string) error {
	if err := WriteBenchmarkResults(r.results, FormatText, outputDir+"/benchmark_results.txt"); err != nil {
		return err
	}

	if err := WriteBenchmarkResults(r.results, FormatJSON, outputDir+"/benchmark_results.json"); err != nil {
		return err
	}

	if err := WriteBenchmarkResults(r.results, FormatCSV, outputDir+"/benchmark_results.csv"); err != nil {
		return err
	}

	return nil
}
