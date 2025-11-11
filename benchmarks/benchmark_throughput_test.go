package daemon_test

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/kapetan-io/querator/daemon"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/stretchr/testify/require"
)

type ThroughputConfig struct {
	PayloadSize PayloadSize
	BatchSize   int
	Duration    time.Duration
}

type ThroughputResult struct {
	PayloadSizeBytes int
	BatchSize        int
	TotalItems       int
	DurationSec      float64
	ItemsPerSec      float64
	CyclesPerSec     float64
	AvgCycleMs       float64
	P50CycleMs       float64
	P95CycleMs       float64
	P99CycleMs       float64
}

func TestThroughputBenchmark(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping throughput benchmark in short mode")
	}

	configs := []ThroughputConfig{
		{PayloadSize: Payload1KB, BatchSize: 10, Duration: 30 * time.Second},
		{PayloadSize: Payload1KB, BatchSize: 100, Duration: 30 * time.Second},
		{PayloadSize: Payload1KB, BatchSize: 1000, Duration: 30 * time.Second},
		{PayloadSize: Payload10KB, BatchSize: 10, Duration: 30 * time.Second},
		{PayloadSize: Payload10KB, BatchSize: 100, Duration: 30 * time.Second},
	}

	results := make([]ThroughputResult, 0, len(configs))

	for _, config := range configs {
		t.Logf("Running throughput test: payload=%d bytes, batch=%d, duration=%s",
			config.PayloadSize, config.BatchSize, config.Duration)

		result, err := runThroughputTest(config)
		require.NoError(t, err)

		t.Logf("  Result: %.2f items/sec, %.2f cycles/sec, p99=%.2f ms",
			result.ItemsPerSec, result.CyclesPerSec, result.P99CycleMs)

		results = append(results, result)
	}

	err := os.MkdirAll("benchmark_results", 0755)
	require.NoError(t, err)

	err = writeThroughputResults(results, "benchmark_results/throughput_results.csv")
	require.NoError(t, err)

	t.Log("Throughput benchmark complete. Results saved to benchmark_results/throughput_results.csv")
}

func runThroughputTest(config ThroughputConfig) (ThroughputResult, error) {
	ctx := context.Background()
	d, err := daemon.NewDaemon(ctx, daemon.Config{
		ListenAddress: "localhost:0",
	})
	if err != nil {
		return ThroughputResult{}, err
	}
	defer func() {
		_ = d.Shutdown(ctx)
	}()

	client := d.MustClient()

	err = client.QueuesCreate(ctx, &pb.QueueInfo{
		RequestedPartitions: 1,
		LeaseTimeout:        "1m",
		ExpireTimeout:       "24h",
		QueueName:           "benchmark",
	})
	if err != nil {
		return ThroughputResult{}, err
	}

	collector := NewMetricsCollector()
	totalItems := 0
	cycles := 0

	startTime := time.Now()
	for time.Since(startTime) < config.Duration {
		cycleStart := time.Now()

		items := GenerateProduceItems(config.BatchSize, config.PayloadSize, benchmarkSeed+cycles)

		err := client.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      "benchmark",
			RequestTimeout: "30s",
			Items:          items,
		})
		if err != nil {
			return ThroughputResult{}, err
		}

		var leaseResp pb.QueueLeaseResponse
		err = client.QueueLease(ctx, &pb.QueueLeaseRequest{
			QueueName:      "benchmark",
			ClientId:       "throughput-bench",
			RequestTimeout: "30s",
			BatchSize:      int32(config.BatchSize),
		}, &leaseResp)
		if err != nil {
			return ThroughputResult{}, err
		}

		ids := make([]string, len(leaseResp.Items))
		for i, item := range leaseResp.Items {
			ids[i] = item.Id
		}

		err = client.QueueComplete(ctx, &pb.QueueCompleteRequest{
			QueueName:      leaseResp.QueueName,
			Partition:      leaseResp.Partition,
			RequestTimeout: "30s",
			Ids:            ids,
		})
		if err != nil {
			return ThroughputResult{}, err
		}

		collector.RecordLatency(time.Since(cycleStart))
		totalItems += config.BatchSize
		cycles++
	}
	elapsed := time.Since(startTime)

	if totalItems == 0 {
		return ThroughputResult{}, fmt.Errorf("no items processed during %v duration", config.Duration)
	}
	if cycles == 0 {
		return ThroughputResult{}, fmt.Errorf("no complete cycles during %v duration", config.Duration)
	}

	p50, p95, p99, _ := collector.CalculatePercentiles()

	var sumLatency int64
	for _, lat := range collector.latencies {
		sumLatency += lat
	}
	avgCycleNs := sumLatency / int64(len(collector.latencies))

	result := ThroughputResult{
		PayloadSizeBytes: int(config.PayloadSize),
		BatchSize:        config.BatchSize,
		TotalItems:       totalItems,
		DurationSec:      elapsed.Seconds(),
		ItemsPerSec:      float64(totalItems) / elapsed.Seconds(),
		CyclesPerSec:     float64(cycles) / elapsed.Seconds(),
		AvgCycleMs:       float64(avgCycleNs) / 1e6,
		P50CycleMs:       float64(p50) / 1e6,
		P95CycleMs:       float64(p95) / 1e6,
		P99CycleMs:       float64(p99) / 1e6,
	}

	return result, nil
}

func writeThroughputResults(results []ThroughputResult, filename string) error {
	var buf strings.Builder
	w := csv.NewWriter(&buf)

	if err := w.Write([]string{
		"payload_size_bytes",
		"batch_size",
		"total_items",
		"duration_sec",
		"items_per_sec",
		"cycles_per_sec",
		"avg_cycle_ms",
		"p50_cycle_ms",
		"p95_cycle_ms",
		"p99_cycle_ms",
	}); err != nil {
		return err
	}

	for _, r := range results {
		if err := w.Write([]string{
			fmt.Sprintf("%d", r.PayloadSizeBytes),
			fmt.Sprintf("%d", r.BatchSize),
			fmt.Sprintf("%d", r.TotalItems),
			fmt.Sprintf("%.2f", r.DurationSec),
			fmt.Sprintf("%.2f", r.ItemsPerSec),
			fmt.Sprintf("%.2f", r.CyclesPerSec),
			fmt.Sprintf("%.2f", r.AvgCycleMs),
			fmt.Sprintf("%.2f", r.P50CycleMs),
			fmt.Sprintf("%.2f", r.P95CycleMs),
			fmt.Sprintf("%.2f", r.P99CycleMs),
		}); err != nil {
			return err
		}
	}

	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}

	return os.WriteFile(filename, []byte(buf.String()), 0644)
}
