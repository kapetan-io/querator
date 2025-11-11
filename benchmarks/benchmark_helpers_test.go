package daemon_test

import (
	"encoding/json"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGeneratePayload(t *testing.T) {
	payload := GeneratePayload(Payload100B, benchmarkSeed)
	assert.Len(t, payload, 100)

	payload1KB := GeneratePayload(Payload1KB, benchmarkSeed)
	assert.Len(t, payload1KB, 1024)

	payload1 := GeneratePayload(Payload100B, benchmarkSeed)
	payload2 := GeneratePayload(Payload100B, benchmarkSeed)
	assert.Equal(t, payload1, payload2)

	payload3 := GeneratePayload(Payload100B, benchmarkSeed+1)
	assert.NotEqual(t, payload1, payload3)
}

func TestGenerateProduceItems(t *testing.T) {
	const count = 10
	items := GenerateProduceItems(count, Payload1KB, benchmarkSeed)

	require.Len(t, items, count)

	for _, item := range items {
		assert.Equal(t, "benchmark-item", item.Kind)
		assert.Equal(t, "bytes", item.Encoding)
		assert.Len(t, item.Bytes, 1024)
	}

	items1 := GenerateProduceItems(count, Payload1KB, benchmarkSeed)
	items2 := GenerateProduceItems(count, Payload1KB, benchmarkSeed)
	for i := 0; i < count; i++ {
		assert.Equal(t, items1[i].Bytes, items2[i].Bytes)
	}
}

func TestMetricsCollectorCalculatePercentiles(t *testing.T) {
	collector := NewMetricsCollector()

	collector.RecordLatency(100 * time.Millisecond)
	collector.RecordLatency(200 * time.Millisecond)
	collector.RecordLatency(300 * time.Millisecond)
	collector.RecordLatency(400 * time.Millisecond)
	collector.RecordLatency(500 * time.Millisecond)

	p50, p95, p99, max := collector.CalculatePercentiles()

	assert.Equal(t, int64(300*time.Millisecond), p50)
	assert.Equal(t, int64(500*time.Millisecond), p95)
	assert.Equal(t, int64(500*time.Millisecond), p99)
	assert.Equal(t, int64(500*time.Millisecond), max)
}

func TestMetricsCollectorCalculatePercentilesEmpty(t *testing.T) {
	collector := NewMetricsCollector()

	p50, p95, p99, max := collector.CalculatePercentiles()

	assert.Equal(t, int64(0), p50)
	assert.Equal(t, int64(0), p95)
	assert.Equal(t, int64(0), p99)
	assert.Equal(t, int64(0), max)
}

func TestBenchmarkMetricsCalculateThroughput(t *testing.T) {
	metrics := BenchmarkMetrics{
		DurationNs: int64(time.Second),
		Operations: 1000,
		Items:      5000,
	}

	opsPerSec, itemsPerSec := metrics.CalculateThroughput()

	assert.Equal(t, 1000.0, opsPerSec)
	assert.Equal(t, 5000.0, itemsPerSec)
}

func TestBenchmarkMetricsCalculateThroughputZeroDuration(t *testing.T) {
	metrics := BenchmarkMetrics{
		DurationNs: 0,
		Operations: 1000,
		Items:      5000,
	}

	opsPerSec, itemsPerSec := metrics.CalculateThroughput()

	assert.Equal(t, 0.0, opsPerSec)
	assert.Equal(t, 0.0, itemsPerSec)
}

func TestMetricsCollectorFinalize(t *testing.T) {
	collector := NewMetricsCollector()
	collector.RecordLatency(100 * time.Millisecond)
	collector.RecordLatency(200 * time.Millisecond)

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	metrics := collector.Finalize(OpProduce, 1024, 10, 1, 0, 100, 1000, memStats)

	assert.Equal(t, OpProduce, metrics.Operation)
	assert.Equal(t, 1024, metrics.PayloadSize)
	assert.Equal(t, 10, metrics.BatchSize)
	assert.Equal(t, 1, metrics.Concurrency)
	assert.Equal(t, 0, metrics.QueueDepth)
	assert.Equal(t, 100, metrics.Operations)
	assert.Equal(t, 1000, metrics.Items)
	assert.Greater(t, metrics.LatencyP50Ns, int64(0))
	assert.Greater(t, metrics.DurationNs, int64(0))
}

func TestFormatAsText(t *testing.T) {
	results := []BenchmarkMetrics{
		{
			Operation:    OpProduce,
			PayloadSize:  1024,
			BatchSize:    10,
			Concurrency:  1,
			QueueDepth:   0,
			Operations:   1000,
			Items:        10000,
			DurationNs:   int64(time.Second),
			LatencyP50Ns: int64(10 * time.Millisecond),
			LatencyP95Ns: int64(20 * time.Millisecond),
			LatencyP99Ns: int64(30 * time.Millisecond),
			LatencyMaxNs: int64(40 * time.Millisecond),
		},
	}

	text := FormatAsText(results)

	assert.Contains(t, text, "Operation")
	assert.Contains(t, text, "produce")
	assert.Contains(t, text, "1024")
	assert.Contains(t, text, "10")
}

func TestFormatAsJSON(t *testing.T) {
	results := []BenchmarkMetrics{
		{
			Operation:    OpLease,
			PayloadSize:  1024,
			BatchSize:    100,
			Concurrency:  2,
			QueueDepth:   1000,
			Operations:   500,
			Items:        50000,
			DurationNs:   int64(time.Second),
			LatencyP50Ns: int64(5 * time.Millisecond),
		},
	}

	jsonBytes, err := FormatAsJSON(results)
	require.NoError(t, err)

	var decoded []BenchmarkMetrics
	err = json.Unmarshal(jsonBytes, &decoded)
	require.NoError(t, err)

	assert.Len(t, decoded, 1)
	assert.Equal(t, OpLease, decoded[0].Operation)
	assert.Equal(t, 1024, decoded[0].PayloadSize)
	assert.Equal(t, 100, decoded[0].BatchSize)
}

func TestFormatAsCSV(t *testing.T) {
	results := []BenchmarkMetrics{
		{
			Operation:    OpComplete,
			PayloadSize:  2048,
			BatchSize:    50,
			Concurrency:  4,
			QueueDepth:   5000,
			Operations:   200,
			Items:        10000,
			DurationNs:   int64(2 * time.Second),
			LatencyP50Ns: int64(15 * time.Millisecond),
			BytesPerOp:   1000000,
			AllocsPerOp:  5000,
		},
	}

	csv := FormatAsCSV(results)

	lines := strings.Split(strings.TrimSpace(csv), "\n")
	require.Len(t, lines, 2)

	assert.Contains(t, lines[0], "operation")
	assert.Contains(t, lines[0], "payload_size")
	assert.Contains(t, lines[1], "complete")
	assert.Contains(t, lines[1], "2048")
	assert.Contains(t, lines[1], "50")
}

func TestGenerateSummary(t *testing.T) {
	results := []BenchmarkMetrics{
		{
			Operation:    OpProduce,
			PayloadSize:  1024,
			BatchSize:    10,
			Concurrency:  1,
			QueueDepth:   0,
			Operations:   1000,
			Items:        10000,
			DurationNs:   int64(time.Second),
			LatencyP99Ns: int64(30 * time.Millisecond),
		},
		{
			Operation:    OpLease,
			PayloadSize:  1024,
			BatchSize:    10,
			Concurrency:  1,
			QueueDepth:   0,
			Operations:   500,
			Items:        5000,
			DurationNs:   int64(time.Second),
			LatencyP99Ns: int64(20 * time.Millisecond),
		},
	}

	summary := GenerateSummary(results)

	assert.Contains(t, summary, "Benchmark Summary")
	assert.Contains(t, summary, "Maximum Throughput")
	assert.Contains(t, summary, "Slowest Operation")
	assert.Contains(t, summary, "Total Benchmarks")
}

func TestWriteBenchmarkResults(t *testing.T) {
	results := []BenchmarkMetrics{
		{
			Operation:    OpProduce,
			PayloadSize:  1024,
			BatchSize:    10,
			Concurrency:  1,
			QueueDepth:   0,
			Operations:   100,
			Items:        1000,
			DurationNs:   int64(time.Second),
			LatencyP50Ns: int64(10 * time.Millisecond),
		},
	}

	tmpFile := "/tmp/benchmark_test_output.txt"
	defer func() { _ = os.Remove(tmpFile) }()

	err := WriteBenchmarkResults(results, FormatText, tmpFile)
	require.NoError(t, err)

	content, err := os.ReadFile(tmpFile)
	require.NoError(t, err)
	assert.Contains(t, string(content), "produce")
}
