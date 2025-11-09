package daemon_test

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	pb "github.com/kapetan-io/querator/proto"
)

const benchmarkSeed = 12345

type PayloadSize int

const (
	Payload100B  PayloadSize = 100
	Payload1KB   PayloadSize = 1024
	Payload10KB  PayloadSize = 10 * 1024
	Payload100KB PayloadSize = 100 * 1024
	Payload300KB PayloadSize = 300 * 1024
)

func GeneratePayload(size PayloadSize, seed int) []byte {
	rng := rand.New(rand.NewSource(int64(seed)))
	payload := make([]byte, size)

	for i := range payload {
		payload[i] = byte(32 + rng.Intn(95))
	}

	return payload
}

func GenerateProduceItems(count int, payloadSize PayloadSize, seed int) []*pb.QueueProduceItem {
	items := make([]*pb.QueueProduceItem, count)

	for i := 0; i < count; i++ {
		payload := GeneratePayload(payloadSize, seed+i)
		items[i] = &pb.QueueProduceItem{
			Kind:     "benchmark-item",
			Encoding: "bytes",
			Bytes:    payload,
		}
	}

	return items
}

func GenerateBatchSizes() []int {
	return []int{1, 10, 100, 1000}
}

type OperationType string

const (
	OpProduce  OperationType = "produce"
	OpLease    OperationType = "lease"
	OpComplete OperationType = "complete"
	OpWorkflow OperationType = "workflow"
)

type BenchmarkMetrics struct {
	LatencyMaxNs    int64
	LatencyP99Ns    int64
	LatencyP95Ns    int64
	LatencyP50Ns    int64
	BytesPerOp      uint64
	AllocsPerOp     uint64
	DurationNs      int64
	Concurrency     int
	PayloadSize     int
	QueueDepth      int
	BatchSize       int
	Operations      int
	Items           int
	Operation       OperationType
}

func (m *BenchmarkMetrics) CalculateThroughput() (opsPerSec, itemsPerSec float64) {
	if m.DurationNs == 0 {
		return 0, 0
	}

	durationSec := float64(m.DurationNs) / float64(time.Second)
	opsPerSec = float64(m.Operations) / durationSec
	itemsPerSec = float64(m.Items) / durationSec

	return opsPerSec, itemsPerSec
}

type MetricsCollector struct {
	latencies []int64
	startTime time.Time
}

func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		latencies: make([]int64, 0),
		startTime: time.Now(),
	}
}

func (c *MetricsCollector) RecordLatency(d time.Duration) {
	c.latencies = append(c.latencies, d.Nanoseconds())
}

func (c *MetricsCollector) CalculatePercentiles() (p50, p95, p99, max int64) {
	if len(c.latencies) == 0 {
		return 0, 0, 0, 0
	}

	sorted := make([]int64, len(c.latencies))
	copy(sorted, c.latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	n := len(sorted)
	p50 = sorted[n*50/100]
	p95 = sorted[n*95/100]
	p99 = sorted[n*99/100]
	max = sorted[n-1]

	return p50, p95, p99, max
}

func (c *MetricsCollector) Finalize(operation OperationType, payloadSize, batchSize, concurrency, queueDepth, operations, items int, memStats runtime.MemStats) BenchmarkMetrics {
	p50, p95, p99, max := c.CalculatePercentiles()

	return BenchmarkMetrics{
		LatencyMaxNs:    max,
		LatencyP99Ns:    p99,
		LatencyP95Ns:    p95,
		LatencyP50Ns:    p50,
		BytesPerOp:      memStats.TotalAlloc,
		AllocsPerOp:     memStats.Mallocs,
		DurationNs:      time.Since(c.startTime).Nanoseconds(),
		Concurrency:     concurrency,
		PayloadSize:     payloadSize,
		QueueDepth:      queueDepth,
		BatchSize:       batchSize,
		Operations:      operations,
		Items:           items,
		Operation:       operation,
	}
}

type OutputFormat string

const (
	FormatText OutputFormat = "text"
	FormatJSON OutputFormat = "json"
	FormatCSV  OutputFormat = "csv"
)

func WriteBenchmarkResults(results []BenchmarkMetrics, format OutputFormat, filename string) error {
	var content string
	var err error

	switch format {
	case FormatText:
		content = FormatAsText(results)
	case FormatJSON:
		var jsonBytes []byte
		jsonBytes, err = FormatAsJSON(results)
		if err != nil {
			return err
		}
		content = string(jsonBytes)
	case FormatCSV:
		content = FormatAsCSV(results)
	default:
		return fmt.Errorf("unknown format: %s", format)
	}

	return os.WriteFile(filename, []byte(content), 0644)
}

func FormatAsText(results []BenchmarkMetrics) string {
	var buf strings.Builder
	w := tabwriter.NewWriter(&buf, 0, 0, 2, ' ', 0)

	fmt.Fprintln(w, "Operation\tPayload\tBatch\tConcur\tDepth\tOps\tItems\tOps/sec\tItems/sec\tP50(ms)\tP95(ms)\tP99(ms)\tMax(ms)")

	for _, r := range results {
		opsPerSec, itemsPerSec := r.CalculateThroughput()

		fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%d\t%d\t%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\n",
			r.Operation,
			r.PayloadSize,
			r.BatchSize,
			r.Concurrency,
			r.QueueDepth,
			r.Operations,
			r.Items,
			opsPerSec,
			itemsPerSec,
			float64(r.LatencyP50Ns)/1e6,
			float64(r.LatencyP95Ns)/1e6,
			float64(r.LatencyP99Ns)/1e6,
			float64(r.LatencyMaxNs)/1e6,
		)
	}

	w.Flush()
	return buf.String()
}

func FormatAsJSON(results []BenchmarkMetrics) ([]byte, error) {
	return json.MarshalIndent(results, "", "  ")
}

func GenerateSummary(results []BenchmarkMetrics) string {
	if len(results) == 0 {
		return "No benchmark results available"
	}

	var buf strings.Builder

	buf.WriteString("# Benchmark Summary\n\n")

	var maxThroughput float64
	var maxThroughputOp string
	var slowestOp string
	var slowestLatency int64

	for _, r := range results {
		opsPerSec, _ := r.CalculateThroughput()
		if opsPerSec > maxThroughput {
			maxThroughput = opsPerSec
			maxThroughputOp = fmt.Sprintf("%s (payload=%d, batch=%d, concurrency=%d)", r.Operation, r.PayloadSize, r.BatchSize, r.Concurrency)
		}

		if r.LatencyP99Ns > slowestLatency {
			slowestLatency = r.LatencyP99Ns
			slowestOp = fmt.Sprintf("%s (payload=%d, batch=%d, concurrency=%d)", r.Operation, r.PayloadSize, r.BatchSize, r.Concurrency)
		}
	}

	buf.WriteString(fmt.Sprintf("**Maximum Throughput**: %.2f ops/sec (%s)\n", maxThroughput, maxThroughputOp))
	buf.WriteString(fmt.Sprintf("**Slowest Operation (P99)**: %.2f ms (%s)\n", float64(slowestLatency)/1e6, slowestOp))
	buf.WriteString(fmt.Sprintf("**Total Benchmarks**: %d\n\n", len(results)))

	concurrencyScaling := make(map[int]float64)
	for _, r := range results {
		if r.Operation == OpProduce && r.PayloadSize == 1024 && r.BatchSize == 10 {
			opsPerSec, _ := r.CalculateThroughput()
			concurrencyScaling[r.Concurrency] = opsPerSec
		}
	}

	if len(concurrencyScaling) > 0 {
		buf.WriteString("## Concurrency Scaling (Produce, 1KB payload, batch=10)\n")
		for c := 1; c <= 10; c++ {
			if ops, ok := concurrencyScaling[c]; ok {
				buf.WriteString(fmt.Sprintf("- Concurrency %d: %.2f ops/sec\n", c, ops))
			}
		}
		buf.WriteString("\n")
	}

	buf.WriteString("## Key Findings\n")
	buf.WriteString("- See benchmark_results.txt for detailed metrics\n")
	buf.WriteString("- See benchmark_results.json for data analysis\n")
	buf.WriteString("- See benchmark_results.csv for graphing in notebooks\n")

	return buf.String()
}

func PrintSummary(results []BenchmarkMetrics) {
	fmt.Println(GenerateSummary(results))
}

func FormatAsCSV(results []BenchmarkMetrics) string {
	var buf strings.Builder
	w := csv.NewWriter(&buf)

	w.Write([]string{
		"operation", "payload_size", "batch_size", "concurrency", "queue_depth",
		"operations", "items", "ops_per_sec", "items_per_sec",
		"p50_ns", "p95_ns", "p99_ns", "max_ns",
		"bytes_per_op", "allocs_per_op", "duration_ns",
	})

	for _, r := range results {
		opsPerSec, itemsPerSec := r.CalculateThroughput()

		w.Write([]string{
			string(r.Operation),
			fmt.Sprintf("%d", r.PayloadSize),
			fmt.Sprintf("%d", r.BatchSize),
			fmt.Sprintf("%d", r.Concurrency),
			fmt.Sprintf("%d", r.QueueDepth),
			fmt.Sprintf("%d", r.Operations),
			fmt.Sprintf("%d", r.Items),
			fmt.Sprintf("%.2f", opsPerSec),
			fmt.Sprintf("%.2f", itemsPerSec),
			fmt.Sprintf("%d", r.LatencyP50Ns),
			fmt.Sprintf("%d", r.LatencyP95Ns),
			fmt.Sprintf("%d", r.LatencyP99Ns),
			fmt.Sprintf("%d", r.LatencyMaxNs),
			fmt.Sprintf("%d", r.BytesPerOp),
			fmt.Sprintf("%d", r.AllocsPerOp),
			fmt.Sprintf("%d", r.DurationNs),
		})
	}

	w.Flush()
	return buf.String()
}
