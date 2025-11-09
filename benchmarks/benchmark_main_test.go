package daemon_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunAllBenchmarks(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping comprehensive benchmark suite in short mode")
	}

	runner := NewBenchmarkRunner()

	t.Log("Running produce benchmarks...")
	err := runner.RunProduceBenchmarks()
	require.NoError(t, err)

	t.Log("Running lease benchmarks...")
	err = runner.RunLeaseBenchmarks()
	require.NoError(t, err)

	t.Log("Running complete benchmarks...")
	err = runner.RunCompleteBenchmarks()
	require.NoError(t, err)

	err = os.MkdirAll("benchmark_results", 0755)
	require.NoError(t, err)

	t.Log("Saving results...")
	err = runner.SaveResults("benchmark_results")
	require.NoError(t, err)

	t.Log("Generating summary...")
	PrintSummary(runner.results)

	t.Logf("Benchmark complete. Results saved to benchmark_results/")
	t.Logf("Total benchmarks run: %d", len(runner.results))
}
