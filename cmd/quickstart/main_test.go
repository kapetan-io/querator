package main_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kapetan-io/querator/daemon"
)

func TestQuickstartPassesAgainstRunningDaemon(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "quickstart")
	require.NoError(t, exec.Command("go", "build", "-o", binary, ".").Run())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d, err := daemon.NewDaemon(ctx, daemon.Config{ListenAddress: "localhost:0"})
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Shutdown(context.Background())) }()

	output, err := exec.CommandContext(ctx, binary,
		"--skip-docker",
		"--endpoint", "http://"+d.Listener.Addr().String(),
		"--health-timeout", "5s",
	).CombinedOutput()
	require.NoError(t, err)
	assert.Contains(t, string(output), "Health check passed")
	assert.Contains(t, string(output), "All checks passed!")
}

func TestHealthTimeoutEndsStalledHealthCheck(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "quickstart")
	require.NoError(t, exec.Command("go", "build", "-o", binary, ".").Run())

	// The endpoint accepts the health request and never answers it.
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// The first health request is still in flight when the 1200ms timeout expires.
	output, err := exec.CommandContext(ctx, binary,
		"--skip-docker",
		"--endpoint", server.URL,
		"--health-timeout", "1200ms",
	).CombinedOutput()
	require.NoError(t, ctx.Err())
	require.Error(t, err)
	assert.Contains(t, string(output), "health check timed out after 1.2s")
}

func TestHealthTimeoutWhileDaemonReportsFail(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "quickstart")
	require.NoError(t, exec.Command("go", "build", "-o", binary, ".").Run())

	var first atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		first.CompareAndSwap(0, time.Now().UnixNano())
		w.Header().Set("Content-Type", "application/json")
		_, err := w.Write([]byte(`{"status":"fail","checks":{` +
			`"zeta":[{"status":"fail","output":"zeta down"}],` +
			`"alpha":[{"status":"fail","output":"alpha down"}]}}`))
		assert.NoError(t, err)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	output, err := exec.CommandContext(ctx, binary,
		"--skip-docker",
		"--verbose",
		"--endpoint", server.URL,
		"--health-timeout", "1500ms",
	).CombinedOutput()
	require.NoError(t, ctx.Err())
	// Polls land at 0s and 1s, so a sleep that ignored the 1.5s deadline would end at 2s.
	assert.Less(t, time.Since(time.Unix(0, first.Load())), 1800*time.Millisecond)
	require.Error(t, err)
	assert.Contains(t, string(output), "health check timed out after 1.5s")
	assert.NotContains(t, string(output), "Health check passed")
	assert.Less(t, strings.Index(string(output), "alpha: fail - alpha down"),
		strings.Index(string(output), "zeta: fail - zeta down"))
}

func TestHealthCheckPassesAfterFailedPolls(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "quickstart")
	require.NoError(t, exec.Command("go", "build", "-o", binary, ".").Run())

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	d, err := daemon.NewDaemon(ctx, daemon.Config{ListenAddress: "localhost:0"})
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Shutdown(context.Background())) }()

	// The first health poll fails; the second is served by the real daemon.
	target, err := url.Parse("http://" + d.Listener.Addr().String())
	require.NoError(t, err)
	proxy := httputil.NewSingleHostReverseProxy(target)
	var polls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" && polls.Add(1) == 1 {
			w.Header().Set("Content-Type", "application/json")
			_, err := w.Write([]byte(`{"status":"fail"}`))
			assert.NoError(t, err)
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	defer server.Close()

	output, err := exec.CommandContext(ctx, binary,
		"--skip-docker",
		"--endpoint", server.URL,
		"--health-timeout", "5s",
	).CombinedOutput()
	require.NoError(t, err)
	assert.Equal(t, int32(2), polls.Load())
	assert.Contains(t, string(output), "Health check passed")
	assert.Contains(t, string(output), "All checks passed!")
}
