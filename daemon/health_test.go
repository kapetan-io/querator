package daemon_test

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"testing"

	"github.com/kapetan-io/querator/daemon"
	"github.com/kapetan-io/querator/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHealthEndpoint(t *testing.T) {
	ctx := context.Background()

	d, err := daemon.NewDaemon(ctx, daemon.Config{
		InMemoryListener: true,
		Version:          "test-version",
	})
	require.NoError(t, err)
	defer func() { _ = d.Shutdown(ctx) }()

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				serverConn, clientConn := net.Pipe()
				_ = d.Listener.(*daemon.InMemoryListener).ServeConn(serverConn)
				return clientConn, nil
			},
		},
	}

	resp, err := client.Get("http://inmemory/health")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/health+json", resp.Header.Get("Content-Type"))

	var health transport.HealthResponse
	err = json.NewDecoder(resp.Body).Decode(&health)
	require.NoError(t, err)

	assert.Equal(t, transport.HealthStatusPass, health.Status)
	assert.Equal(t, "test-version", health.Version)
	assert.NotEmpty(t, health.Checks)

	checks, ok := health.Checks["queues:storage"]
	require.True(t, ok)
	require.Len(t, checks, 1)
	assert.Equal(t, transport.HealthStatusPass, checks[0].Status)
	assert.Equal(t, "datastore", checks[0].ComponentType)
	assert.NotEmpty(t, checks[0].Time)
}
