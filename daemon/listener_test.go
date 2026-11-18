package daemon_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/kapetan-io/querator/daemon"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryListener(t *testing.T) {
	listener := daemon.NewInMemoryListener()
	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := fmt.Fprintf(w, "Hello, %s!", r.URL.Path[1:])
			assert.NoError(t, err)
		}),
	}
	var serving sync.WaitGroup
	serving.Go(func() {
		assert.ErrorIs(t, server.Serve(listener), http.ErrServerClosed)
	})
	defer serving.Wait()
	defer func() { assert.NoError(t, server.Close()) }()

	clientCount := 3
	var wg sync.WaitGroup
	for i := 0; i < clientCount; i++ {
		wg.Go(func() {
			// Each client gets its own net.Pipe
			serverConn, clientConn := net.Pipe()
			if !assert.NoError(t, listener.ServeConn(serverConn)) {
				return
			}

			// Custom DialContext returns the clientConn for this request
			dialOnce := sync.Once{}

			transport := &http.Transport{
				DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
					var c net.Conn
					dialOnce.Do(func() { c = clientConn })
					return c, nil
				},
			}
			defer transport.CloseIdleConnections()
			client := &http.Client{
				Transport: transport,
				Timeout:   2 * time.Second,
			}

			url := fmt.Sprintf("http://inmemory/client%d", i)
			resp, err := client.Get(url)
			if err != nil {
				t.Errorf("client %d error: %v", i, err)
				return
			}
			defer func() { _ = resp.Body.Close() }()
			body, err := io.ReadAll(resp.Body)
			if !assert.NoError(t, err) {
				return
			}
			expected := fmt.Sprintf("Hello, client%d!", i)
			if !strings.Contains(string(body), expected) {
				t.Errorf("client %d got unexpected body: %q", i, body)
			}
		})
	}
	// The deferred server.Close closes the listener, so Serve reports ErrServerClosed.
	wg.Wait()
}

func TestDaemonInMemoryListener(t *testing.T) {
	ctx := context.Background()

	// Create daemon with InMemoryListener enabled
	d, err := daemon.NewDaemon(ctx, daemon.Config{
		InMemoryListener: true,
	})
	require.NoError(t, err)
	defer func() { _ = d.Shutdown(ctx) }()

	// Get a client - should create a new net.Pipe connection
	{
		client, err := d.Client()
		require.NoError(t, err)

		// Test QueuesList to verify the connection works
		var resp pb.QueuesListResponse
		err = client.QueuesList(ctx, &resp, nil)
		require.NoError(t, err)

		// Verify we got an empty list
		assert.Nil(t, resp.Items)

		// Should work a second time also
		err = client.QueuesList(ctx, &resp, nil)
		require.NoError(t, err)

		// Verify we got an empty list
		assert.Nil(t, resp.Items)
	}

	// Test getting multiple clients - each should work independently
	{
		client, err := d.Client()
		require.NoError(t, err)

		// Test QueuesList to verify the connection works
		var resp pb.QueuesListResponse
		err = client.QueuesList(ctx, &resp, nil)
		require.NoError(t, err)

		// Verify we got an empty list
		assert.Nil(t, resp.Items)
	}
}

func TestInMemoryListenerServeConnAfterClose(t *testing.T) {
	synctest.Test(t, serveConnAfterClose)
}

// serveConnAfterClose runs in a synctest bubble, so synctest.Wait returns only once ServeConn is blocked.
func serveConnAfterClose(t *testing.T) {
	listener := daemon.NewInMemoryListener()
	serverConn, clientConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()
	defer func() { _ = serverConn.Close() }()

	// Nothing calls Accept, so ServeConn blocks until Close releases it.
	var wg sync.WaitGroup
	wg.Go(func() {
		assert.ErrorIs(t, listener.ServeConn(serverConn), net.ErrClosed)
	})
	defer wg.Wait()
	synctest.Wait()

	require.NoError(t, listener.Close())
}
