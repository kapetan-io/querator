package daemon_test

import (
	"context"
	"fmt"
	"github.com/kapetan-io/querator/daemon"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestInMemoryListener(t *testing.T) {
	listener := daemon.NewInMemoryListener()
	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = fmt.Fprintf(w, "Hello, %s!", r.URL.Path[1:])
		}),
	}
	go func() { _ = server.Serve(listener) }()
	defer func() { _ = server.Close() }()

	clientCount := 3
	var wg sync.WaitGroup
	for i := 0; i < clientCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// Each client gets its own net.Pipe
			serverConn, clientConn := net.Pipe()
			_ = listener.ServeConn(serverConn)

			// Custom DialContext returns the clientConn for this request
			dialOnce := sync.Once{}

			client := &http.Client{
				Transport: &http.Transport{
					DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
						var c net.Conn
						dialOnce.Do(func() { c = clientConn })
						return c, nil
					},
				},
				Timeout: 2 * time.Second,
			}

			url := fmt.Sprintf("http://inmemory/client%d", id)
			resp, err := client.Get(url)
			if err != nil {
				t.Errorf("client %d error: %v", id, err)
				return
			}
			defer func() { _ = resp.Body.Close() }()
			body, _ := io.ReadAll(resp.Body)
			expected := fmt.Sprintf("Hello, client%d!", id)
			if !strings.Contains(string(body), expected) {
				t.Errorf("client %d got unexpected body: %q", id, body)
			}
		}(i)
	}
	wg.Wait()
	_ = listener.Close()
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
