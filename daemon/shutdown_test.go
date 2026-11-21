package daemon_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/kapetan-io/querator/daemon"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestDaemonShutdown(t *testing.T) {
	// IgnoreCurrent is evaluated before the daemons start, so only goroutines
	// created by this test are checked.
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	ctx := context.Background()

	t.Run("HTTP", func(t *testing.T) {
		d, err := daemon.NewDaemon(ctx, daemon.Config{ListenAddress: "localhost:0"})
		require.NoError(t, err)
		require.NoError(t, d.Shutdown(ctx))
	})

	t.Run("InMemory", func(t *testing.T) {
		d, err := daemon.NewDaemon(ctx, daemon.Config{InMemoryListener: true})
		require.NoError(t, err)
		require.NoError(t, d.Shutdown(ctx))
	})
}

func TestDaemonShutdownReportsExpiredDeadline(t *testing.T) {
	ctx := context.Background()
	d, err := daemon.NewDaemon(ctx, daemon.Config{InMemoryListener: true})
	require.NoError(t, err)

	// ServeConn returns once the server has accepted the connection. One that has sent no request
	// stays open through http.Server.Shutdown for its first 5 seconds, so a shorter deadline expires first.
	serverConn, clientConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()
	require.NoError(t, d.Listener.(*daemon.InMemoryListener).ServeConn(serverConn))

	timeout, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, d.Shutdown(timeout), context.DeadlineExceeded)
}

func TestDaemonStartTwice(t *testing.T) {
	ctx := context.Background()
	d, err := daemon.NewDaemon(ctx, daemon.Config{InMemoryListener: true})
	require.NoError(t, err)

	// A second server would replace the first, which Shutdown could then never stop.
	require.ErrorContains(t, d.Start(ctx), "daemon already started")

	timeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	require.NoError(t, d.Shutdown(timeout))
}

func TestDaemonShutdownClosesListenerWhenServiceShutdownFails(t *testing.T) {
	ctx := context.Background()
	d, err := daemon.NewDaemon(ctx, daemon.Config{InMemoryListener: true})
	require.NoError(t, err)

	// A canceled ctx makes the service shutdown fail before the HTTP server's turn, which must
	// still stop the server. The queue drain can finish first (about 1 run in 1,500), and then
	// Shutdown succeeds.
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := d.Shutdown(canceled); err != nil {
		require.ErrorIs(t, err, context.Canceled)
	}

	serverConn, clientConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()
	require.ErrorIs(t, d.Listener.(*daemon.InMemoryListener).ServeConn(serverConn), net.ErrClosed)
}
