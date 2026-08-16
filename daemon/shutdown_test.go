package daemon_test

import (
	"context"
	"testing"

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
