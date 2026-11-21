package main_test

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kapetan-io/querator/cmd/querator"
)

func freeAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := l.Addr().String()
	require.NoError(t, l.Close())
	return address
}

func runServer(t *testing.T, params main.FlagParams) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- main.StartServer(ctx, io.Discard, params) }()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-done)
	})
}

func requireListening(t *testing.T, address string) {
	t.Helper()
	require.Eventually(t, func() bool {
		conn, err := net.Dial("tcp", address)
		if err != nil {
			return false
		}
		_ = conn.Close()
		return true
	}, 5*time.Second, 20*time.Millisecond)
}

func TestServerAddress(t *testing.T) {
	t.Run("FlagSetsListenAddress", func(t *testing.T) {
		address := freeAddress(t)
		runServer(t, main.FlagParams{Address: address})
		requireListening(t, address)
	})

	t.Run("ConfigFileSetsListenAddressWhenFlagUnset", func(t *testing.T) {
		address := freeAddress(t)
		path := filepath.Join(t.TempDir(), "config.yaml")
		require.NoError(t, os.WriteFile(path, []byte("address: "+address+"\n"), 0o600))
		runServer(t, main.FlagParams{ConfigFile: path})
		requireListening(t, address)
	})

	t.Run("FlagWinsOverConfigFile", func(t *testing.T) {
		fileAddress := freeAddress(t)
		flagAddress := freeAddress(t)
		path := filepath.Join(t.TempDir(), "config.yaml")
		require.NoError(t, os.WriteFile(path, []byte("address: "+fileAddress+"\n"), 0o600))
		runServer(t, main.FlagParams{ConfigFile: path, Address: flagAddress})
		requireListening(t, flagAddress)
		_, err := net.DialTimeout("tcp", fileAddress, 200*time.Millisecond)
		require.Error(t, err)
	})
}
