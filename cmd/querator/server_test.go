package main_test

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
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

func withServer(t *testing.T, params main.FlagParams, check func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var served error
	wg.Go(func() { served = main.StartServer(ctx, io.Discard, params) })
	check()
	cancel()
	wg.Wait()
	require.NoError(t, served)
}

func requireListening(t *testing.T, address string) {
	t.Helper()
	require.Eventually(t, func() bool {
		conn, err := net.Dial("tcp", address)
		return err == nil && conn.Close() == nil
	}, 5*time.Second, 20*time.Millisecond)
}

func TestServerAddress(t *testing.T) {
	t.Run("FlagSetsListenAddress", func(t *testing.T) {
		address := freeAddress(t)
		withServer(t, main.FlagParams{Address: address}, func() {
			requireListening(t, address)
		})
	})

	t.Run("ConfigFileSetsListenAddressWhenFlagUnset", func(t *testing.T) {
		address := freeAddress(t)
		path := filepath.Join(t.TempDir(), "config.yaml")
		require.NoError(t, os.WriteFile(path, []byte("address: "+address+"\n"), 0o600))
		withServer(t, main.FlagParams{ConfigFile: path}, func() {
			requireListening(t, address)
		})
	})

	t.Run("FlagWinsOverConfigFile", func(t *testing.T) {
		fileAddress := freeAddress(t)
		flagAddress := freeAddress(t)
		path := filepath.Join(t.TempDir(), "config.yaml")
		require.NoError(t, os.WriteFile(path, []byte("address: "+fileAddress+"\n"), 0o600))
		withServer(t, main.FlagParams{ConfigFile: path, Address: flagAddress}, func() {
			requireListening(t, flagAddress)
			_, err := net.DialTimeout("tcp", fileAddress, 200*time.Millisecond)
			require.Error(t, err)
		})
	})
}
