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

func writeConfig(t *testing.T, address string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("address: "+address+"\n"), 0o600))
	return path
}

func withServer(t *testing.T, args []string, check func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var served error
	command := main.NewServerCommand(io.Discard)
	command.SetArgs(args)
	wg.Go(func() { served = command.ExecuteContext(ctx) })
	defer func() {
		cancel()
		wg.Wait()
		require.NoError(t, served)
	}()
	check()
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
		withServer(t, []string{"--address", address}, func() {
			requireListening(t, address)
		})
	})

	t.Run("EnvSetsListenAddress", func(t *testing.T) {
		address := freeAddress(t)
		t.Setenv("QUERATOR_ADDRESS", address)
		withServer(t, nil, func() {
			requireListening(t, address)
		})
	})

	t.Run("ConfigFileSetsListenAddressWhenNothingElseSet", func(t *testing.T) {
		address := freeAddress(t)
		t.Setenv("QUERATOR_ADDRESS", "")
		withServer(t, []string{"--config", writeConfig(t, address)}, func() {
			requireListening(t, address)
		})
	})

	t.Run("FlagWinsOverEnvAndConfigFile", func(t *testing.T) {
		fileAddress := freeAddress(t)
		envAddress := freeAddress(t)
		flagAddress := freeAddress(t)
		t.Setenv("QUERATOR_ADDRESS", envAddress)
		args := []string{"--address", flagAddress, "--config", writeConfig(t, fileAddress)}
		withServer(t, args, func() {
			requireListening(t, flagAddress)
			_, err := net.DialTimeout("tcp", fileAddress, 200*time.Millisecond)
			require.Error(t, err)
			_, err = net.DialTimeout("tcp", envAddress, 200*time.Millisecond)
			require.Error(t, err)
		})
	})

	t.Run("EnvWinsOverConfigFile", func(t *testing.T) {
		fileAddress := freeAddress(t)
		envAddress := freeAddress(t)
		t.Setenv("QUERATOR_ADDRESS", envAddress)
		withServer(t, []string{"--config", writeConfig(t, fileAddress)}, func() {
			requireListening(t, envAddress)
			_, err := net.DialTimeout("tcp", fileAddress, 200*time.Millisecond)
			require.Error(t, err)
		})
	})
}
