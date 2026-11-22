package main_test

import (
	"bytes"
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

type logBuffer struct {
	mutex  sync.Mutex
	buffer bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.Write(p)
}

func (b *logBuffer) String() string {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.String()
}

type levelFile struct {
	Address string
	Level   string
}

func writeLevelConfig(t *testing.T, file levelFile) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	content := "address: " + file.Address + "\nlogging:\n  level: " + file.Level + "\n"
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func withServer(t *testing.T, args []string, check func()) {
	t.Helper()
	withLoggedServer(t, io.Discard, args, check)
}

func withLoggedServer(t *testing.T, w io.Writer, args []string, check func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var served error
	command := main.NewServerCommand(w)
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
		withServer(t, []string{}, func() {
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

func TestServerLogLevel(t *testing.T) {
	const debugLine = "Shutdown complete"
	const infoLine = "HTTP Server Started"

	t.Run("FlagSetsLogLevel", func(t *testing.T) {
		address := freeAddress(t)
		t.Setenv("QUERATOR_LOG_LEVEL", "")
		logs := &logBuffer{}
		args := []string{"--log-level", "debug", "--config", writeLevelConfig(t, levelFile{Address: address, Level: "error"})}
		withLoggedServer(t, logs, args, func() {
			requireListening(t, address)
		})
		require.Contains(t, logs.String(), debugLine)
	})

	t.Run("EnvSetsLogLevel", func(t *testing.T) {
		address := freeAddress(t)
		t.Setenv("QUERATOR_LOG_LEVEL", "debug")
		logs := &logBuffer{}
		withLoggedServer(t, logs, []string{"--config", writeLevelConfig(t, levelFile{Address: address, Level: "error"})}, func() {
			requireListening(t, address)
		})
		require.Contains(t, logs.String(), debugLine)
	})

	t.Run("ConfigFileSetsLogLevelWhenNothingElseSet", func(t *testing.T) {
		address := freeAddress(t)
		t.Setenv("QUERATOR_LOG_LEVEL", "")
		logs := &logBuffer{}
		withLoggedServer(t, logs, []string{"--config", writeLevelConfig(t, levelFile{Address: address, Level: "error"})}, func() {
			requireListening(t, address)
		})
		require.NotContains(t, logs.String(), debugLine)
		require.NotContains(t, logs.String(), infoLine)
	})

	t.Run("ConfigFileDebugStillApplies", func(t *testing.T) {
		address := freeAddress(t)
		t.Setenv("QUERATOR_LOG_LEVEL", "")
		logs := &logBuffer{}
		withLoggedServer(t, logs, []string{"--config", writeLevelConfig(t, levelFile{Address: address, Level: "debug"})}, func() {
			requireListening(t, address)
		})
		require.Contains(t, logs.String(), debugLine)
	})

	t.Run("FlagWinsOverEnvAndConfigFile", func(t *testing.T) {
		address := freeAddress(t)
		t.Setenv("QUERATOR_LOG_LEVEL", "debug")
		logs := &logBuffer{}
		args := []string{"--log-level", "error", "--config", writeLevelConfig(t, levelFile{Address: address, Level: "debug"})}
		withLoggedServer(t, logs, args, func() {
			requireListening(t, address)
		})
		require.NotContains(t, logs.String(), debugLine)
	})

	t.Run("EnvWinsOverConfigFile", func(t *testing.T) {
		address := freeAddress(t)
		t.Setenv("QUERATOR_LOG_LEVEL", "error")
		logs := &logBuffer{}
		withLoggedServer(t, logs, []string{"--config", writeLevelConfig(t, levelFile{Address: address, Level: "debug"})}, func() {
			requireListening(t, address)
		})
		require.NotContains(t, logs.String(), debugLine)
	})

	t.Run("FlagSetsLogLevelWithoutConfigFile", func(t *testing.T) {
		address := freeAddress(t)
		t.Setenv("QUERATOR_LOG_LEVEL", "")
		t.Setenv("QUERATOR_CONFIG", "")
		logs := &logBuffer{}
		withLoggedServer(t, logs, []string{"--log-level", "debug", "--address", address}, func() {
			requireListening(t, address)
		})
		require.Contains(t, logs.String(), debugLine)
	})

	t.Run("ExplicitInfoFlagWinsOverDebugConfigFile", func(t *testing.T) {
		address := freeAddress(t)
		t.Setenv("QUERATOR_LOG_LEVEL", "")
		logs := &logBuffer{}
		args := []string{"--log-level", "info", "--config", writeLevelConfig(t, levelFile{Address: address, Level: "debug"})}
		withLoggedServer(t, logs, args, func() {
			requireListening(t, address)
		})
		require.Contains(t, logs.String(), infoLine)
		require.NotContains(t, logs.String(), debugLine)
	})
}
