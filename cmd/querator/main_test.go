package main_test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/proxy"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestCLI(t *testing.T) {
	// Build the binary for testing
	binPath := buildTestBinary(t)
	defer func() {
		_ = os.Remove(binPath)
	}()

	t.Run("HelpCommand", func(t *testing.T) {
		cmd := exec.Command(binPath, "--help")
		output, err := cmd.CombinedOutput()
		assert.NoError(t, err)

		outputStr := string(output)
		assert.Contains(t, outputStr, "Querator is a distributed queue system")
		assert.Contains(t, outputStr, "server")
		assert.Contains(t, outputStr, "version")
		assert.Contains(t, outputStr, "--endpoint")
	})

	t.Run("VersionCommand", func(t *testing.T) {
		cmd := exec.Command(binPath, "version")
		output, err := cmd.CombinedOutput()
		assert.NoError(t, err)

		outputStr := string(output)
		assert.Contains(t, outputStr, "querator dev-build")
	})

	t.Run("ServerHelpCommand", func(t *testing.T) {
		cmd := exec.Command(binPath, "server", "--help")
		output, err := cmd.CombinedOutput()
		assert.NoError(t, err)

		outputStr := string(output)
		assert.Contains(t, outputStr, "Start the Querator daemon")
		assert.Contains(t, outputStr, "--config")
		assert.Contains(t, outputStr, "--address")
		assert.Contains(t, outputStr, "--log-level")
	})

	t.Run("EndpointGlobalFlag", func(t *testing.T) {
		cmd := exec.Command(binPath, "--endpoint", "http://test:8080", "version")
		output, err := cmd.CombinedOutput()
		assert.NoError(t, err)

		outputStr := string(output)
		assert.Contains(t, outputStr, "querator dev-build")
	})

	t.Run("InvalidCommand", func(t *testing.T) {
		cmd := exec.Command(binPath, "invalid-command")
		output, err := cmd.CombinedOutput()
		assert.Error(t, err)

		outputStr := string(output)
		assert.Contains(t, outputStr, "unknown command")
	})

	t.Run("ServerWithoutConfig", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		cmd := exec.CommandContext(ctx, binPath, "server")

		// Start the server
		err := cmd.Start()
		assert.NoError(t, err)

		// Ensure process cleanup
		defer func() {
			if cmd.Process != nil {
				_ = cmd.Process.Signal(os.Interrupt)
				_ = cmd.Wait()
			}
		}()

		// Wait for server to accept connections
		err = waitForConnect(ctx, "localhost:2319", nil)
		assert.NoError(t, err)

		// If we can connect, the server started successfully
		// This test verifies the server starts without config
	})

	t.Run("ServerWithConfig", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		cmd := exec.CommandContext(ctx, binPath, "server", "--config", "../../example.yaml")

		// Start the server
		err := cmd.Start()
		assert.NoError(t, err)

		// Wait for server to accept connections
		err = waitForConnect(ctx, "localhost:2319", nil)
		assert.NoError(t, err)

		// If we can connect, the server started successfully with config
		// This test verifies the server starts with the example.yaml config
	})

	t.Run("ProduceCommandHelp", func(t *testing.T) {
		cmd := exec.Command(binPath, "produce", "--help")
		output, err := cmd.CombinedOutput()
		assert.NoError(t, err)

		outputStr := string(output)
		assert.Contains(t, outputStr, "Produce items to a queue")
		assert.Contains(t, outputStr, "--payload")
		assert.Contains(t, outputStr, "--timeout")
		assert.Contains(t, outputStr, "--encoding")
	})

	t.Run("LeaseCommandHelp", func(t *testing.T) {
		cmd := exec.Command(binPath, "lease", "--help")
		output, err := cmd.CombinedOutput()
		assert.NoError(t, err)

		outputStr := string(output)
		assert.Contains(t, outputStr, "Lease items from a queue")
		assert.Contains(t, outputStr, "--client-id")
		assert.Contains(t, outputStr, "--batch-size")
		assert.Contains(t, outputStr, "--timeout")
	})

	t.Run("CompleteCommandHelp", func(t *testing.T) {
		cmd := exec.Command(binPath, "complete", "--help")
		output, err := cmd.CombinedOutput()
		assert.NoError(t, err)

		outputStr := string(output)
		assert.Contains(t, outputStr, "Mark leased items as complete")
		assert.Contains(t, outputStr, "--timeout")
		assert.Contains(t, outputStr, "--file")
	})

	t.Run("CreateCommandHelp", func(t *testing.T) {
		cmd := exec.Command(binPath, "create", "--help")
		output, err := cmd.CombinedOutput()
		assert.NoError(t, err)

		outputStr := string(output)
		assert.Contains(t, outputStr, "Create a new queue")
		assert.Contains(t, outputStr, "--lease-timeout")
		assert.Contains(t, outputStr, "--max-attempts")
	})

	t.Run("ListCommandHelp", func(t *testing.T) {
		cmd := exec.Command(binPath, "list", "--help")
		output, err := cmd.CombinedOutput()
		assert.NoError(t, err)

		outputStr := string(output)
		assert.Contains(t, outputStr, "List all queues")
		assert.Contains(t, outputStr, "--limit")
		assert.Contains(t, outputStr, "--pivot")
	})

	t.Run("UpdateCommandHelp", func(t *testing.T) {
		cmd := exec.Command(binPath, "update", "--help")
		output, err := cmd.CombinedOutput()
		assert.NoError(t, err)

		outputStr := string(output)
		assert.Contains(t, outputStr, "Update an existing queue")
		assert.Contains(t, outputStr, "--lease-timeout")
		assert.Contains(t, outputStr, "--max-attempts")
	})

	t.Run("DeleteCommandHelp", func(t *testing.T) {
		cmd := exec.Command(binPath, "delete", "--help")
		output, err := cmd.CombinedOutput()
		assert.NoError(t, err)

		outputStr := string(output)
		assert.Contains(t, outputStr, "Delete a queue")
		assert.Contains(t, outputStr, "--force")
	})
}

func buildTestBinary(t *testing.T) string {
	binPath := "./querator-test"
	cmd := exec.Command("go", "build", "-o", binPath, ".")
	err := cmd.Run()
	assert.NoError(t, err, "Failed to build test binary")
	return binPath
}

// waitForConnect waits until the passed address is accepting connections.
// It will continue to attempt a connection until context is canceled.
func waitForConnect(ctx context.Context, address string, cfg *tls.Config) error {
	if address == "" {
		return fmt.Errorf("waitForConnect() requires a valid address")
	}

	var errs []string
	for {
		var d proxy.ContextDialer
		if cfg != nil {
			d = &tls.Dialer{Config: cfg}
		} else {
			d = &net.Dialer{}
		}
		conn, err := d.DialContext(ctx, "tcp", address)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		errs = append(errs, err.Error())
		if ctx.Err() != nil {
			errs = append(errs, ctx.Err().Error())
			return errors.New(strings.Join(errs, "\n"))
		}
		time.Sleep(time.Millisecond * 100)
		continue
	}
}
