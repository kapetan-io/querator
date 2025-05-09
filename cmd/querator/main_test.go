package main_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/proxy"
	"net"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	cli "github.com/kapetan-io/querator/cmd/querator"
)

var configFiles = map[string]string{
	"invalid-config.yaml": `	`,
	"valid-config.yaml":   `	`,
}

var cliRunning = flag.Bool("test_cli_running", false, "True if running as a child process; used by TestCLI")

func TestCLI(t *testing.T) {
	if *cliRunning {
		if err := cli.Start(context.Background()); err != nil {
			fmt.Print(err.Error())
			os.Exit(1)
		}
		os.Exit(0)
	}

	tests := []struct {
		args     []string
		config   string
		name     string
		contains string
	}{
		{
			name:     "ShouldStartWithNoConfigProvided",
			args:     []string{},
			config:   "",
			contains: "Starting querator",
		},
		{
			name:     "ShouldStartWithValidConfig",
			args:     []string{},
			config:   "valid-config.yaml",
			contains: "Starting querator",
		},
		{
			name:     "ShouldStartWithInValidConfig",
			args:     []string{},
			config:   "",
			contains: "!!!!",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// write a config file and add it to the args if provided
			if tt.config != "" {
				file := writeFile(t, tt.config, configFiles[tt.config])
				tt.args = append(tt.args, "--config", file)
				defer os.RemoveAll(file)
			}

			c := exec.Command(os.Args[0], append([]string{"--test.run=TestCLI", "--test_cli_running"}, tt.args...)...)
			var out bytes.Buffer
			c.Stdout = &out
			c.Stderr = &out

			if err := c.Start(); err != nil {
				t.Fatal("failed to start child process: ", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			defer cancel()

			waitCh := make(chan struct{})
			go func() {
				_ = c.Wait()
				close(waitCh)
			}()

			err := waitForConnect(ctx, "localhost:2319", nil)
			assert.NoError(t, err)
			time.Sleep(time.Second * 1)

			err = c.Process.Signal(syscall.SIGTERM)
			require.NoError(t, err)

			<-waitCh
			assert.Contains(t, out.String(), tt.contains)
		})
	}
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

func writeFile(t *testing.T, name, contents string) string {
	path, err := os.MkdirTemp("/tmp/", "querator")
	require.NoError(t, err)
	f, err := os.Create(path + "/" + name)
	defer f.Close()
	_, err = f.WriteString(contents)
	require.NoError(t, err)
	return path + "/" + name
}
