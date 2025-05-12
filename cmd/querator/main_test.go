package main_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/proxy"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	cli "github.com/kapetan-io/querator/cmd/querator"
)

func TestCLI(t *testing.T) {
	tests := []struct {
		args     []string
		config   string
		name     string
		contains string
	}{
		{
			name:     "ShouldStartWithNoConfigProvided",
			args:     []string{"querator"},
			config:   "",
			contains: "Starting querator",
		},
		{
			name:     "ShouldStartWithValidConfig",
			args:     []string{"querator"},
			config:   validConfig,
			contains: "Starting querator",
		},
		{
			name:     "ShouldStartWithInValidConfig",
			args:     []string{"querator"},
			config:   invalidConfig,
			contains: "!!!!",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// write a config file and add it to the args if provided
			if tt.config != "" {
				file := writeFile(t, tt.config)
				tt.args = append(tt.args, "--config", file)
				defer os.RemoveAll(file)
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			defer cancel()
			var out bytes.Buffer

			waitCh := make(chan struct{})
			go func() {
				err := cli.Start(ctx, tt.args, &out)
				if err != nil {
					t.Logf("cli.Start() returned error: '%v'", err)
				}
				close(waitCh)
			}()

			err := waitForConnect(ctx, "localhost:2319", nil)
			assert.NoError(t, err)
			time.Sleep(time.Second * 1)
			cancel()

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

func writeFile(t *testing.T, contents string) string {
	t.Helper()
	path, err := os.MkdirTemp("/tmp/", "querator")
	require.NoError(t, err)
	file := path + "/" + random.String("", 10)
	f, err := os.Create(file)
	defer f.Close()
	_, err = f.WriteString(contents)
	require.NoError(t, err)
	return file
}

const (
	invalidConfig = `
queues:
  - name: queue-1
     lease-timeout: 10m
    expire-timeout: 10m
    dead-queue: queue-1-dead
    max-attempts: 10
    reference: test
     requested-partitions: 20
 partitions:
      - partition: 0
        read-only: false
        storage-name: badger-00

backends:
  - name: badger-00
    driver: Badger 
    affinity: 0

    config:
      storage-dir: "/tmp/badger1"
`
	validConfig = `
queues:
  - name: queue-1
    lease-timeout: 10m
    expire-timeout: 10m
    dead-queue: queue-1-dead
    max-attempts: 10
    reference: test
    requested-partitions: 20
    partitions:
      - partition: 0
        read-only: false
        storage-name: badger-00

backends:
  - name: badger-00
    driver: Badger 
    affinity: 0

    config:
      storage-dir: "/tmp/badger1"
`
)
