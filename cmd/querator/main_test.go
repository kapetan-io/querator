package main_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/proxy"
	"net"
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
		contains []string
	}{
		{
			name:     "ShouldStartWithNoConfigProvided",
			args:     []string{""},
			contains: []string{"Server Started"},
		},
		{
			name: "ShouldStartWithSampleConfig",
			args: []string{"-config=../../example.yaml"},
			contains: []string{
				"Server Started",
				"Loaded config from file",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			var buf bytes.Buffer
			w := bufio.NewWriter(&buf)

			waitCh := make(chan struct{})
			go func() {
				err := cli.Start(ctx, tt.args, w)
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
			w.Flush()
			for _, s := range tt.contains {
				//t.Logf("Checking for '%s' in output", s)
				assert.Contains(t, buf.String(), s)
			}
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
