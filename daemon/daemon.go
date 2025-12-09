/*
Copyright 2024 Derrick J. Wippler

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package daemon

import (
	"context"
	"errors"
	"fmt"
	"github.com/duh-rpc/duh-go"
	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/internal"
	"github.com/kapetan-io/querator/service"
	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/tackle/set"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/net/netutil"
	"log/slog"
	"net"
	"net/http"
	"sync"
)

type Daemon struct {
	service  *service.Service
	client   *querator.Client
	servers  []*http.Server
	wg       sync.WaitGroup
	Listener net.Listener
	conf     Config
}

func NewDaemon(ctx context.Context, conf Config) (*Daemon, error) {
	set.Default(&conf.Service.Log, slog.Default())

	conf.SetDefaults()

	s, err := service.NewService(conf.Service)
	if err != nil {
		return nil, err
	}

	conf.Service.Log = conf.Service.Log.With("code.namespace", "Daemon")
	d := &Daemon{
		conf:    conf,
		service: s,
	}
	return d, d.Start(ctx)
}

func (d *Daemon) Start(ctx context.Context) error {
	registry := prometheus.NewRegistry()

	handler := transport.NewHTTPHandler(d.service, promhttp.InstrumentMetricHandler(
		registry, promhttp.HandlerFor(registry, promhttp.HandlerOpts{}),
	), d.conf.MaxProducePayloadSize, d.conf.Service.Log)
	registry.MustRegister(handler)

	switch {
	case d.conf.InMemoryListener:
		if err := d.spawnInMemory(ctx, handler); err != nil {
			return err
		}
	case d.conf.ServerTLS() != nil:
		if err := d.spawnHTTPS(ctx, handler); err != nil {
			return err
		}
	default:
		if err := d.spawnHTTP(ctx, handler); err != nil {
			return err
		}
	}
	return nil
}

func (d *Daemon) Shutdown(ctx context.Context) error {

	// See 0015-shutdown-errors.md for a discussion of shutdown operation
	if err := d.service.Shutdown(ctx); err != nil {
		return err
	}
	for _, srv := range d.servers {
		d.conf.Service.Log.LogAttrs(ctx, internal.LevelDebugAll, "Shutting down HTTP server", slog.String("address", srv.Addr))
		_ = srv.Shutdown(ctx)
		d.conf.Service.Log.Info("HTTP Server shutdown", "address", srv.Addr)
	}
	d.servers = nil
	return nil
}

func (d *Daemon) Service() *service.Service {
	return d.service
}

func (d *Daemon) MustClient() *querator.Client {
	c, err := d.Client()
	if err != nil {
		panic(fmt.Sprintf("[%s] failed to init daemon client - '%s'", d.conf.Service.InstanceID, err))
	}
	return c
}

func (d *Daemon) Client() (*querator.Client, error) {
	if d.conf.InMemoryListener {
		// Create a new net.Pipe for each client connection
		clientConn, serverConn := net.Pipe()

		// Serve the server side of the pipe through the InMemoryListener
		if inMemListener, ok := d.Listener.(*InMemoryListener); ok {
			if err := inMemListener.ServeConn(serverConn); err != nil {
				_ = clientConn.Close()
				_ = serverConn.Close()
				return nil, fmt.Errorf("failed to serve connection: %w", err)
			}
		} else {
			_ = clientConn.Close()
			_ = serverConn.Close()
			return nil, fmt.Errorf("InMemoryListener is enabled but listener is not of type *InMemoryListener")
		}

		// Create a new client using the client side of the pipe
		return querator.NewClient(querator.WithConn(clientConn))
	}

	// Original logic for non-InMemoryListener clients
	var err error
	if d.client != nil {
		return d.client, nil
	}

	if d.conf.TLS != nil {
		d.client, err = querator.NewClient(querator.WithTLS(d.conf.ClientTLS(), d.Listener.Addr().String()))
		return d.client, err
	}
	d.client, err = querator.NewClient(querator.WithNoTLS(d.Listener.Addr().String()))
	return d.client, err
}

func (d *Daemon) spawnHTTPS(ctx context.Context, mux http.Handler) error {
	srv := &http.Server{
		ErrorLog:  slog.NewLogLogger(d.conf.Service.Log.Handler(), slog.LevelError),
		TLSConfig: d.conf.ServerTLS().Clone(),
		Addr:      d.conf.ListenAddress,
		Handler:   mux,
	}

	var err error
	l, err := net.Listen("tcp", d.conf.ListenAddress)
	if err != nil {
		return fmt.Errorf("while starting HTTPS listener: %w", err)
	}
	d.Listener = netutil.LimitListener(l, d.conf.Service.MaxConcurrentRequests)
	srv.Addr = d.Listener.Addr().String()

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		d.conf.Service.Log.Info("HTTPS Listening ...", "address", d.Listener.Addr().String())
		if err := srv.ServeTLS(d.Listener, "", ""); err != nil {
			if !errors.Is(err, http.ErrServerClosed) {
				d.conf.Service.Log.Error("while starting TLS HTTP server", "error", err)
			}
		}
	}()
	if err := duh.WaitForConnect(ctx, d.Listener.Addr().String(), d.conf.ClientTLS()); err != nil {
		return err
	}

	d.servers = append(d.servers, srv)

	return nil
}

func (d *Daemon) spawnHTTP(ctx context.Context, h http.Handler) error {
	srv := &http.Server{
		ErrorLog: slog.NewLogLogger(d.conf.Service.Log.Handler(), slog.LevelError),
		Addr:     d.conf.ListenAddress,
		Handler:  h,
	}
	var err error
	l, err := net.Listen("tcp", d.conf.ListenAddress)
	if err != nil {
		return fmt.Errorf("while starting HTTP listener: %w", err)
	}
	// Limit the number of concurrent connections allowed to avoid abusing our resources.
	d.Listener = netutil.LimitListener(l, d.conf.Service.MaxConcurrentRequests)
	srv.Addr = d.Listener.Addr().String()

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		if err := srv.Serve(d.Listener); err != nil {
			if !errors.Is(err, http.ErrServerClosed) {
				d.conf.Service.Log.Error("while starting HTTP server", "error", err)
			}
		}
	}()

	if err := duh.WaitForConnect(ctx, d.Listener.Addr().String(), nil); err != nil {
		return err
	}

	d.conf.Service.Log.Info("HTTP Server Started", "address", d.Listener.Addr().String())
	d.servers = append(d.servers, srv)
	return nil
}

func (d *Daemon) spawnInMemory(_ context.Context, h http.Handler) error {
	srv := &http.Server{
		ErrorLog: slog.NewLogLogger(d.conf.Service.Log.Handler(), slog.LevelError),
		Handler:  h,
	}

	d.Listener = NewInMemoryListener()
	srv.Addr = d.Listener.Addr().String()

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		if err := srv.Serve(d.Listener); err != nil {
			if !errors.Is(err, http.ErrServerClosed) {
				d.conf.Service.Log.Error("while starting InMemory server", "error", err)
			}
		}
	}()

	d.conf.Service.Log.Info("InMemory Server Started", "address", d.Listener.Addr().String())
	d.servers = append(d.servers, srv)
	return nil
}
