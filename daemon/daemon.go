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
	"github.com/kapetan-io/tackle/set"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"log/slog"
	"net"
	"net/http"
	"sync"
)

type Daemon struct {
	service    *internal.Service
	logAdaptor *duh.HttpLogAdaptor
	servers    []*http.Server
	wg         sync.WaitGroup
	Listener   net.Listener
	conf       Config
}

func NewDaemon(ctx context.Context, conf Config) (*Daemon, error) {
	set.Default(&conf.Logger, slog.Default())

	d := &Daemon{
		service: internal.NewService(internal.ServiceOptions{}),
		conf:    conf,
	}
	return d, d.Start(ctx)
}

func (d *Daemon) Start(ctx context.Context) error {
	registry := prometheus.NewRegistry()

	handler := internal.NewHandler(d.service, promhttp.InstrumentMetricHandler(
		registry, promhttp.HandlerFor(registry, promhttp.HandlerOpts{}),
	))
	registry.MustRegister(handler)

	if d.conf.ServerTLS() != nil {
		if err := d.spawnHTTPS(ctx, handler); err != nil {
			return err
		}
	} else {
		if err := d.spawnHTTP(ctx, handler); err != nil {
			return err
		}
	}
	return nil
}

func (d *Daemon) Shutdown(ctx context.Context) error {
	for _, srv := range d.servers {
		d.conf.Logger.Info("Shutting down server", "address", srv.Addr)
		_ = srv.Shutdown(ctx)
	}
	d.servers = nil
	return nil
}

func (d *Daemon) MustClient() *querator.Client {
	c, err := d.Client()
	if err != nil {
		panic(fmt.Sprintf("[%s] failed to init daemon client - '%d'", d.conf.InstanceID, err))
	}
	return c
}

func (d *Daemon) Client() (*querator.Client, error) {
	if d.conf.TLS != nil {
		return querator.NewClient(querator.WithTLS(d.conf.ClientTLS(), d.Listener.Addr().String()))
	}
	return querator.NewClient(querator.WithNoTLS(d.Listener.Addr().String()))
}

func (d *Daemon) spawnHTTPS(ctx context.Context, mux http.Handler) error {
	srv := &http.Server{
		ErrorLog:  log.New(d.logAdaptor, "", 0),
		TLSConfig: d.conf.ServerTLS().Clone(),
		Addr:      d.conf.ListenAddress,
		Handler:   mux,
	}

	var err error
	d.Listener, err = net.Listen("tcp", d.conf.ListenAddress)
	if err != nil {
		return fmt.Errorf("while starting HTTPS listener: %w", err)
	}
	srv.Addr = d.Listener.Addr().String()

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		d.conf.Logger.Info("HTTPS Listening ...", "address", d.Listener.Addr().String())
		if err := srv.ServeTLS(d.Listener, "", ""); err != nil {
			if !errors.Is(err, http.ErrServerClosed) {
				d.conf.Logger.Error("while starting TLS HTTP server", "error", err)
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
		ErrorLog: log.New(d.logAdaptor, "", 0),
		Addr:     d.conf.ListenAddress,
		Handler:  h,
	}
	var err error
	d.Listener, err = net.Listen("tcp", d.conf.ListenAddress)
	if err != nil {
		return fmt.Errorf("while starting HTTP listener: %w", err)
	}
	srv.Addr = d.Listener.Addr().String()

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		d.conf.Logger.Info("HTTP Listening ...", "address", d.Listener.Addr().String())
		if err := srv.Serve(d.Listener); err != nil {
			if !errors.Is(err, http.ErrServerClosed) {
				d.conf.Logger.Error("while starting HTTP server", "error", err)
			}
		}
	}()

	if err := duh.WaitForConnect(ctx, d.Listener.Addr().String(), nil); err != nil {
		return err
	}

	d.servers = append(d.servers, srv)
	return nil
}
