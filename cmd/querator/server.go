package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/kapetan-io/querator/config"
	"github.com/kapetan-io/querator/daemon"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

var serverCommand = &cobra.Command{
	Use:   "server",
	Short: "Start the Querator daemon",
	Long: `Start the Querator daemon server.

The server command starts the HTTP API server that handles queue operations.
Configuration can be provided via flags, environment variables, or a config file.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return startServer(context.Background(), os.Stdout)
	},
}

func startServer(ctx context.Context, w io.Writer) error {
	var file config.File
	if flags.ConfigFile != "" {
		reader, err := os.Open(flags.ConfigFile)
		if err != nil {
			return fmt.Errorf("while opening config file: %w", err)
		}
		defer func() { _ = reader.Close() }()

		decoder := yaml.NewDecoder(reader)
		if err := decoder.Decode(&file); err != nil {
			return fmt.Errorf("while reading config file: %w", err)
		}
		file.ConfigFile = flags.ConfigFile
	}

	var conf daemon.Config
	if err := config.ApplyConfigFile(ctx, &conf, file, w); err != nil {
		return fmt.Errorf("while applying config file: %w", err)
	}

	conf.Log.Info(fmt.Sprintf("Querator %s (%s/%s)", Version, runtime.GOARCH, runtime.GOOS))
	d, err := daemon.NewDaemon(ctx, conf)
	if err != nil {
		return fmt.Errorf("while creating daemon: %w", err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	select {
	case <-c:
		return d.Shutdown(ctx)
	case <-ctx.Done():
		return d.Shutdown(context.Background())
	}
}
