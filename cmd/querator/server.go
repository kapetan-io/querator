package main

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/daemon"
	"github.com/spf13/cobra"
)

func NewServerCommand(w io.Writer) *cobra.Command {
	var params FlagParams
	command := &cobra.Command{
		Use:   "server",
		Short: "Start the Querator daemon",
		Long: `Start the Querator daemon server.

The server command starts the HTTP API server that handles queue operations.
Configuration can be provided via flags, environment variables, or a config file.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if params.Address == "" {
				params.Address = os.Getenv("QUERATOR_ADDRESS")
			}
			return StartServer(cmd.Context(), w, params)
		},
	}
	command.Flags().StringVar(&params.ConfigFile, "config",
		os.Getenv("QUERATOR_CONFIG"),
		"Configuration file path")
	command.Flags().StringVar(&params.Address, "address", "",
		"HTTP address to bind. When not set, QUERATOR_ADDRESS is used, then the config file's address")
	command.Flags().StringVar(&params.LogLevel, "log-level",
		cmp.Or(os.Getenv("QUERATOR_LOG_LEVEL"), "info"),
		"Logging level (debug,error,warn,info)")
	return command
}

func StartServer(ctx context.Context, w io.Writer, params FlagParams) error {
	var file daemon.File
	if params.ConfigFile != "" {
		reader, err := os.Open(params.ConfigFile)
		if err != nil {
			return fmt.Errorf("while opening config file: %w", err)
		}
		defer func() { _ = reader.Close() }()

		file, err = daemon.ReadConfig(reader)
		if err != nil {
			return err
		}
		file.ConfigFile = params.ConfigFile
	}

	var conf daemon.Config
	if err := daemon.ApplyConfigFile(ctx, &conf, file, w); err != nil {
		return fmt.Errorf("while applying config file: %w", err)
	}

	if params.Address != "" {
		conf.ListenAddress = params.Address
	}

	conf.Service.Log.Info(fmt.Sprintf("Querator %s (%s/%s)", querator.Version, runtime.GOARCH, runtime.GOOS))
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
