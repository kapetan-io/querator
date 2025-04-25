package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/kapetan-io/querator/daemon"
	"github.com/kapetan-io/querator/pkg/config"
)

var log = slog.New(slog.NewTextHandler(os.Stdout, nil))

type FlagParams struct {
	ConfigFile string
}

func main() {
	err := Start(context.Background())
	if err != nil {
		log.Error("failed", "error", err)
		os.Exit(1)
	}
}

func Start(ctx context.Context) error {
	flagParams, err := parseFlags()
	if err != nil {
		return err
	}

	cfg, err := config.LoadFile(flagParams.ConfigFile)
	if err != nil {
		return err
	}

	dmnCfg, err := cfg.ToDaemonConfig(ctx, log)
	if err != nil {
		return err
	}

	dmn, err := daemon.NewDaemon(ctx, dmnCfg)
	if err != nil {
		return err
	}

	err = waitForShutdown(ctx, dmn)
	if err != nil {
		return err
	}

	return nil
}

func parseFlags() (FlagParams, error) {
	var flagParams FlagParams

	flags := flag.NewFlagSet("querator", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&flagParams.ConfigFile, "config", "", "environment config file")
	if err := flags.Parse(os.Args[1:]); err != nil {
		if !strings.Contains(err.Error(), "flag provided but not defined") {
			return FlagParams{}, fmt.Errorf("while parsing flags: %w", err)
		}
	}

	return flagParams, nil
}

func waitForShutdown(ctx context.Context, d *daemon.Daemon) error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	select {
	case <-c:
		log.Info("caught signal; shutting down")
		d.Shutdown(ctx)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
