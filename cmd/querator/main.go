package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"

	"github.com/kapetan-io/querator/config"
	"github.com/kapetan-io/querator/daemon"
	"gopkg.in/yaml.v3"
)

var Version = "dev-build"

type FlagParams struct {
	ConfigFile  string
	ShowVersion bool
}

func main() {
	if err := Start(context.Background(), os.Args[1:], os.Stdout); err != nil {
		os.Exit(1)
	}
}

func Start(ctx context.Context, args []string, w io.Writer) error {
	flags, err := parseFlags(args)
	if err != nil {
		return err
	}

	if flags.ShowVersion {
		fmt.Fprintf(w, "querator %s\n", Version)
		return nil
	}

	var file config.File
	if flags.ConfigFile != "" {
		reader, err := os.Open(flags.ConfigFile)
		if err != nil {
			return fmt.Errorf("while opening config file: %w", err)
		}
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

func parseFlags(args []string) (FlagParams, error) {
	var flagParams FlagParams

	flags := flag.NewFlagSet("querator", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&flagParams.ConfigFile, "config", "", "environment config file")
	flags.BoolVar(&flagParams.ShowVersion, "version", false, "show version information")
	if err := flags.Parse(args); err != nil {
		if !strings.Contains(err.Error(), "flag provided but not defined") {
			return FlagParams{}, fmt.Errorf("while parsing flags: %w", err)
		}
	}
	return flagParams, nil
}
