package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
)

var (
	Version = "dev-build"
	flags   FlagParams
)

type FlagParams struct {
	// Global Flags
	Endpoint string

	// Server Flags
	ConfigFile string
	Address    string
	LogLevel   string
}

func main() {
	var root = &cobra.Command{
		Use:   "querator",
		Short: "Querator is a distributed queue system",
		Long: `Querator is a distributed queue system that provides reliable,
scalable message queuing with lease-based processing semantics.

Use 'querator server' to start the daemon, or use the various subcommands
to interact with a running Querator instance via its HTTP API.`,
	}

	root.PersistentFlags().StringVar(&flags.Endpoint, "endpoint",
		getEnv("QUERATOR_ENDPOINT", "http://localhost:2319"),
		"Querator server endpoint for API calls")

	// ======== Version =========
	root.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print the version number",
		Long:  "Print the version number of Querator",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("querator %s\n", Version)
		},
	})

	// ======== Server =========
	serverCommand.Flags().StringVar(&flags.ConfigFile, "config",
		getEnv("QUERATOR_CONFIG", ""),
		"Configuration file path")
	serverCommand.Flags().StringVar(&flags.Address, "address",
		getEnv("QUERATOR_ADDRESS", "localhost:2319"),
		"HTTP address to bind")
	serverCommand.Flags().StringVar(&flags.LogLevel, "log-level",
		getEnv("QUERATOR_LOG_LEVEL", "info"),
		"Logging level (debug,error,warn,info)")
	root.AddCommand(serverCommand)

	if err := root.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func getEnv(envVar, defaultValue string) string {
	if value := os.Getenv(envVar); value != "" {
		return value
	}
	return defaultValue
}
