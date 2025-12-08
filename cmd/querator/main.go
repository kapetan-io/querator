package main

import (
	"fmt"
	"os"

	"github.com/kapetan-io/querator"
	"github.com/spf13/cobra"
)

var flags FlagParams

type FlagParams struct {
	// Global Flags
	Endpoint string

	// Server Flags
	ConfigFile string
	Address    string
	LogLevel   string

	// Produce Flags
	Payload   string
	Encoding  string
	Kind      string
	Reference string
	EnqueueAt string
	Timeout   string

	// Lease Flags
	ClientId     string
	BatchSize    int32
	LeaseTimeout string

	// Complete Flags
	File            string
	CompleteTimeout string
	Partition       int32

	// Create Flags
	LeaseTimeoutCreate string
	ExpireTimeout      string
	MaxAttempts        int32
	DeadQueue          string
	ReferenceCreate    string
	Partitions         int32

	// List Flags
	Limit int32
	Pivot string

	// Update Flags
	LeaseTimeoutUpdate  string
	ExpireTimeoutUpdate string
	MaxAttemptsUpdate   int32
	DeadQueueUpdate     string
	ReferenceUpdate     string
	PartitionsUpdate    int32

	// Delete Flags
	Force bool
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
			fmt.Printf("querator %s\n", querator.Version)
		},
	})

	// Server
	root.AddCommand(serverCommand)

	// Queue Commands
	root.AddCommand(produceCommand)
	root.AddCommand(leaseCommand)
	root.AddCommand(completeCommand)

	// Queue Management Commands
	root.AddCommand(createCommand)
	root.AddCommand(listCommand)
	root.AddCommand(UpdateCommand)
	root.AddCommand(deleteCmd)

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
