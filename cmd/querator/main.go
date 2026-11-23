package main

import (
	"cmp"
	"fmt"
	"os"

	"github.com/kapetan-io/querator"
	"github.com/spf13/cobra"
)

var flags FlagParams

type FlagParams struct {
	Endpoint string

	ConfigFile string
	Address    string
	LogLevel   string

	Payload   string
	Encoding  string
	Kind      string
	Reference string
	EnqueueAt string
	Timeout   string

	ClientId     string
	BatchSize    int32
	LeaseTimeout string

	File            string
	CompleteTimeout string
	Partition       int32

	LeaseTimeoutCreate string
	ExpireTimeout      string
	MaxAttempts        int32
	DeadQueue          string
	ReferenceCreate    string
	Partitions         int32

	Limit int32
	Pivot string

	LeaseTimeoutUpdate  string
	ExpireTimeoutUpdate string
	MaxAttemptsUpdate   int32
	DeadQueueUpdate     string
	ReferenceUpdate     string
	PartitionsUpdate    int32
	ClearDeadQueue      bool
	ClearReference      bool
	UnlimitedAttempts   bool

	Affinity    float64
	AffinitySet bool

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
		cmp.Or(os.Getenv("QUERATOR_ENDPOINT"), "http://localhost:2319"),
		"Querator server endpoint for API calls")

	root.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print the version number",
		Long:  "Print the version number of Querator",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("querator %s\n", querator.Version)
		},
	})

	root.AddCommand(NewServerCommand(os.Stdout))

	root.AddCommand(produceCommand)
	root.AddCommand(leaseCommand)
	if err := completeCommand.MarkFlagRequired("partition"); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	root.AddCommand(completeCommand)

	root.AddCommand(createCommand)
	root.AddCommand(listCommand)
	root.AddCommand(UpdateCommand)
	root.AddCommand(deleteCmd)
	root.AddCommand(backendsCommand)

	if err := root.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
