package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/kapetan-io/tackle/random"
	"os"
	"time"

	"github.com/kapetan-io/querator/proto"
	"github.com/spf13/cobra"
)

var leaseCommand = &cobra.Command{
	Use:   "lease [flags] <queue-name>",
	Short: "Lease items from a queue",
	Long: `Lease items from a queue. Outputs leased items to stdout as JSON.
The command exits after successfully leasing items.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return runLease(args[0])
	},
}

var leaseFlags struct {
	clientId  string
	batchSize int32
	timeout   string
}

func init() {
	leaseCommand.Flags().StringVarP(&leaseFlags.clientId, "client-id", "c",
		random.Alpha("id-", 10), "Client identifier (required)")
	leaseCommand.Flags().Int32VarP(&leaseFlags.batchSize, "batch-size", "b",
		1, "Number of items to lease")
	leaseCommand.Flags().StringVar(&leaseFlags.timeout, "timeout",
		"30s", "Request timeout")
}

func runLease(queueName string) error {
	client, err := createClient()
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	timeout, err := time.ParseDuration(leaseFlags.timeout)
	if err != nil {
		return fmt.Errorf("invalid timeout format: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req := &proto.QueueLeaseRequest{
		QueueName:      queueName,
		BatchSize:      leaseFlags.batchSize,
		ClientId:       leaseFlags.clientId,
		RequestTimeout: leaseFlags.timeout,
	}

	var resp proto.QueueLeaseResponse
	if err := client.QueueLease(ctx, req, &resp); err != nil {
		return fmt.Errorf("failed to lease items: %w", err)
	}

	// Convert to JSON for output
	output := struct {
		QueueName string                  `json:"queue_name"`
		Partition int32                   `json:"partition"`
		Items     []*proto.QueueLeaseItem `json:"items"`
	}{
		QueueName: resp.QueueName,
		Partition: resp.Partition,
		Items:     resp.Items,
	}

	jsonBytes, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal response: %w", err)
	}

	fmt.Println(string(jsonBytes))

	fmt.Fprintf(os.Stderr, "Successfully leased %d item(s) from queue '%s'\n", len(resp.Items), queueName)
	return nil
}
