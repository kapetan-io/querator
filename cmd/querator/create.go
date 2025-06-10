package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/kapetan-io/querator/proto"
	"github.com/spf13/cobra"
)

var createCommand = &cobra.Command{
	Use:   "create [flags] <queue-name>",
	Short: "Create a new queue",
	Long: `Create a new queue with specified configuration.
All flags are optional and will use server defaults if not provided.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return runCreate(args[0])
	},
}

var createFlags struct {
	leaseTimeout  string
	expireTimeout string
	maxAttempts   int32
	deadQueue     string
	reference     string
	partitions    int32
}

func init() {
	createCommand.Flags().StringVar(&createFlags.leaseTimeout, "lease-timeout",
		"1m", "Lease timeout duration (e.g., '5m', '1h')")
	createCommand.Flags().StringVar(&createFlags.expireTimeout, "expire-timeout",
		"", "Item expiration timeout (defaults to 60x the lease-timeout if not provided)")
	createCommand.Flags().Int32Var(&createFlags.maxAttempts, "max-attempts",
		0, "Maximum retry attempts (0 for unlimited)")
	createCommand.Flags().StringVar(&createFlags.deadQueue, "dead-queue",
		"", "Dead letter queue name")
	createCommand.Flags().StringVar(&createFlags.reference, "reference",
		"", "Queue reference/owner")
	createCommand.Flags().Int32Var(&createFlags.partitions, "partitions",
		1, "Number of requested partitions (defaults to 1)")
}

func runCreate(queueName string) error {
	client, err := createClient()
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req := &proto.QueueInfo{
		QueueName: queueName,
	}

	// Set lease timeout (always has a default)
	req.LeaseTimeout = createFlags.leaseTimeout

	// Set expire timeout with smart calculation
	if createFlags.expireTimeout != "" {
		req.ExpireTimeout = createFlags.expireTimeout
	} else {
		// Calculate expire timeout as 60x lease timeout
		leaseTimeout, err := time.ParseDuration(createFlags.leaseTimeout)
		if err != nil {
			return fmt.Errorf("invalid lease-timeout format: %w", err)
		}
		expireTimeout := leaseTimeout * 60
		req.ExpireTimeout = expireTimeout.String()
	}

	// Set partitions (always has a default)
	req.RequestedPartitions = createFlags.partitions

	// Set optional fields if provided
	if createFlags.maxAttempts > 0 {
		req.MaxAttempts = createFlags.maxAttempts
	}
	if createFlags.deadQueue != "" {
		req.DeadQueue = createFlags.deadQueue
	}
	if createFlags.reference != "" {
		req.Reference = createFlags.reference
	}

	if err := client.QueuesCreate(ctx, req); err != nil {
		return fmt.Errorf("failed to create queue: %w", err)
	}

	fmt.Fprintf(os.Stderr, "Successfully created queue '%s' (partitions: %d, lease-timeout: %s, expire-timeout: %s)\n",
		queueName, req.RequestedPartitions, req.LeaseTimeout, req.ExpireTimeout)
	return nil
}
