package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/kapetan-io/querator"
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
		return RunCreate(flags, args[0])
	},
}

func init() {
	createCommand.Flags().StringVar(&flags.LeaseTimeoutCreate, "lease-timeout",
		"1m", "Lease timeout duration (e.g., '5m', '1h')")
	createCommand.Flags().StringVar(&flags.ExpireTimeout, "expire-timeout",
		"", "Item expiration timeout (defaults to 60x the lease-timeout if not provided)")
	createCommand.Flags().Int32Var(&flags.MaxAttempts, "max-attempts",
		0, "Maximum retry attempts (0 for unlimited)")
	createCommand.Flags().StringVar(&flags.DeadQueue, "dead-queue",
		"", "Dead letter queue name")
	createCommand.Flags().StringVar(&flags.ReferenceCreate, "reference",
		"", "Queue reference/owner")
	createCommand.Flags().Int32Var(&flags.Partitions, "partitions",
		1, "Number of requested partitions (defaults to 1)")
}

func RunCreate(flags FlagParams, queueName string) error {
	client, err := querator.NewClient(querator.ClientConfig{Endpoint: flags.Endpoint})
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req := &proto.QueueInfo{
		QueueName: queueName,
	}

	// Set lease timeout (always has a default)
	req.LeaseTimeout = flags.LeaseTimeoutCreate

	// Set expire timeout with smart calculation
	if flags.ExpireTimeout != "" {
		req.ExpireTimeout = flags.ExpireTimeout
	} else {
		// Calculate expire timeout as 60x lease timeout
		leaseTimeout, err := time.ParseDuration(flags.LeaseTimeoutCreate)
		if err != nil {
			return fmt.Errorf("invalid lease-timeout format: %w", err)
		}
		expireTimeout := leaseTimeout * 60
		req.ExpireTimeout = expireTimeout.String()
	}

	// Set partitions (always has a default)
	req.RequestedPartitions = flags.Partitions

	// Set optional fields if provided
	if flags.MaxAttempts > 0 {
		req.MaxAttempts = flags.MaxAttempts
	}
	if flags.DeadQueue != "" {
		req.DeadQueue = flags.DeadQueue
	}
	if flags.ReferenceCreate != "" {
		req.Reference = flags.ReferenceCreate
	}

	if err := client.QueuesCreate(ctx, req); err != nil {
		return fmt.Errorf("failed to create queue: %w", err)
	}

	fmt.Fprintf(os.Stderr, "Successfully created queue '%s' (partitions: %d, lease-timeout: %s, expire-timeout: %s)\n",
		queueName, req.RequestedPartitions, req.LeaseTimeout, req.ExpireTimeout)
	return nil
}
