package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/kapetan-io/querator/proto"
	"github.com/spf13/cobra"
)

var updateCommand = &cobra.Command{
	Use:   "update [flags] <queue-name>",
	Short: "Update an existing queue",
	Long: `Update an existing queue configuration.
Only the flags provided will be updated, others remain unchanged.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return runUpdate(cmd, args[0])
	},
}

var updateFlags struct {
	leaseTimeout  string
	expireTimeout string
	maxAttempts   int32
	deadQueue     string
	reference     string
	partitions    int32
	// Track which flags were actually set
	leaseTimeoutSet  bool
	expireTimeoutSet bool
	maxAttemptsSet   bool
	deadQueueSet     bool
	referenceSet     bool
	partitionsSet    bool
}

func init() {
	updateCommand.Flags().StringVar(&updateFlags.leaseTimeout, "lease-timeout",
		"", "Lease timeout duration (e.g., '5m', '1h')")
	updateCommand.Flags().StringVar(&updateFlags.expireTimeout, "expire-timeout",
		"", "Item expiration timeout (e.g., '24h', '7d')")
	updateCommand.Flags().Int32Var(&updateFlags.maxAttempts, "max-attempts",
		0, "Maximum retry attempts (0 for unlimited)")
	updateCommand.Flags().StringVar(&updateFlags.deadQueue, "dead-queue",
		"", "Dead letter queue name")
	updateCommand.Flags().StringVar(&updateFlags.reference, "reference",
		"", "Queue reference/owner")
	updateCommand.Flags().Int32Var(&updateFlags.partitions, "partitions",
		0, "Number of requested partitions")
}

func runUpdate(cmd *cobra.Command, queueName string) error {
	client, err := createClient()
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// First get the current queue info
	var currentQueue proto.QueueInfo
	infoReq := &proto.QueuesInfoRequest{
		QueueName: queueName,
	}

	if err := client.QueuesInfo(ctx, infoReq, &currentQueue); err != nil {
		return fmt.Errorf("failed to get current queue info: %w", err)
	}

	// Start with current values
	req := &proto.QueueInfo{
		QueueName:           currentQueue.QueueName,
		LeaseTimeout:        currentQueue.LeaseTimeout,
		ExpireTimeout:       currentQueue.ExpireTimeout,
		MaxAttempts:         currentQueue.MaxAttempts,
		DeadQueue:           currentQueue.DeadQueue,
		Reference:           currentQueue.Reference,
		RequestedPartitions: currentQueue.RequestedPartitions,
	}

	// Update only the fields that were explicitly set
	updateFlags.leaseTimeoutSet = cmd.Flags().Changed("lease-timeout")
	updateFlags.expireTimeoutSet = cmd.Flags().Changed("expire-timeout")
	updateFlags.maxAttemptsSet = cmd.Flags().Changed("max-attempts")
	updateFlags.deadQueueSet = cmd.Flags().Changed("dead-queue")
	updateFlags.referenceSet = cmd.Flags().Changed("reference")
	updateFlags.partitionsSet = cmd.Flags().Changed("partitions")

	if updateFlags.leaseTimeoutSet {
		req.LeaseTimeout = updateFlags.leaseTimeout
		
		// If lease-timeout is updated but expire-timeout is not explicitly set,
		// recalculate expire-timeout as 60x the new lease-timeout
		if !updateFlags.expireTimeoutSet {
			leaseTimeout, err := time.ParseDuration(updateFlags.leaseTimeout)
			if err != nil {
				return fmt.Errorf("invalid lease-timeout format: %w", err)
			}
			expireTimeout := leaseTimeout * 60
			req.ExpireTimeout = expireTimeout.String()
		}
	}
	if updateFlags.expireTimeoutSet {
		req.ExpireTimeout = updateFlags.expireTimeout
	}
	if updateFlags.maxAttemptsSet {
		req.MaxAttempts = updateFlags.maxAttempts
	}
	if updateFlags.deadQueueSet {
		req.DeadQueue = updateFlags.deadQueue
	}
	if updateFlags.referenceSet {
		req.Reference = updateFlags.reference
	}
	if updateFlags.partitionsSet {
		req.RequestedPartitions = updateFlags.partitions
	}

	// Check if any flags were actually set
	if !updateFlags.leaseTimeoutSet && !updateFlags.expireTimeoutSet && !updateFlags.maxAttemptsSet &&
		!updateFlags.deadQueueSet && !updateFlags.referenceSet && !updateFlags.partitionsSet {
		return fmt.Errorf("no update flags provided (use --help to see available options)")
	}

	if err := client.QueuesUpdate(ctx, req); err != nil {
		return fmt.Errorf("failed to update queue: %w", err)
	}

	fmt.Fprintf(os.Stderr, "Successfully updated queue '%s'\n", queueName)
	return nil
}
