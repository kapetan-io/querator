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

var UpdateCommand = &cobra.Command{
	Use:   "update [flags] <queue-name>",
	Short: "Update an existing queue",
	Long: `Update an existing queue configuration.
Only the flags provided will be updated, others remain unchanged.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return RunUpdate(flags, args[0])
	},
}

func init() {
	UpdateCommand.Flags().StringVar(&flags.LeaseTimeoutUpdate, "lease-timeout",
		"", "Lease timeout duration (e.g., '5m', '1h')")
	UpdateCommand.Flags().StringVar(&flags.ExpireTimeoutUpdate, "expire-timeout",
		"", "Item expiration timeout (e.g., '24h', '7d')")
	UpdateCommand.Flags().Int32Var(&flags.MaxAttemptsUpdate, "max-attempts",
		0, "Maximum retry attempts (0 for unlimited)")
	UpdateCommand.Flags().StringVar(&flags.DeadQueueUpdate, "dead-queue",
		"", "Dead letter queue name")
	UpdateCommand.Flags().StringVar(&flags.ReferenceUpdate, "reference",
		"", "Queue reference/owner")
	UpdateCommand.Flags().Int32Var(&flags.PartitionsUpdate, "partitions",
		0, "Number of requested partitions")
}

func RunUpdate(flags FlagParams, queueName string) error {
	client, err := querator.NewClient(querator.ClientConfig{Endpoint: flags.Endpoint})
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

	// Update only the fields that have non-default values
	leaseTimeoutSet := flags.LeaseTimeoutUpdate != ""
	expireTimeoutSet := flags.ExpireTimeoutUpdate != ""
	maxAttemptsSet := flags.MaxAttemptsUpdate != 0
	deadQueueSet := flags.DeadQueueUpdate != ""
	referenceSet := flags.ReferenceUpdate != ""
	partitionsSet := flags.PartitionsUpdate != 0

	if leaseTimeoutSet {
		req.LeaseTimeout = flags.LeaseTimeoutUpdate

		// If lease-timeout is updated but expire-timeout is not explicitly set,
		// recalculate expire-timeout as 60x the new lease-timeout
		if !expireTimeoutSet {
			leaseTimeout, err := time.ParseDuration(flags.LeaseTimeoutUpdate)
			if err != nil {
				return fmt.Errorf("invalid lease-timeout format: %w", err)
			}
			expireTimeout := leaseTimeout * 60
			req.ExpireTimeout = expireTimeout.String()
		}
	}

	if expireTimeoutSet {
		req.ExpireTimeout = flags.ExpireTimeoutUpdate
	}
	if maxAttemptsSet {
		req.MaxAttempts = flags.MaxAttemptsUpdate
	}
	if deadQueueSet {
		req.DeadQueue = flags.DeadQueueUpdate
	}
	if referenceSet {
		req.Reference = flags.ReferenceUpdate
	}
	if partitionsSet {
		req.RequestedPartitions = flags.PartitionsUpdate
	}

	// Check if any flags were actually set
	if !leaseTimeoutSet && !expireTimeoutSet && !maxAttemptsSet &&
		!deadQueueSet && !referenceSet && !partitionsSet {
		return fmt.Errorf("no update flags provided (use --help to see available options)")
	}

	if err := client.QueuesUpdate(ctx, req); err != nil {
		return fmt.Errorf("failed to update queue: %w", err)
	}

	fmt.Fprintf(os.Stderr, "Successfully updated queue '%s'\n", queueName)
	return nil
}
