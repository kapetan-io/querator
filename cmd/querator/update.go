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
Only the flags provided will be updated, others remain unchanged.
Pass --dead-queue "" or --reference "" to remove them, and --max-attempts 0 for unlimited attempts.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		flags.ClearDeadQueue = cmd.Flags().Changed("dead-queue") && flags.DeadQueueUpdate == ""
		flags.ClearReference = cmd.Flags().Changed("reference") && flags.ReferenceUpdate == ""
		flags.UnlimitedAttempts = cmd.Flags().Changed("max-attempts") && flags.MaxAttemptsUpdate == 0
		return RunUpdate(flags, args[0])
	},
}

func init() {
	UpdateCommand.Flags().StringVar(&flags.LeaseTimeoutUpdate, "lease-timeout",
		"", "Lease timeout duration (e.g., '5m', '1h')")
	UpdateCommand.Flags().StringVar(&flags.ExpireTimeoutUpdate, "expire-timeout",
		"", "Item expiration timeout (e.g., '24h', '168h')")
	UpdateCommand.Flags().Int32Var(&flags.MaxAttemptsUpdate, "max-attempts",
		0, "Maximum retry attempts (0 for unlimited)")
	UpdateCommand.Flags().StringVar(&flags.DeadQueueUpdate, "dead-queue",
		"", "Dead letter queue name (\"\" to remove)")
	UpdateCommand.Flags().StringVar(&flags.ReferenceUpdate, "reference",
		"", "Queue reference/owner (\"\" to remove)")
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

	var currentQueue proto.QueueInfo
	if err := client.QueuesInfo(ctx, &proto.QueuesInfoRequest{QueueName: queueName}, &currentQueue); err != nil {
		return fmt.Errorf("failed to get current queue info: %w", err)
	}

	req := &proto.QueuesUpdateRequest{QueueName: currentQueue.QueueName}

	leaseTimeoutSet := flags.LeaseTimeoutUpdate != ""
	expireTimeoutSet := flags.ExpireTimeoutUpdate != ""
	maxAttemptsSet := flags.MaxAttemptsUpdate != 0 || flags.UnlimitedAttempts
	deadQueueSet := flags.DeadQueueUpdate != "" || flags.ClearDeadQueue
	referenceSet := flags.ReferenceUpdate != "" || flags.ClearReference
	partitionsSet := flags.PartitionsUpdate != 0

	if leaseTimeoutSet {
		req.LeaseTimeout = flags.LeaseTimeoutUpdate
	}

	if expireTimeoutSet {
		req.ExpireTimeout = flags.ExpireTimeoutUpdate
	}
	if maxAttemptsSet {
		req.MaxAttempts = new(flags.MaxAttemptsUpdate)
	}
	if deadQueueSet {
		req.DeadQueue = new(flags.DeadQueueUpdate)
	}
	if referenceSet {
		req.Reference = new(flags.ReferenceUpdate)
	}
	if partitionsSet {
		req.RequestedPartitions = flags.PartitionsUpdate
	}

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
