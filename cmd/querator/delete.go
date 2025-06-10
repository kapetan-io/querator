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

var deleteCmd = &cobra.Command{
	Use:   "delete [flags] <queue-name>",
	Short: "Delete a queue",
	Long: `Delete a queue. Use --force to delete queues with existing items.
This operation is irreversible.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return RunDelete(flags, args[0])
	},
}

func init() {
	deleteCmd.Flags().BoolVar(&flags.Force, "force",
		false, "Force deletion with items present")
}

func RunDelete(flags FlagParams, queueName string) error {
	client, err := querator.NewClient(querator.ClientConfig{Endpoint: flags.Endpoint})
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req := &proto.QueuesDeleteRequest{
		QueueName: queueName,
		Force:     flags.Force,
	}

	if err := client.QueuesDelete(ctx, req); err != nil {
		return fmt.Errorf("failed to delete queue: %w", err)
	}

	fmt.Fprintf(os.Stderr, "Successfully deleted queue '%s'\n", queueName)
	return nil
}
