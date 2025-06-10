package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/proto"
	"github.com/spf13/cobra"
)

var listCommand = &cobra.Command{
	Use:   "list [flags]",
	Short: "List queues",
	Long: `List all queues with pagination support.
Outputs queue information as JSON.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return RunList(flags)
	},
}

func init() {
	listCommand.Flags().Int32Var(&flags.Limit, "limit",
		100, "Maximum results to return")
	listCommand.Flags().StringVar(&flags.Pivot, "pivot",
		"", "Pagination pivot")
}

func RunList(flags FlagParams) error {
	client, err := querator.NewClient(querator.ClientConfig{Endpoint: flags.Endpoint})
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var resp proto.QueuesListResponse
	opts := &querator.ListOptions{
		Limit: int(flags.Limit),
		Pivot: flags.Pivot,
	}

	if err := client.QueuesList(ctx, &resp, opts); err != nil {
		return fmt.Errorf("failed to list queues: %w", err)
	}

	// Convert to JSON for output
	output := struct {
		Queues []*proto.QueueInfo `json:"queues"`
		Count  int                `json:"count"`
	}{
		Queues: resp.Items,
		Count:  len(resp.Items),
	}

	jsonBytes, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal response: %w", err)
	}

	// TODO: Support table output for queue listing

	fmt.Println(string(jsonBytes))

	fmt.Fprintf(os.Stderr, "Found %d queue(s)\n", len(resp.Items))
	return nil
}
