package main

import (
	"context"
	"fmt"
	"time"

	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/proto"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
)

var infoCommand = &cobra.Command{
	Use:   "info [flags] <queue-name>",
	Short: "Show a queue's saved settings next to the partitions it is running",
	Long: `Show one queue's saved settings next to the partitions it is running.
'requested_partitions' is the saved count and 'partitions' is what is running. A changed
requested count or a changed storage backend affinity takes effect only on 'querator rebalance'.
'pending' lists the partitions a rebalance would create and the ones it would mark draining,
and is left out when a rebalance would change nothing for the queue.
'rebalance_refused' gives the reason when a rebalance would be refused.
Outputs the queue as JSON. Needs the queue.stats permission.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return RunInfo(flags, args[0])
	},
}

func RunInfo(flags FlagParams, queueName string) error {
	client, err := querator.NewClient(querator.ClientConfig{Endpoint: flags.Endpoint})
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var resp proto.QueueInfo
	if err := client.QueuesInfo(ctx, &proto.QueuesInfoRequest{QueueName: queueName}, &resp); err != nil {
		return fmt.Errorf("failed to get queue info: %w", err)
	}

	out, err := protojson.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(&resp)
	if err != nil {
		return fmt.Errorf("failed to marshal response: %w", err)
	}
	fmt.Println(string(out))
	return nil
}
