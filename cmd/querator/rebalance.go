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

var rebalanceCommand = &cobra.Command{
	Use:   "rebalance",
	Short: "Give every queue the partitions its requested count and the saved affinities call for",
	Long: `Give every queue the partitions its requested partition count and its storage backends'
saved affinities call for. A changed count or affinity takes effect only here.
A partition a backend should no longer hold becomes draining, and takes no new items while
its remaining items are leased and completed. New partitions are created on the backends
that hold too few. Run 'querator backends list' or 'querator info <queue-name>' first to see what a rebalance would do.
If a rebalance stops part way, run it again to finish. Needs the queue.rebalance permission.
Outputs JSON with the number of queues changed, partitions created and partitions this run
marked draining, and waiting_queues, the queues with one partition that wait on a draining
partition whose storage backend is missing from the config file.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		return RunRebalance(flags)
	},
}

func RunRebalance(flags FlagParams) error {
	client, err := querator.NewClient(querator.ClientConfig{Endpoint: flags.Endpoint})
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	var resp proto.QueuesRebalanceResponse
	if err := client.QueuesRebalance(ctx, &proto.QueuesRebalanceRequest{}, &resp); err != nil {
		return fmt.Errorf("failed to rebalance queues: %w", err)
	}

	out, err := protojson.MarshalOptions{Multiline: true, Indent: "  ", EmitUnpopulated: true}.Marshal(&resp)
	if err != nil {
		return fmt.Errorf("failed to marshal response: %w", err)
	}
	fmt.Println(string(out))
	return nil
}
