package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/duh-rpc/duh-go"

	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/querator/transport/reply"
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
that hold too few. Run 'querator backends list' first to see what a rebalance would do.
If a rebalance stops part way, run it again to finish. Needs the queue.rebalance permission.
Outputs JSON with the number of queues changed, partitions created and partitions this run
marked draining. A rebalance that stops part way outputs the same counts for what it saved
before it stopped, plus queue_name, the queue it stopped on, then exits non-zero.`,
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
		if report, ok := stoppedRebalanceReport(err); ok {
			fmt.Println(report)
		}
		return fmt.Errorf("failed to rebalance queues: %w", err)
	}

	out, err := protojson.MarshalOptions{Multiline: true, Indent: "  ", EmitUnpopulated: true}.Marshal(&resp)
	if err != nil {
		return fmt.Errorf("failed to marshal response: %w", err)
	}
	fmt.Println(string(out))
	return nil
}

func stoppedRebalanceReport(err error) (string, bool) {
	var stopped duh.Error
	if !errors.As(err, &stopped) {
		return "", false
	}
	saved, queueName, ok := reply.ParseRebalanceStopped(stopped.Details())
	if !ok {
		return "", false
	}
	out, _ := json.MarshalIndent(struct {
		QueuesChanged      int    `json:"queues_changed"`
		PartitionsCreated  int    `json:"partitions_created"`
		PartitionsDraining int    `json:"partitions_draining"`
		QueueName          string `json:"queue_name"`
	}{
		QueuesChanged:      saved.QueuesChanged,
		PartitionsCreated:  saved.PartitionsCreated,
		PartitionsDraining: saved.PartitionsDraining,
		QueueName:          queueName,
	}, "", "  ")
	return string(out), true
}
