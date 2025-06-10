package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/proto"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var produceCommand = &cobra.Command{
	Use:   "produce [flags] <queue-name>",
	Short: "Produce items to a queue",
	Long: `Produce items to a queue. By default, reads payload from stdin.
Use --payload flag to provide payload directly.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return RunProduce(flags, args[0])
	},
}

func init() {
	produceCommand.Flags().StringVarP(&flags.Payload, "payload", "p",
		"", "Item payload (if not provided, reads from stdin)")
	produceCommand.Flags().StringVar(&flags.Encoding, "encoding",
		"", "Payload encoding")
	produceCommand.Flags().StringVar(&flags.Kind, "kind",
		"", "Item kind/type")
	produceCommand.Flags().StringVar(&flags.Reference, "reference",
		"", "Reference identifier")
	produceCommand.Flags().StringVar(&flags.EnqueueAt, "enqueue-at",
		"", "Scheduled enqueue time (RFC3339 format)")
	produceCommand.Flags().StringVar(&flags.Timeout, "timeout",
		"30s", "Request timeout")
}

func RunProduce(flags FlagParams, queueName string) error {
	client, err := querator.NewClient(querator.ClientConfig{Endpoint: flags.Endpoint})
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	timeout, err := time.ParseDuration(flags.Timeout)
	if err != nil {
		return fmt.Errorf("invalid timeout format: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Get payload from stdin or flag
	var payload []byte
	if flags.Payload != "" {
		payload = []byte(flags.Payload)
	} else {
		payload, err = io.ReadAll(os.Stdin)
		if err != nil {
			return fmt.Errorf("failed to read from stdin: %w", err)
		}
	}

	// Parse enqueue time if provided
	var enqueueAt *timestamppb.Timestamp
	if flags.EnqueueAt != "" {
		t, err := time.Parse(time.RFC3339, flags.EnqueueAt)
		if err != nil {
			return fmt.Errorf("invalid enqueue-at time format (use RFC3339): %w", err)
		}
		enqueueAt = timestamppb.New(t)
	}

	req := &proto.QueueProduceRequest{
		QueueName:      queueName,
		RequestTimeout: flags.Timeout,
		Items: []*proto.QueueProduceItem{
			{
				Encoding:  flags.Encoding,
				Kind:      flags.Kind,
				Reference: flags.Reference,
				EnqueueAt: enqueueAt,
				Bytes:     payload,
			},
		},
	}

	if err := client.QueueProduce(ctx, req); err != nil {
		return fmt.Errorf("failed to produce item: %w", err)
	}
	if flags.Payload != "" {
		fmt.Printf("Successfully produced item with payload of %d bytes\n", len(payload))
	}
	return nil
}
