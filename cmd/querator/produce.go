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
		return runProduce(args[0])
	},
}

var produceFlags struct {
	payload   string
	encoding  string
	kind      string
	reference string
	enqueueAt string
	timeout   string
}

func init() {
	produceCommand.Flags().StringVarP(&produceFlags.payload, "payload", "p",
		"", "Item payload (if not provided, reads from stdin)")
	produceCommand.Flags().StringVar(&produceFlags.encoding, "encoding",
		"", "Payload encoding")
	produceCommand.Flags().StringVar(&produceFlags.kind, "kind",
		"", "Item kind/type")
	produceCommand.Flags().StringVar(&produceFlags.reference, "reference",
		"", "Reference identifier")
	produceCommand.Flags().StringVar(&produceFlags.enqueueAt, "enqueue-at",
		"", "Scheduled enqueue time (RFC3339 format)")
	produceCommand.Flags().StringVar(&produceFlags.timeout, "timeout",
		"30s", "Request timeout")
}

func runProduce(queueName string) error {
	client, err := createClient()
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	timeout, err := time.ParseDuration(produceFlags.timeout)
	if err != nil {
		return fmt.Errorf("invalid timeout format: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Get payload from stdin or flag
	var payload []byte
	if produceFlags.payload != "" {
		payload = []byte(produceFlags.payload)
	} else {
		payload, err = io.ReadAll(os.Stdin)
		if err != nil {
			return fmt.Errorf("failed to read from stdin: %w", err)
		}
	}

	// Parse enqueue time if provided
	var enqueueAt *timestamppb.Timestamp
	if produceFlags.enqueueAt != "" {
		t, err := time.Parse(time.RFC3339, produceFlags.enqueueAt)
		if err != nil {
			return fmt.Errorf("invalid enqueue-at time format (use RFC3339): %w", err)
		}
		enqueueAt = timestamppb.New(t)
	}

	req := &proto.QueueProduceRequest{
		QueueName:      queueName,
		RequestTimeout: produceFlags.timeout,
		Items: []*proto.QueueProduceItem{
			{
				Encoding:  produceFlags.encoding,
				Kind:      produceFlags.kind,
				Reference: produceFlags.reference,
				EnqueueAt: enqueueAt,
				Bytes:     payload,
			},
		},
	}

	if err := client.QueueProduce(ctx, req); err != nil {
		return fmt.Errorf("failed to produce item: %w", err)
	}
	return nil
}

func createClient() (*querator.Client, error) {
	return querator.NewClient(querator.ClientConfig{
		Endpoint: flags.Endpoint,
	})
}
