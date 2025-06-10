package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/kapetan-io/querator/proto"
	"github.com/spf13/cobra"
)

var completeCommand = &cobra.Command{
	Use:   "complete [flags] <queue-name> <partition> <id>...",
	Short: "Mark items as complete",
	Long: `Mark leased items as complete. Provide item IDs as arguments or use --file to read from file.
Partition must be specified as it's required for completion.`,
	Args: cobra.MinimumNArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		queueName := args[0]
		partition, err := strconv.Atoi(args[1])
		if err != nil {
			return fmt.Errorf("invalid partition number: %w", err)
		}

		var ids []string
		if len(args) > 2 {
			ids = args[2:]
		}

		return runComplete(queueName, int32(partition), ids)
	},
}

var completeFlags struct {
	file    string
	timeout string
}

func init() {
	completeCommand.Flags().StringVar(&completeFlags.file, "file",
		"", "Read IDs from file (one per line)")
	completeCommand.Flags().StringVar(&completeFlags.timeout, "timeout",
		"30s", "Request timeout")
}

func runComplete(queueName string, partition int32, cmdLineIds []string) error {
	client, err := createClient()
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	timeout, err := time.ParseDuration(completeFlags.timeout)
	if err != nil {
		return fmt.Errorf("invalid timeout format: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var ids []string

	// Read IDs from file if specified
	if completeFlags.file != "" {
		file, err := os.Open(completeFlags.file)
		if err != nil {
			return fmt.Errorf("failed to open file: %w", err)
		}
		defer func() {
			_ = file.Close()
		}()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line != "" {
				ids = append(ids, line)
			}
		}
		if err := scanner.Err(); err != nil {
			return fmt.Errorf("failed to read file: %w", err)
		}
	} else {
		// Use command line IDs
		if len(cmdLineIds) == 0 {
			return fmt.Errorf("no item IDs provided (use command line arguments or --file)")
		}
		ids = cmdLineIds
	}

	if len(ids) == 0 {
		return fmt.Errorf("no item IDs to complete")
	}

	req := &proto.QueueCompleteRequest{
		QueueName:      queueName,
		Partition:      partition,
		RequestTimeout: completeFlags.timeout,
		Ids:            ids,
	}

	if err := client.QueueComplete(ctx, req); err != nil {
		return fmt.Errorf("failed to complete items: %w", err)
	}

	fmt.Fprintf(os.Stderr, "Successfully completed %d item(s) in queue '%s' partition %d\n",
		len(ids), queueName, partition)
	return nil
}
