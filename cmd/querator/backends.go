package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/proto"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
)

var backendsCommand = &cobra.Command{
	Use:   "backends",
	Short: "List storage backends and change their affinity",
}

var backendsListCommand = &cobra.Command{
	Use:   "list",
	Short: "List storage backends",
	Long: `List every storage backend with its saved affinity, the partitions on it with their
item counts, and what a rebalance would do to it.
Outputs JSON.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		return RunBackendsList(flags)
	},
}

var backendsUpdateCommand = &cobra.Command{
	Use:   "update [flags] <backend>",
	Short: "Change a storage backend's affinity",
	Long: `Change the saved affinity of a storage backend. The saved affinity decides where
new queues place their partitions. An affinity of 0 means no new partition goes to the backend.
Example: querator backends update postgres-1 --affinity 0`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		flags.AffinitySet = cmd.Flags().Changed("affinity")
		return RunBackendsUpdate(flags, args[0])
	},
}

func init() {
	backendsUpdateCommand.Flags().Float64Var(&flags.Affinity, "affinity",
		0, "The backend's affinity, 0 or more")
	backendsCommand.AddCommand(backendsListCommand)
	backendsCommand.AddCommand(backendsUpdateCommand)
}

func RunBackendsList(flags FlagParams) error {
	client, err := querator.NewClient(querator.ClientConfig{Endpoint: flags.Endpoint})
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var resp proto.StorageBackendsListResponse
	if err := client.StorageBackendsList(ctx, &resp); err != nil {
		return fmt.Errorf("failed to list storage backends: %w", err)
	}

	out, err := protojson.MarshalOptions{Multiline: true, Indent: "  ", EmitUnpopulated: true}.Marshal(&resp)
	if err != nil {
		return fmt.Errorf("failed to marshal response: %w", err)
	}
	fmt.Println(string(out))
	return nil
}

func RunBackendsUpdate(flags FlagParams, backend string) error {
	if !flags.AffinitySet {
		return fmt.Errorf("no update flags provided (use --help to see available options)")
	}

	client, err := querator.NewClient(querator.ClientConfig{Endpoint: flags.Endpoint})
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := client.StorageBackendsUpdate(ctx, &proto.StorageBackendsUpdateRequest{
		Backend:  backend,
		Affinity: new(flags.Affinity),
	}); err != nil {
		return fmt.Errorf("failed to update storage backend: %w", err)
	}

	fmt.Fprintf(os.Stderr, "Successfully updated storage backend '%s'\n", backend)
	return nil
}
