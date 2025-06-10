/*
Copyright 2024 Derrick J. Wippler

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kapetan-io/querator/daemon"
	pb "github.com/kapetan-io/querator/proto"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create daemon with default config (localhost:2319, in-memory storage)
	d, err := daemon.NewDaemon(ctx, daemon.Config{})
	if err != nil {
		log.Fatalf("Failed to create daemon: %v", err)
	}

	// Setup graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutdown signal received, gracefully shutting down...")
		cancel()

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()

		if err := d.Shutdown(shutdownCtx); err != nil {
			log.Printf("Error during shutdown: %v", err)
		}
		os.Exit(0)
	}()

	// Get client for interacting with the embedded daemon
	client := d.MustClient()

	log.Printf("Querator daemon started on %s", d.Listener.Addr().String())

	// Create a queue
	queueName := "example-queue"
	err = client.QueuesCreate(ctx, &pb.QueueInfo{
		QueueName:     queueName,
		LeaseTimeout:  "5m",
		ExpireTimeout: "10m",
		MaxAttempts:   3,
		Reference:     "embedded-example",
	})
	if err != nil {
		log.Fatalf("Failed to create queue: %v", err)
	}
	log.Printf("Created queue: %s", queueName)

	// Produce some items to the queue
	err = client.QueueProduce(ctx, &pb.QueueProduceRequest{
		QueueName:      queueName,
		RequestTimeout: "30s",
		Items: []*pb.QueueProduceItem{
			{
				Kind:     "example-task",
				Encoding: "utf8",
				Utf8:     "Hello from embedded querator!",
			},
			{
				Kind:     "example-task",
				Encoding: "utf8",
				Utf8:     "Second example item",
			},
		},
	})
	if err != nil {
		log.Fatalf("Failed to produce items: %v", err)
	}
	log.Println("Produced 2 items to queue")

	// Lease items from the queue
	var leaseResp pb.QueueLeaseResponse
	err = client.QueueLease(ctx, &pb.QueueLeaseRequest{
		QueueName:      queueName,
		BatchSize:      10,
		ClientId:       "embedded-consumer",
		RequestTimeout: "30s",
	}, &leaseResp)
	if err != nil {
		log.Fatalf("Failed to lease items: %v", err)
	}

	log.Printf("Leased %d items from queue", len(leaseResp.Items))

	// Process the leased items
	var itemIds []string
	for _, item := range leaseResp.Items {
		log.Printf("Processing item %s: kind=%s, payload=%s",
			item.Id, item.Kind, string(item.Bytes))

		// Simulate some work
		time.Sleep(100 * time.Millisecond)

		itemIds = append(itemIds, item.Id)
	}

	// Complete the processed items
	if len(itemIds) > 0 {
		err = client.QueueComplete(ctx, &pb.QueueCompleteRequest{
			QueueName:      queueName,
			Partition:      leaseResp.Partition,
			RequestTimeout: "30s",
			Ids:            itemIds,
		})
		if err != nil {
			log.Fatalf("Failed to complete items: %v", err)
		}
		log.Printf("Completed %d items", len(itemIds))
	}

	log.Println("Example completed successfully. Press Ctrl+C to shutdown.")

	// Keep the program running until shutdown signal
	<-ctx.Done()
}
