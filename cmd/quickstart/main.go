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

// Command quickstart validates the Querator quick start documentation
// by starting docker-compose, checking health, and performing basic queue operations.
//
// Usage:
//
//	go run quickstart.go [flags]
//
// Flags:
//
//	--endpoint string   Querator endpoint (default "http://localhost:2319")
//	--skip-docker       Skip docker compose up/down (use existing instance)
//	--cleanup           Run docker compose down after tests
//	--verbose           Print detailed output
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/kapetan-io/querator"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/querator/transport"
)

const (
	defaultEndpoint = "http://localhost:2319"
	queueName       = "quickstart-test"
	numItems        = 3
	healthRetry     = 1 * time.Second
)

var (
	endpoint      = flag.String("endpoint", defaultEndpoint, "Querator endpoint")
	skipDocker    = flag.Bool("skip-docker", false, "Skip docker compose up/down (use existing instance)")
	cleanup       = flag.Bool("cleanup", false, "Run docker compose down after tests")
	verbose       = flag.Bool("verbose", false, "Print detailed output")
	healthTimeout = flag.Duration("health-timeout", 60*time.Second, "Timeout for health check")
)

func main() {
	flag.Parse()

	fmt.Println("Querator Quick Start Validation")
	fmt.Println("================================")

	if err := run(); err != nil {
		fmt.Printf("\n[✗] Error: %v\n", err)
		if !*skipDocker {
			showDockerLogs()
			if *cleanup {
				fmt.Println("\n[~] Attempting cleanup...")
				_ = stopDocker()
			}
		}
		os.Exit(1)
	}

	if *cleanup && !*skipDocker {
		if err := stopDocker(); err != nil {
			fmt.Printf("\n[✗] Cleanup failed: %v\n", err)
			os.Exit(1)
		}
	}

	fmt.Println("\nAll checks passed!")
}

func run() error {
	if err := checkPrerequisites(); err != nil {
		return err
	}
	printCheck("Prerequisites check passed")

	if !*skipDocker {
		if err := startDocker(); err != nil {
			return err
		}
		printCheck("Docker compose started")
	}

	if err := waitForHealth(*endpoint, *healthTimeout); err != nil {
		return err
	}
	printCheck("Health check passed (status: pass)")

	client, err := querator.NewClient(querator.ClientConfig{
		Endpoint: *endpoint,
	})
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	if err := runQueueWorkflow(client); err != nil {
		return err
	}

	return nil
}

func checkPrerequisites() error {
	if *skipDocker {
		return nil
	}

	if _, err := exec.LookPath("docker"); err != nil {
		return fmt.Errorf("docker command not found: %w", err)
	}

	cmd := exec.Command("docker", "compose", "version")
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker compose not available: %w", err)
	}

	return nil
}

func startDocker() error {
	verboseLog("Starting docker compose...")

	cmd := exec.Command("docker", "compose", "up", "-d")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to start docker compose: %w", err)
	}

	return nil
}

func stopDocker() error {
	verboseLog("Stopping docker compose...")

	cmd := exec.Command("docker", "compose", "down")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to stop docker compose: %w", err)
	}

	printCheck("Docker compose stopped")
	return nil
}

func showDockerLogs() {
	fmt.Println("\n[~] === Docker Compose Logs ===")
	cmd := exec.Command("docker", "compose", "logs", "--tail=100")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	_ = cmd.Run()

	fmt.Println("\n[~] === Docker PS ===")
	cmd = exec.Command("docker", "ps", "-a")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	_ = cmd.Run()
}

func waitForHealth(endpoint string, timeout time.Duration) error {
	verboseLog("Waiting for health endpoint...")

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	healthURL := endpoint + "/health"
	ticker := time.NewTicker(healthRetry)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("health check timed out after %v", timeout)
		case <-ticker.C:
			resp, err := http.Get(healthURL)
			if err != nil {
				verboseLog("Health check failed, retrying: %v", err)
				continue
			}

			var health transport.HealthResponse
			if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
				_ = resp.Body.Close()
				verboseLog("Failed to decode health response, retrying: %v", err)
				continue
			}
			_ = resp.Body.Close()

			if health.Status == transport.HealthStatusPass {
				return nil
			}

			// Print health check details when status is fail
			if health.Status == transport.HealthStatusFail {
				verboseLog("Health status: %s", health.Status)
				for name, checks := range health.Checks {
					for _, check := range checks {
						if check.Output != "" {
							verboseLog("  %s: %s - %s", name, check.Status, check.Output)
						} else {
							verboseLog("  %s: %s", name, check.Status)
						}
					}
				}
			} else {
				verboseLog("Health status not ready: %s", health.Status)
			}
		}
	}
}

func runQueueWorkflow(client *querator.Client) error {
	ctx := context.Background()

	err := client.QueuesCreate(ctx, &pb.QueueInfo{
		QueueName:           queueName,
		ExpireTimeout:       "24h0m0s",
		LeaseTimeout:        "1m0s",
		RequestedPartitions: 1,
	})
	if err != nil {
		// Queue may already exist from previous run - that's OK
		if strings.Contains(err.Error(), "already exists") {
			printCheck(fmt.Sprintf("Queue %q already exists (reusing)", queueName))
		} else {
			return fmt.Errorf("failed to create queue: %w", err)
		}
	} else {
		printCheck(fmt.Sprintf("Queue %q created", queueName))
	}

	items := make([]*pb.QueueProduceItem, numItems)
	for i := 0; i < numItems; i++ {
		items[i] = &pb.QueueProduceItem{
			Encoding:  "application/json",
			Kind:      "test-item",
			Bytes:     []byte(fmt.Sprintf(`{"test": %d}`, i)),
			Reference: fmt.Sprintf("ref-%d", i),
		}
	}

	if err := client.QueueProduce(ctx, &pb.QueueProduceRequest{
		QueueName:      queueName,
		Items:          items,
		RequestTimeout: "10s",
	}); err != nil {
		return fmt.Errorf("failed to produce items: %w", err)
	}
	printCheck(fmt.Sprintf("Produced %d items", numItems))

	var leaseResp pb.QueueLeaseResponse
	if err := client.QueueLease(ctx, &pb.QueueLeaseRequest{
		QueueName:      queueName,
		ClientId:       "quickstart-client",
		BatchSize:      int32(numItems),
		RequestTimeout: "10s",
	}, &leaseResp); err != nil {
		return fmt.Errorf("failed to lease items: %w", err)
	}

	if len(leaseResp.Items) != numItems {
		return fmt.Errorf("expected %d leased items, got %d", numItems, len(leaseResp.Items))
	}
	printCheck(fmt.Sprintf("Leased %d items", len(leaseResp.Items)))

	ids := make([]string, len(leaseResp.Items))
	for i, item := range leaseResp.Items {
		ids[i] = item.Id
	}

	if err := client.QueueComplete(ctx, &pb.QueueCompleteRequest{
		QueueName:      queueName,
		Ids:            ids,
		RequestTimeout: "10s",
	}); err != nil {
		return fmt.Errorf("failed to complete items: %w", err)
	}
	printCheck(fmt.Sprintf("Completed %d items", len(ids)))

	return nil
}

func printCheck(msg string) {
	fmt.Printf("[✓] %s\n", msg)
}

func verboseLog(format string, args ...interface{}) {
	if *verbose {
		fmt.Printf("[~] "+format+"\n", args...)
	}
}
