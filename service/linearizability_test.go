package service_test

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/duh-rpc/duh-go"
	"github.com/kapetan-io/querator/internal/store"
	pb "github.com/kapetan-io/querator/proto"
	svc "github.com/kapetan-io/querator/service"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestLinearizability(t *testing.T) {
	badgerdb := badgerTestSetup{Dir: t.TempDir()}

	for _, tc := range []struct {
		Setup    NewStorageFunc
		TearDown func()
		Name     string
	}{
		{
			Name: "InMemory",
			Setup: func() store.Config {
				return setupMemoryStorage(store.Config{})
			},
			TearDown: func() {},
		},
		{
			Name: "BadgerDB",
			Setup: func() store.Config {
				return badgerdb.Setup(store.BadgerConfig{})
			},
			TearDown: func() {
				badgerdb.Teardown()
			},
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			testLinearizability(t, tc.Setup, tc.TearDown)
		})
	}
}

func testLinearizability(t *testing.T, setup NewStorageFunc, tearDown func()) {
	defer goleak.VerifyNone(t, goleakOptions...)

	t.Run("ProduceLeaseComplete", func(t *testing.T) {
		d, c, ctx := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: setup()})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()

		queueName := random.String("lin-", 10)
		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			QueueName:           queueName,
			LeaseTimeout:        "5m0s",
			ExpireTimeout:       ExpireTimeout,
			RequestedPartitions: 1,
		})

		const (
			numItems     = 50
			numConsumers = 4
		)

		// Pre-produce items sequentially so their FIFO order is unambiguous.
		var history []porcupine.Operation
		for i := range numItems {
			id := fmt.Sprintf("item-%d", i)
			call := time.Now().UnixNano()

			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "5s",
				Items: []*pb.QueueProduceItem{
					{
						Reference: id,
						Bytes:     []byte(id),
					},
				},
			}))
			history = append(history, porcupine.Operation{
				ClientId: 0,
				Input:    linInput{op: opProduce, ids: []string{id}},
				Call:     call,
				Output:   linOutput{ok: true},
				Return:   time.Now().UnixNano(),
			})
		}

		// Concurrent consumers race to lease and complete items.
		var (
			mu      sync.Mutex
			opCount atomic.Int64
			wg      sync.WaitGroup
		)

		for ci := range numConsumers {
			clientID := ci + 1
			wg.Add(1)
			go func() {
				defer wg.Done()
				leaseClientID := random.String("lin-client-", 10)
				for opCount.Load() < int64(numItems) {
					inp := linInput{op: opLease}
					call := time.Now().UnixNano()

					var lease pb.QueueLeaseResponse
					err := c.QueueLease(ctx, &pb.QueueLeaseRequest{
						QueueName:      queueName,
						ClientId:       leaseClientID,
						RequestTimeout: "1s",
						BatchSize:      1,
					}, &lease)
					if err != nil {
						if ctx.Err() != nil {
							return
						}
						var de duh.Error
						if errors.As(err, &de) && de.Code() == duh.CodeRetryRequest {
							continue
						}
						t.Errorf("consumer %d lease: %v", ci, err)
						return
					}

					if len(lease.Items) == 0 {
						mu.Lock()
						history = append(history, porcupine.Operation{
							ClientId: clientID,
							Input:    inp,
							Call:     call,
							Output:   linOutput{ok: false},
							Return:   time.Now().UnixNano(),
						})
						mu.Unlock()
						continue
					}

					ids := make([]string, len(lease.Items))
					for i, item := range lease.Items {
						ids[i] = string(item.Bytes)
					}
					mu.Lock()
					history = append(history, porcupine.Operation{
						ClientId: clientID,
						Input:    inp,
						Call:     call,
						Output:   linOutput{ids: ids, ok: true},
						Return:   time.Now().UnixNano(),
					})
					mu.Unlock()

					completeIDs := make([]string, len(lease.Items))
					for i, item := range lease.Items {
						completeIDs[i] = item.Id
					}

					compInp := linInput{op: opComplete, ids: ids}
					compCall := time.Now().UnixNano()

					err = c.QueueComplete(ctx, &pb.QueueCompleteRequest{
						QueueName:      queueName,
						Partition:      lease.Partition,
						RequestTimeout: "5s",
						Ids:            completeIDs,
					})
					if err != nil {
						if ctx.Err() != nil {
							return
						}
						t.Errorf("consumer %d complete: %v", ci, err)
						return
					}

					mu.Lock()
					history = append(history, porcupine.Operation{
						ClientId: clientID,
						Input:    compInp,
						Call:     compCall,
						Output:   linOutput{ok: true},
						Return:   time.Now().UnixNano(),
					})
					mu.Unlock()
					opCount.Add(1)
				}
			}()
		}

		wg.Wait()

		t.Logf("recorded %d operations", len(history))

		result, info := porcupine.CheckOperationsVerbose(linearizabilityModel, history, 30*time.Second)
		t.Logf("porcupine result: %s", result)

		if result != porcupine.Ok {
			name := strings.ReplaceAll(t.Name(), "/", "_")
			path := fmt.Sprintf("testdata/linearization_%s.html", name)
			if err := os.MkdirAll("testdata", 0755); err == nil {
				if err := porcupine.VisualizePath(linearizabilityModel, info, path); err == nil {
					t.Logf("visualization written to %s", path)
				}
			}
		}

		require.Equal(t, porcupine.Ok, result, "history is not linearizable")
	})

	t.Run("ProduceLeaseRetryComplete", func(t *testing.T) {
		d, c, ctx := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: setup()})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()

		queueName := random.String("lin-", 10)
		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			QueueName:           queueName,
			LeaseTimeout:        "5m0s",
			ExpireTimeout:       ExpireTimeout,
			MaxAttempts:         0,
			RequestedPartitions: 1,
		})

		const (
			numItems     = 30
			numConsumers = 4
			retryPercent = 40
		)

		var history []porcupine.Operation
		for i := range numItems {
			id := fmt.Sprintf("item-%d", i)
			call := time.Now().UnixNano()

			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "5s",
				Items: []*pb.QueueProduceItem{
					{
						Reference: id,
						Bytes:     []byte(id),
					},
				},
			}))
			history = append(history, porcupine.Operation{
				ClientId: 0,
				Input:    linInput{op: opProduce, ids: []string{id}},
				Call:     call,
				Output:   linOutput{ok: true},
				Return:   time.Now().UnixNano(),
			})
		}

		var (
			mu        sync.Mutex
			completed atomic.Int64
			wg        sync.WaitGroup
		)

		for ci := range numConsumers {
			clientID := ci + 1
			wg.Add(1)
			go func() {
				defer wg.Done()
				rng := rand.New(rand.NewSource(int64(ci)))
				leaseClientID := random.String("lin-client-", 10)

				for completed.Load() < int64(numItems) {
					call := time.Now().UnixNano()

					var lease pb.QueueLeaseResponse
					err := c.QueueLease(ctx, &pb.QueueLeaseRequest{
						QueueName:      queueName,
						ClientId:       leaseClientID,
						RequestTimeout: "1s",
						BatchSize:      1,
					}, &lease)
					if err != nil {
						if ctx.Err() != nil {
							return
						}
						var de duh.Error
						if errors.As(err, &de) && de.Code() == duh.CodeRetryRequest {
							continue
						}
						t.Errorf("consumer %d lease: %v", ci, err)
						return
					}

					if len(lease.Items) == 0 {
						mu.Lock()
						history = append(history, porcupine.Operation{
							ClientId: clientID,
							Input:    linInput{op: opLease},
							Call:     call,
							Output:   linOutput{ok: false},
							Return:   time.Now().UnixNano(),
						})
						mu.Unlock()
						continue
					}

					itemID := string(lease.Items[0].Bytes)
					mu.Lock()
					history = append(history, porcupine.Operation{
						ClientId: clientID,
						Input:    linInput{op: opLease},
						Call:     call,
						Output:   linOutput{ids: []string{itemID}, ok: true},
						Return:   time.Now().UnixNano(),
					})
					mu.Unlock()

					if rng.Intn(100) < retryPercent {
						retryInp := linInput{op: opRetry, ids: []string{itemID}}
						retryCall := time.Now().UnixNano()

						err = c.QueueRetry(ctx, &pb.QueueRetryRequest{
							QueueName: queueName,
							Partition: lease.Partition,
							Items: []*pb.QueueRetryItem{
								{Id: lease.Items[0].Id},
							},
						})
						if err != nil {
							if ctx.Err() != nil {
								return
							}
							t.Errorf("consumer %d retry: %v", ci, err)
							return
						}

						mu.Lock()
						history = append(history, porcupine.Operation{
							ClientId: clientID,
							Input:    retryInp,
							Call:     retryCall,
							Output:   linOutput{ok: true},
							Return:   time.Now().UnixNano(),
						})
						mu.Unlock()
						continue
					}

					compInp := linInput{op: opComplete, ids: []string{itemID}}
					compCall := time.Now().UnixNano()

					err = c.QueueComplete(ctx, &pb.QueueCompleteRequest{
						QueueName:      queueName,
						Partition:      lease.Partition,
						RequestTimeout: "5s",
						Ids:            []string{lease.Items[0].Id},
					})
					if err != nil {
						if ctx.Err() != nil {
							return
						}
						t.Errorf("consumer %d complete: %v", ci, err)
						return
					}

					mu.Lock()
					history = append(history, porcupine.Operation{
						ClientId: clientID,
						Input:    compInp,
						Call:     compCall,
						Output:   linOutput{ok: true},
						Return:   time.Now().UnixNano(),
					})
					mu.Unlock()
					completed.Add(1)
				}
			}()
		}

		wg.Wait()

		t.Logf("recorded %d operations", len(history))

		result, info := porcupine.CheckOperationsVerbose(linearizabilityModel, history, 30*time.Second)
		t.Logf("porcupine result: %s", result)

		if result != porcupine.Ok {
			name := strings.ReplaceAll(t.Name(), "/", "_")
			path := fmt.Sprintf("testdata/linearization_%s.html", name)
			if err := os.MkdirAll("testdata", 0755); err == nil {
				if err := porcupine.VisualizePath(linearizabilityModel, info, path); err == nil {
					t.Logf("visualization written to %s", path)
				}
			}
		}

		require.Equal(t, porcupine.Ok, result, "history is not linearizable")
	})
}
