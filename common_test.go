package querator_test

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/duh-rpc/duh-go"
	"github.com/duh-rpc/duh-go/retry"
	que "github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/daemon"
	"github.com/kapetan-io/querator/internal"
	"github.com/kapetan-io/querator/internal/store"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/color"
	"github.com/kapetan-io/tackle/random"
	"github.com/kapetan-io/tackle/set"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/types/known/timestamppb"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

const (
	ExpireTimeout = "24h0m0s"
	LeaseTimeout  = "1m0s"
)

var RetryTenTimes = retry.Policy{Interval: retry.Sleep(100 * clock.Millisecond), Attempts: 20}

type NewStorageFunc func() store.StorageConfig

var log *slog.Logger

func TestMain(m *testing.M) {

	logFlag := flag.String("logging", "", "indicates the type of logging during tests. "+
		"If unset tests run with debug level colored text log output. "+
		"If set to 'ci' discards logs during tests which greatly reduces logs during CI runs")
	flag.Parse()

	switch *logFlag {
	case "":
		log = slog.New(color.NewLog(&color.LogOptions{
			HandlerOptions: slog.HandlerOptions{
				ReplaceAttr: color.SuppressAttrs(slog.TimeKey),
				Level:       internal.LevelDebugAll,
			},
		}))
	case "ci":
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	goleak.VerifyTestMain(m)
	//os.Exit(m.Run())
}

// ---------------------------------------------------------------------
// TestDaemon
// ---------------------------------------------------------------------

type testDaemon struct {
	cancel context.CancelFunc
	ctx    context.Context
	d      *daemon.Daemon
}

func (td *testDaemon) Shutdown(t *testing.T) {
	t.Helper()

	require.NoError(t, td.d.Shutdown(td.ctx))
	td.cancel()
}

func (td *testDaemon) MustClient() *que.Client {
	return td.d.MustClient()
}

func (td *testDaemon) Context() context.Context {
	return td.ctx
}

func (td *testDaemon) Service() *que.Service {
	return td.d.Service()
}

func newDaemon(t *testing.T, duration clock.Duration, conf que.ServiceConfig) (*testDaemon, *que.Client, context.Context) {
	t.Helper()

	set.Default(&conf.Log, log)
	td := &testDaemon{}
	var err error

	td.ctx, td.cancel = context.WithTimeout(context.Background(), duration)
	td.d, err = daemon.NewDaemon(td.ctx, daemon.Config{
		ServiceConfig: conf,
	})
	require.NoError(t, err)
	return td, td.d.MustClient(), td.ctx
}

// ---------------------------------------------------------------------
// Badger test setup
// ---------------------------------------------------------------------

type badgerTestSetup struct {
	Dir string
}

func (b *badgerTestSetup) Setup(bc store.BadgerConfig) store.StorageConfig {
	if !dirExists(b.Dir) {
		if err := os.Mkdir(b.Dir, 0777); err != nil {
			panic(err)
		}
	}
	b.Dir = filepath.Join(b.Dir, random.String("test-data-", 10))
	if err := os.Mkdir(b.Dir, 0777); err != nil {
		panic(err)
	}
	bc.StorageDir = b.Dir
	bc.Log = log

	var conf store.StorageConfig
	conf.Queues = store.NewBadgerQueues(bc)
	conf.Backends = []store.Backend{
		{
			PartitionStore: store.NewBadgerPartitionStore(bc),
			Name:           "badger-0",
			Affinity:       1,
		},
	}
	return conf
}

func (b *badgerTestSetup) Teardown() {
	if err := os.RemoveAll(b.Dir); err != nil {
		panic(err)
	}
}

// ---------------------------------------------
// Test Helpers
// ---------------------------------------------

func produceRandomItems(count int) []*pb.QueueProduceItem {
	batch := random.String("", 5)
	var items []*pb.QueueProduceItem
	for i := 0; i < count; i++ {
		items = append(items, &pb.QueueProduceItem{
			Reference: random.String("ref-", 10),
			Encoding:  random.String("enc-", 10),
			Kind:      random.String("kind-", 10),
			Bytes:     []byte(fmt.Sprintf("message-%s-%d", batch, i)),
		})
	}
	return items
}

func compareStorageItem(t *testing.T, l *pb.StorageItem, r *pb.StorageItem) {
	t.Helper()
	require.Equal(t, l.Id, r.Id)
	require.Equal(t, l.IsLeased, r.IsLeased)
	require.Equal(t, l.ExpireDeadline.AsTime(), r.ExpireDeadline.AsTime())
	require.Equal(t, l.LeaseDeadline.AsTime(), r.LeaseDeadline.AsTime())
	require.Equal(t, l.Attempts, r.Attempts)
	require.Equal(t, l.Reference, r.Reference)
	require.Equal(t, l.Encoding, r.Encoding)
	require.Equal(t, l.Kind, r.Kind)
	require.Equal(t, l.Payload, r.Payload)
}

func setupMemoryStorage(conf store.StorageConfig) store.StorageConfig {
	conf.Queues = store.NewMemoryQueues(log)
	conf.Backends = []store.Backend{
		{
			PartitionStore: store.NewMemoryPartitionStore(conf, log),
			Name:           "memory-0",
			Affinity:       1,
		},
	}
	return conf
}

func dirExists(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
		return false
	}
	return info.IsDir()
}

func findInResponses(responses []*pb.QueueLeaseResponse, ref string) bool {

	for _, item := range responses {
		for _, idItem := range item.Items {
			if idItem.Reference == ref {
				return true
			}
		}
	}
	return false
}

func findInStorageList(ref string, resp *pb.StorageItemsListResponse) *pb.StorageItem {
	for _, i := range resp.Items {
		if i.Reference == ref {
			return i
		}
	}
	return nil
}

func findInLeaseResp(ref string, resp *pb.QueueLeaseResponse) *pb.QueueLeaseItem {
	for _, i := range resp.Items {
		if i.Reference == ref {
			return i
		}
	}
	return nil
}

func writeRandomItems(t *testing.T, ctx context.Context, c *que.Client,
	name string, count int) []*pb.StorageItem {

	t.Helper()
	expire := clock.Now().UTC().Add(random.Duration(10*clock.Second, clock.Minute))

	var items []*pb.StorageItem
	for i := 0; i < count; i++ {
		items = append(items, &pb.StorageItem{
			ExpireDeadline: timestamppb.New(expire),
			Attempts:       int32(rand.Intn(10)),
			Reference:      random.String("ref-", 10),
			Encoding:       random.String("enc-", 10),
			Kind:           random.String("kind-", 10),
			Payload:        []byte(fmt.Sprintf("message-%d", i)),
		})
	}

	var resp pb.StorageItemsImportResponse
	err := c.StorageItemsImport(ctx, &pb.StorageItemsImportRequest{Items: items, QueueName: name}, &resp)
	require.NoError(t, err)
	return resp.Items
}

func randomSliceStrings(count int) []string {
	var result []string
	for i := 0; i < count; i++ {
		result = append(result, fmt.Sprintf("string-%d", i))
	}
	return result
}

func pauseAndLease(t *testing.T, ctx context.Context, s *que.Service, c *que.Client, name string,
	requests []*pb.QueueLeaseRequest) []*pb.QueueLeaseResponse {
	t.Helper()

	// Pause processing of the queue for testing
	require.NoError(t, s.PauseQueue(ctx, name, true))

	responses := make([]*pb.QueueLeaseResponse, len(requests))
	for i := range responses {
		responses[i] = &pb.QueueLeaseResponse{}
	}

	var wg sync.WaitGroup
	wg.Add(len(requests))

	for i := range requests {
		go func(idx int) {
			defer wg.Done()
			if err := c.QueueLease(ctx, requests[idx], responses[idx]); err != nil {
				var d duh.Error
				if errors.As(err, &d) {
					if d.Code() == duh.CodeRetryRequest {
						return
					}
				}
				panic(err)
			}
		}(i)
	}

	_ctx, cancel := context.WithTimeout(context.Background(), clock.Second*10)
	defer cancel()

	// Wait until every request is waiting
	err := retry.On(_ctx, RetryTenTimes, func(ctx context.Context, i int) error {
		var resp pb.QueueStatsResponse
		require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: name}, &resp))
		// There should eventually be `len(requests)` waiting lease requests
		if int(resp.LogicalQueues[0].LeaseWaiting) != len(requests) {
			return fmt.Errorf("LeaseWaiting never reached expected %d", len(requests))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("while waiting on %d leased requests: %v", len(requests), err)
	}

	// Unpause processing of the queue to allow the leases to be filled.
	require.NoError(t, s.PauseQueue(ctx, name, false))

	// Wait for each request to complete
	done := make(chan struct{})

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-clock.After(5 * clock.Second):
		t.Fatalf("timed out waiting for distribution of requests")
	}
	return responses
}

func untilLeaseClientWaiting(t *testing.T, c *que.Client, queueName string, numWaiting int) error {
	_ctx, cancel := context.WithTimeout(context.Background(), 5*clock.Second)
	defer cancel()
	t.Helper()

	// Wait until every request is waiting
	return retry.On(_ctx, RetryTenTimes, func(ctx context.Context, i int) error {
		var resp pb.QueueStatsResponse
		require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &resp))
		if int(resp.LogicalQueues[0].LeaseWaiting) != numWaiting {
			return fmt.Errorf("LeaseWaiting never reached expected %d", numWaiting)
		}
		return nil
	})
}

func compareQueueInfo(t *testing.T, expected *pb.QueueInfo, actual *pb.QueueInfo) {
	t.Helper()
	require.Equal(t, expected.QueueName, actual.QueueName)
	require.Equal(t, expected.ExpireTimeout, actual.ExpireTimeout)
	require.Equal(t, expected.LeaseTimeout, actual.LeaseTimeout)
	require.Equal(t, expected.MaxAttempts, actual.MaxAttempts)
	require.Equal(t, expected.DeadQueue, actual.DeadQueue)
	require.Equal(t, expected.Reference, actual.Reference)
}

type Pair struct {
	Lease string
	Dead  string
}

var validTimeouts = []Pair{
	{
		Lease: "15s",
		Dead:  "1m0s",
	},
	{
		Lease: "1m0s",
		Dead:  "10m0s",
	},
	{
		Lease: "10m0s",
		Dead:  "24h0m0s",
	},
	{
		Lease: "30m0s",
		Dead:  "1h0m0s",
	},
}

func createRandomQueues(t *testing.T, ctx context.Context, c *que.Client, count int) []*pb.QueueInfo {
	t.Helper()

	var idx int
	var items []*pb.QueueInfo
	for i := 0; i < count; i++ {
		timeOuts := random.Slice(validTimeouts)
		info := pb.QueueInfo{
			QueueName:           fmt.Sprintf("queue-%05d", idx),
			DeadQueue:           random.String("dead-", 10),
			Reference:           random.String("ref-", 10),
			MaxAttempts:         int32(rand.Intn(100)),
			LeaseTimeout:        timeOuts.Lease,
			ExpireTimeout:       timeOuts.Dead,
			RequestedPartitions: 1,
		}
		idx++
		items = append(items, &info)
		require.NoError(t, c.QueuesCreate(ctx, &info))
	}
	return items
}
