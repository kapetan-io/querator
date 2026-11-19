package service_test

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"io"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duh-rpc/duh-go"
	"github.com/duh-rpc/duh-go/retry"
	"github.com/jackc/pgx/v5"
	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/daemon"
	"github.com/kapetan-io/querator/internal"
	"github.com/kapetan-io/querator/internal/store"
	pb "github.com/kapetan-io/querator/proto"
	svc "github.com/kapetan-io/querator/service"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/color"
	"github.com/kapetan-io/tackle/random"
	"github.com/kapetan-io/tackle/set"
	tigerassert "github.com/kapetan-io/tiger/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/mongo"
	mongoopts "go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	ExpireTimeout = "24h0m0s"
	LeaseTimeout  = "1m0s"
)

var RetryTenTimes = retry.Policy{Interval: retry.Sleep(100 * clock.Millisecond), Attempts: 20}

type NewStorageFunc func() store.Config

var log *slog.Logger

var goleakOptions = []goleak.Option{
	goleak.IgnoreTopFunction("github.com/testcontainers/testcontainers-go.(*Reaper).connect.func1"),
}

// ---------------------------------------------------------------------
// Shared PostgreSQL Container
// ---------------------------------------------------------------------

type sharedPostgresContainer struct {
	container *postgres.PostgresContainer
	host      string
	port      string
	dbCounter atomic.Int64
}

var (
	sharedPostgres     *sharedPostgresContainer
	sharedPostgresOnce sync.Once
	sharedPostgresErr  error
)

func getSharedPostgresContainer() (*sharedPostgresContainer, error) {
	sharedPostgresOnce.Do(func() {
		sharedPostgres = &sharedPostgresContainer{}
		sharedPostgresErr = sharedPostgres.Start(context.Background())
	})
	return sharedPostgres, sharedPostgresErr
}

func (s *sharedPostgresContainer) Start(ctx context.Context) (err error) {
	// Recover from panic when Docker is not available and convert to clear error
	defer func() {
		if r := recover(); r != nil {
			msg := fmt.Sprintf("%v", r)
			if strings.Contains(msg, "rootless Docker not found") {
				err = fmt.Errorf("Docker is not available: %s", msg)
			} else {
				err = fmt.Errorf("container start panicked: %s\n%s", msg, debug.Stack())
			}
		}
	}()

	container, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("postgres"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second)),
	)
	if err != nil {
		return fmt.Errorf("failed to start postgres container: %w", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		return fmt.Errorf("failed to get container host: %w", err)
	}

	mappedPort, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		return fmt.Errorf("failed to get mapped port: %w", err)
	}

	s.container = container
	s.host = host
	s.port = mappedPort.Port()
	return nil
}

func (s *sharedPostgresContainer) Stop(ctx context.Context) error {
	if s.container == nil {
		return nil
	}

	if err := s.container.Terminate(ctx); err != nil {
		log.Warn("failed to terminate postgres container", "error", err)
		return err
	}
	return nil
}

// testDatabase is a database created in the shared postgres container for one test.
type testDatabase struct {
	DSN  string
	Name string
}

func (s *sharedPostgresContainer) CreateDatabase(ctx context.Context) (testDatabase, error) {
	dbName := fmt.Sprintf("querator_test_%d", s.dbCounter.Add(1))
	adminDSN := fmt.Sprintf("postgres://postgres:postgres@%s:%s/postgres?sslmode=disable", s.host, s.port)

	conn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		return testDatabase{}, fmt.Errorf("connect to postgres db: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	_, err = conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", pgx.Identifier{dbName}.Sanitize()))
	if err != nil {
		return testDatabase{}, fmt.Errorf("create database %s: %w", dbName, err)
	}

	return testDatabase{
		DSN:  fmt.Sprintf("postgres://postgres:postgres@%s:%s/%s?sslmode=disable", s.host, s.port, dbName),
		Name: dbName,
	}, nil
}

func (s *sharedPostgresContainer) DropDatabase(ctx context.Context, dbName string) error {
	adminDSN := fmt.Sprintf("postgres://postgres:postgres@%s:%s/postgres?sslmode=disable", s.host, s.port)

	conn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		return fmt.Errorf("connect to postgres db: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	_, err = conn.Exec(ctx,
		"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()",
		dbName)
	if err != nil {
		log.Warn("failed to terminate connections", "database", dbName, "error", err)
	}

	_, err = conn.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", pgx.Identifier{dbName}.Sanitize()))
	if err != nil {
		return fmt.Errorf("drop database %s: %w", dbName, err)
	}

	return nil
}

func TestMain(m *testing.M) {
	// TEST_LOGGING env var controls log output:
	// - unset or empty: debug level colored text log output
	// - "ci": discards logs (reduces noise during CI runs)
	switch os.Getenv("TEST_LOGGING") {
	case "ci":
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	default:
		log = slog.New(color.NewLog(&color.LogOptions{
			HandlerOptions: slog.HandlerOptions{
				ReplaceAttr: color.SuppressAttrs(slog.TimeKey),
				Level:       internal.LevelDebugAll,
			},
		}))
	}

	defer func() {
		if sharedPostgres != nil {
			if err := sharedPostgres.Stop(context.Background()); err != nil {
				fmt.Fprintf(os.Stderr, "failed to stop shared postgres container: %v\n", err)
			}
		}
		if sharedMongo != nil {
			if err := sharedMongo.Stop(context.Background()); err != nil {
				fmt.Fprintf(os.Stderr, "failed to stop shared mongo container: %v\n", err)
			}
		}
	}()

	goleak.VerifyTestMain(m, goleakOptions...)
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

func (td *testDaemon) MustClient() *querator.Client {
	return td.d.MustClient()
}

func (td *testDaemon) Context() context.Context {
	return td.ctx
}

func (td *testDaemon) Service() *svc.Service {
	return td.d.Service()
}

func newDaemon(t *testing.T, duration clock.Duration, conf svc.Config) *testDaemon {
	t.Helper()

	set.Default(&conf.Log, log)
	td := &testDaemon{}
	var err error

	td.ctx, td.cancel = context.WithTimeout(context.Background(), duration)
	td.d, err = daemon.NewDaemon(td.ctx, daemon.Config{
		Service:       conf,
		ListenAddress: "localhost:0",
	})
	require.NoError(t, err)
	return td
}

// ---------------------------------------------------------------------
// Badger test setup
// ---------------------------------------------------------------------

type badgerTestSetup struct {
	Dir string
}

func (b *badgerTestSetup) Setup(bc store.BadgerConfig) store.Config {
	if !dirExists(b.Dir) {
		tigerassert.NoError(os.Mkdir(b.Dir, 0777), "failed to create badger test directory")
	}
	b.Dir = filepath.Join(b.Dir, random.String("test-data-", 10))
	tigerassert.NoError(os.Mkdir(b.Dir, 0777), "failed to create badger test data directory")
	bc.StorageDir = b.Dir
	bc.Log = log

	var conf store.Config
	conf.Namespaces = store.NewBadgerNamespaces(bc)
	conf.Queues = store.NewBadgerQueues(bc)
	conf.Users = store.NewBadgerUsers(bc)
	conf.APIKeys = store.NewBadgerAPIKeys(bc)
	conf.Roles = store.NewBadgerRoles(bc)
	conf.RoleBindings = store.NewBadgerRoleBindings(bc)
	conf.PartitionStorage = []store.PartitionStorage{
		{
			PartitionStore: store.NewBadgerPartitionStore(bc),
			Name:           "badger-0",
			Affinity:       1,
		},
	}
	return conf
}

func (b *badgerTestSetup) Teardown() {
	tigerassert.NoError(os.RemoveAll(b.Dir), "failed to remove badger test data directory")
}

// ---------------------------------------------------------------------
// PostgreSQL test setup
// ---------------------------------------------------------------------

type postgresTestSetup struct {
	dsn    string
	dbName string
}

func (p *postgresTestSetup) Setup(conf store.PostgresConfig) store.Config {
	container, err := getSharedPostgresContainer()
	tigerassert.NoError(err, "failed to get shared postgres container")

	database, err := container.CreateDatabase(context.Background())
	tigerassert.NoError(err, "failed to create test database")

	p.dsn = database.DSN
	p.dbName = database.Name

	conf.ConnectionString = database.DSN
	conf.Log = log
	conf.MaxConns = 10

	var storageConf store.Config
	storageConf.Queues = store.NewPostgresQueues(conf)
	storageConf.PartitionStorage = []store.PartitionStorage{
		{
			PartitionStore: store.NewPostgresPartitionStore(conf),
			Name:           "postgres-0",
			Affinity:       1,
		},
	}
	return storageConf
}

func (p *postgresTestSetup) Teardown() {
	if p.dbName == "" {
		return
	}

	container, err := getSharedPostgresContainer()
	if err != nil {
		log.Warn("failed to get shared postgres container for cleanup", "error", err)
		return
	}

	if err := container.DropDatabase(context.Background(), p.dbName); err != nil {
		log.Warn("failed to drop test database", "database", p.dbName, "error", err)
	}
}

// ---------------------------------------------------------------------
// Shared MongoDB Container
// ---------------------------------------------------------------------

type sharedMongoContainer struct {
	container *mongodb.MongoDBContainer
	uri       string
	dbCounter atomic.Int64
}

var (
	sharedMongo     *sharedMongoContainer
	sharedMongoOnce sync.Once
	sharedMongoErr  error
)

func getSharedMongoContainer() (*sharedMongoContainer, error) {
	sharedMongoOnce.Do(func() {
		sharedMongo = &sharedMongoContainer{}
		sharedMongoErr = sharedMongo.Start(context.Background())
	})
	return sharedMongo, sharedMongoErr
}

func (s *sharedMongoContainer) Start(ctx context.Context) (err error) {
	// Recover from panic when Docker is not available and convert to clear error
	defer func() {
		if r := recover(); r != nil {
			msg := fmt.Sprintf("%v", r)
			if strings.Contains(msg, "rootless Docker not found") {
				err = fmt.Errorf("Docker is not available: %s", msg)
			} else {
				err = fmt.Errorf("container start panicked: %s\n%s", msg, debug.Stack())
			}
		}
	}()

	// A standalone mongod (no replica set) is used deliberately: it is the practical proof that the
	// non-transactional MongoDB backend design holds.
	container, err := mongodb.Run(ctx, "mongo:7")
	if err != nil {
		return fmt.Errorf("failed to start mongo container: %w", err)
	}

	uri, err := container.ConnectionString(ctx)
	if err != nil {
		return fmt.Errorf("failed to get mongo connection string: %w", err)
	}

	s.container = container
	s.uri = uri
	return nil
}

func (s *sharedMongoContainer) Stop(ctx context.Context) error {
	if s.container == nil {
		return nil
	}

	if err := s.container.Terminate(ctx); err != nil {
		log.Warn("failed to terminate mongo container", "error", err)
		return err
	}
	return nil
}

func (s *sharedMongoContainer) DatabaseName() string {
	return fmt.Sprintf("querator_test_%d", s.dbCounter.Add(1))
}

func (s *sharedMongoContainer) DropDatabase(ctx context.Context, dbName string) error {
	client, err := mongo.Connect(ctx, mongoopts.Client().ApplyURI(s.uri))
	if err != nil {
		return fmt.Errorf("connect to mongo: %w", err)
	}
	defer func() { _ = client.Disconnect(ctx) }()

	if err := client.Database(dbName).Drop(ctx); err != nil {
		return fmt.Errorf("drop database %s: %w", dbName, err)
	}
	return nil
}

// ---------------------------------------------------------------------
// MongoDB test setup
// ---------------------------------------------------------------------

type mongoTestSetup struct {
	dbName string
}

func (m *mongoTestSetup) Setup(conf store.MongoConfig) store.Config {
	container, err := getSharedMongoContainer()
	tigerassert.NoError(err, "failed to get shared mongo container")

	m.dbName = container.DatabaseName()

	conf.ConnectionString = container.uri
	conf.Database = m.dbName
	conf.Log = log

	var storageConf store.Config
	storageConf.Queues = store.NewMongoQueues(conf)
	storageConf.PartitionStorage = []store.PartitionStorage{
		{
			PartitionStore: store.NewMongoPartitionStore(conf),
			Name:           "mongo-0",
			Affinity:       1,
		},
	}
	return storageConf
}

func (m *mongoTestSetup) Teardown() {
	if m.dbName == "" {
		return
	}

	container, err := getSharedMongoContainer()
	if err != nil {
		log.Warn("failed to get shared mongo container for cleanup", "error", err)
		return
	}

	if err := container.DropDatabase(context.Background(), m.dbName); err != nil {
		log.Warn("failed to drop test database", "database", m.dbName, "error", err)
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

// storageItemFields holds the StorageItem fields tests compare, so require.Equal can compare two items.
type storageItemFields struct {
	Id             string
	IsLeased       bool
	ExpireDeadline time.Time
	LeaseDeadline  time.Time
	Attempts       int32
	Reference      string
	Encoding       string
	Kind           string
	Payload        string
}

func storageItemFieldsOf(item *pb.StorageItem) storageItemFields {
	return storageItemFields{
		Id:             item.Id,
		IsLeased:       item.IsLeased,
		ExpireDeadline: item.ExpireDeadline.AsTime(),
		LeaseDeadline:  item.LeaseDeadline.AsTime(),
		Attempts:       item.Attempts,
		Reference:      item.Reference,
		Encoding:       item.Encoding,
		Kind:           item.Kind,
		Payload:        string(item.Payload),
	}
}

//nolint:unparam // conf param kept for consistency with other setup functions
func setupMemoryStorage(conf store.Config) store.Config {
	conf.Namespaces = store.NewMemoryNamespaces(log)
	conf.Queues = store.NewMemoryQueues(log)
	conf.Users = store.NewMemoryUsers(log)
	conf.APIKeys = store.NewMemoryAPIKeys(log)
	conf.Roles = store.NewMemoryRoles(log)
	conf.RoleBindings = store.NewMemoryRoleBindings(log)
	conf.PartitionStorage = []store.PartitionStorage{
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

func writeRandomItems(t *testing.T, ctx context.Context, c *querator.Client,
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

func pauseAndLease(t *testing.T, ctx context.Context, s *svc.Service, c *querator.Client, name string,
	requests []*pb.QueueLeaseRequest) []*pb.QueueLeaseResponse {
	t.Helper()

	// Pause processing of the queue for testing
	require.NoError(t, s.PauseQueue(ctx, name))

	responses := make([]*pb.QueueLeaseResponse, len(requests))
	for i := range responses {
		responses[i] = &pb.QueueLeaseResponse{}
	}

	// Cancelling leaseCtx ends any lease request still waiting once the wait below times out.
	leaseCtx, cancelLeases := context.WithCancel(ctx)
	defer cancelLeases()

	var wg sync.WaitGroup

	for i := range requests {
		wg.Go(func() {
			if err := c.QueueLease(leaseCtx, requests[i], responses[i]); err != nil {
				var d duh.Error
				if errors.As(err, &d) {
					if d.Code() == duh.CodeRetryRequest {
						return
					}
				}
				// WaitGroup.Go must not panic, or Done is skipped and wg.Wait hangs
				t.Errorf("lease request %d: %v", i, err)
			}
		})
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
	require.NoError(t, s.ResumeQueue(ctx, name))

	// Wait for each request to complete
	timer := clock.AfterFunc(5*clock.Second, cancelLeases)
	wg.Wait()
	if !timer.Stop() {
		t.Fatalf("timed out waiting for distribution of requests")
	}
	return responses
}

// backgroundCall runs Call in its own goroutine while While runs on the calling goroutine, so
// While can drive the system and use require. When While returns normally, run waits for Call;
// when While fails the test, run does not. The Go and Wait sit in a named method because Tiger
// does not credit a Wait inside a t.Run literal (ENG-762).
type backgroundCall struct {
	Call  func()
	While func()
}

func (b backgroundCall) run() {
	var wg sync.WaitGroup
	wg.Go(b.Call)
	b.While()
	wg.Wait()
}

// concurrently calls work once for each index in [0, count), each call in its own goroutine,
// and returns once every call has returned.
func concurrently(count int, work func(i int)) {
	var wg sync.WaitGroup
	for i := range count {
		wg.Go(func() { work(i) })
	}
	wg.Wait()
}

func untilLeaseClientWaiting(t *testing.T, c *querator.Client, queueName string, numWaiting int) error {
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

// queueInfoFields holds the QueueInfo fields tests compare, so require.Equal can compare two queues.
type queueInfoFields struct {
	QueueName     string
	ExpireTimeout string
	LeaseTimeout  string
	MaxAttempts   int32
	DeadQueue     string
	Reference     string
}

func queueInfoFieldsOf(info *pb.QueueInfo) queueInfoFields {
	return queueInfoFields{
		QueueName:     info.QueueName,
		ExpireTimeout: info.ExpireTimeout,
		LeaseTimeout:  info.LeaseTimeout,
		MaxAttempts:   info.MaxAttempts,
		DeadQueue:     info.DeadQueue,
		Reference:     info.Reference,
	}
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

func createRandomQueues(t *testing.T, ctx context.Context, c *querator.Client, count int) []*pb.QueueInfo {
	t.Helper()

	var idx int
	var items []*pb.QueueInfo
	for i := 0; i < count; i++ {
		timeOuts := random.Slice(validTimeouts)
		info := pb.QueueInfo{
			QueueName:           fmt.Sprintf("queue-%05d", idx),
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

func createQueueAndWait(t *testing.T, ctx context.Context, c *querator.Client, info *pb.QueueInfo) {
	t.Helper()

	require.NoError(t, c.QueuesCreate(ctx, info))

	// Wait for the partitions to become available
	err := retry.On(ctx, RetryTenTimes, func(ctx context.Context, i int) error {
		var resp pb.QueueStatsResponse
		require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: info.QueueName}, &resp))
		for _, p := range resp.LogicalQueues[0].Partitions {
			if p.Failures != 0 {
				return fmt.Errorf("partition '%d' never became available", p.Partition)
			}
		}
		return nil
	})
	require.NoError(t, err)
}

func leaseOne(t *testing.T, ctx context.Context, c *querator.Client, queueName string) *pb.QueueLeaseItem {
	t.Helper()
	var lease pb.QueueLeaseResponse
	require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
		ClientId:       random.String("client-", 10),
		RequestTimeout: "3s",
		QueueName:      queueName,
		BatchSize:      1,
	}, &lease))
	require.Len(t, lease.Items, 1)
	return lease.Items[0]
}

// itemReference is the Reference a test gave an item when producing it. A distinct type keeps
// it from being swapped with the queue name beside it.
type itemReference string

func isLeased(t *testing.T, ctx context.Context, c *querator.Client, queueName string, ref itemReference) bool {
	t.Helper()
	var resp pb.StorageItemsListResponse
	require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &resp, nil))
	item := findInStorageList(string(ref), &resp)
	require.NotNil(t, item)
	return item.IsLeased
}

// isUnLeased reports whether the item is in the queue and not leased. It takes no t and returns
// false on any error so it can be polled from require.Eventually.
func isUnLeased(ctx context.Context, c *querator.Client, queueName string, ref itemReference) bool {
	var resp pb.StorageItemsListResponse
	if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
		return false
	}
	item := findInStorageList(string(ref), &resp)
	return item != nil && !item.IsLeased
}

// nextLifecycleRun returns how long until the lifecycle next runs on the queue's only partition.
func nextLifecycleRun(t *testing.T, ctx context.Context, c *querator.Client, queueName string) clock.Duration {
	t.Helper()
	var stats pb.QueueStatsResponse
	require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats))
	require.Len(t, stats.LogicalQueues, 1)
	require.Len(t, stats.LogicalQueues[0].Partitions, 1)
	next, err := clock.ParseDuration(stats.LogicalQueues[0].Partitions[0].NextLifecycleRun)
	require.NoError(t, err)
	return next
}

// waitForLifecycleRun waits until the next lifecycle run of the queue's only partition is in
// the future again, which happens only after a due run has finished and rescheduled itself.
// Call it right after advancing a frozen clock past the next run; otherwise it returns at once.
func waitForLifecycleRun(t *testing.T, ctx context.Context, c *querator.Client, queueName string) {
	t.Helper()
	require.Eventually(t, func() bool {
		var stats pb.QueueStatsResponse
		if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats); err != nil {
			return false
		}
		if len(stats.LogicalQueues) != 1 || len(stats.LogicalQueues[0].Partitions) != 1 {
			return false
		}
		next, err := clock.ParseDuration(stats.LogicalQueues[0].Partitions[0].NextLifecycleRun)
		return err == nil && next > 0
	}, 5*clock.Second, 20*clock.Millisecond)
}
