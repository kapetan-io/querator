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
	"github.com/kapetan-io/querator/internal/types"
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
	Dir  string
	data string
}

func (b *badgerTestSetup) Setup(bc store.BadgerConfig) store.Config {
	if !dirExists(b.Dir) {
		tigerassert.NoError(os.Mkdir(b.Dir, 0777), "failed to create badger test directory")
	}
	b.data = filepath.Join(b.Dir, random.String("test-data-", 10))
	tigerassert.NoError(os.Mkdir(b.data, 0777), "failed to create badger test data directory")
	bc.StorageDir = b.data
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
	tigerassert.NoError(os.RemoveAll(b.data), "failed to remove badger test data directory")
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
	// non-transactional MongoDB backend design holds. Test commands let a test make a write apply
	// and still report an error, which no other fault a test can add does.
	container, err := mongodb.Run(ctx, "mongo:7",
		testcontainers.WithCmdArgs("--setParameter", "enableTestCommands=1"))
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
	conf = withMemoryAuth(conf)
	conf.Queues = store.NewMemoryQueues(log)
	conf.PartitionStorage = []store.PartitionStorage{
		{
			PartitionStore: store.NewMemoryPartitionStore(conf, log),
			Name:           "memory-0",
			Affinity:       1,
		},
	}
	return conf
}

func withMemoryAuth(conf store.Config) store.Config {
	conf.Namespaces = store.NewMemoryNamespaces(log)
	conf.Users = store.NewMemoryUsers(log)
	conf.APIKeys = store.NewMemoryAPIKeys(log)
	conf.Roles = store.NewMemoryRoles(log)
	conf.RoleBindings = store.NewMemoryRoleBindings(log)
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

// ---------------------------------------------------------------------
// Partition storage fault injection
// ---------------------------------------------------------------------

var (
	errProduceWriteFailed = errors.New("injected produce write failure")
	errLeaseWriteFailed   = errors.New("injected lease write failure")
	errClearFailed        = errors.New("injected clear failure")
)

// partitionFault makes partition storage refuse calls on demand. Each switch is set from the
// test, and every call it does not name reaches the real backend. While scanFailing holds a
// queue name, that queue's partitions fail the lifecycle scan, so each lifecycle run marks
// them failed.
type partitionFault struct {
	failingProduce atomic.Bool
	failingLease   atomic.Bool
	// failingClear holds the number of the one partition whose Clear fails, plus one. Zero fails none.
	failingClear atomic.Int32
	scanFailing  atomic.Pointer[string]
	// scanFailingOnly narrows scanFailing to the one partition whose number it holds, plus one. Zero fails every partition.
	scanFailingOnly atomic.Int32
	holdClear       atomic.Pointer[clearHold]
	// statsPastDeadline makes each Stats call that has a deadline wait until the deadline passes
	// before it reaches the backend, the way a scan longer than the read timeout outlasts it.
	statsPastDeadline atomic.Bool
	// statsCancelled hands each Stats call a context that is already cancelled, the way a caller
	// that gave up does.
	statsCancelled atomic.Bool
	// statsCancelNext holds how many of the next Stats calls get an already cancelled context. Each
	// call takes one, so later calls, such as a recovery scan, reach the backend untouched.
	statsCancelNext atomic.Int32
	// statsCancelAfter, when above zero, cancels each Stats call's context that long after the call
	// starts, the way a caller that gives up partway through a scan does.
	statsCancelAfter atomic.Int64
	// statsHung makes each Stats call that has a deadline wait until the deadline passes and
	// return its error without reaching the backend, the way a hung database does.
	statsHung atomic.Bool
	// statsFailing makes every Stats call fail without reaching the backend, the way a backend
	// that is down does.
	statsFailing atomic.Bool
}

// clearHold stops a Clear partway through. On the Clear's After-th check of its context it
// closes Reached, then waits until Release closes or the Clear's own context ends, so the test
// knows the clear is running and decides when it goes on. With After 0 the Clear waits before it
// reaches the backend, which holds on every backend, including one whose Clear never checks its context.
type clearHold struct {
	After   int
	Reached chan struct{}
	Release chan struct{}
}

// holdWait is how long a test waits for a hold to be reached before it fails.
const holdWait = 5 * clock.Second

// WaitReached returns once the clear has made its After-th check of its context. It fails the
// test after holdWait, so a clear that never gets that far does not wait for the daemon's context.
func (h *clearHold) WaitReached(t *testing.T, ctx context.Context) {
	t.Helper()
	select {
	case <-h.Reached:
	case <-ctx.Done():
		require.FailNow(t, "the daemon stopped before the clear reached its hold")
	case <-clock.After(holdWait):
		require.FailNowf(t, "the clear never reached its hold",
			"the clear did not make %d checks of its context within %s", h.After, holdWait)
	}
}

type heldContext struct {
	context.Context
	hold   *clearHold
	checks int
}

func (h *heldContext) Err() error {
	ctx := h.Context
	h.checks++
	if h.checks == h.hold.After {
		close(h.hold.Reached)
		select {
		case <-h.hold.Release:
		case <-ctx.Done():
		}
	}
	return ctx.Err()
}

func (f *partitionFault) Wrap(conf store.Config) store.Config {
	wrapped := make([]store.PartitionStorage, len(conf.PartitionStorage))
	for i, ps := range conf.PartitionStorage {
		ps.PartitionStore = faultyPartitionStore{PartitionStore: ps.PartitionStore, fault: f}
		wrapped[i] = ps
	}
	conf.PartitionStorage = wrapped
	return conf
}

type faultyPartitionStore struct {
	store.PartitionStore
	fault *partitionFault
}

func (f faultyPartitionStore) Get(info types.PartitionInfo) store.Partition {
	return faultyPartition{Partition: f.PartitionStore.Get(info), fault: f.fault}
}

type faultyPartition struct {
	store.Partition
	fault *partitionFault
}

func (f faultyPartition) Produce(ctx context.Context, batch types.ProduceBatch, now clock.Time) error {
	if f.fault.failingProduce.Load() {
		return errProduceWriteFailed
	}
	return f.Partition.Produce(ctx, batch, now)
}

func (f faultyPartition) Lease(ctx context.Context, batch types.LeaseBatch, opts store.LeaseOptions) error {
	if f.fault.failingLease.Load() {
		return errLeaseWriteFailed
	}
	return f.Partition.Lease(ctx, batch, opts)
}

func (f faultyPartition) Clear(ctx context.Context, req types.ClearRequest) error {
	if f.fault.failingClear.Load() == int32(f.Info().PartitionNum)+1 {
		return errClearFailed
	}
	if hold := f.fault.holdClear.Load(); hold != nil {
		if hold.After == 0 {
			close(hold.Reached)
			select {
			case <-hold.Release:
			case <-ctx.Done():
			}
			return f.Partition.Clear(ctx, req)
		}
		return f.Partition.Clear(&heldContext{Context: ctx, hold: hold}, req)
	}
	return f.Partition.Clear(ctx, req)
}

var errScanFailed = errors.New("injected lifecycle scan failure")

func (f *partitionFault) takeStatsCancelNext() bool {
	if f.statsCancelNext.Add(-1) >= 0 {
		return true
	}
	f.statsCancelNext.Add(1)
	return false
}

var errStatsFailed = errors.New("injected stats failure")

func (f faultyPartition) Stats(ctx context.Context, stats *types.PartitionStats, now clock.Time) error {
	if f.fault.statsFailing.Load() {
		return errStatsFailed
	}
	if _, ok := ctx.Deadline(); ok && f.fault.statsHung.Load() {
		<-ctx.Done()
		return ctx.Err()
	}
	if _, ok := ctx.Deadline(); ok && f.fault.statsPastDeadline.Load() {
		<-ctx.Done()
	}
	if after := clock.Duration(f.fault.statsCancelAfter.Load()); after > 0 {
		cancellable, cancel := context.WithCancel(ctx)
		defer cancel()
		timer := clock.AfterFunc(after, cancel)
		defer timer.Stop()
		return f.Partition.Stats(cancellable, stats, now)
	}
	if f.fault.statsCancelled.Load() || f.fault.takeStatsCancelNext() {
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		return f.Partition.Stats(cancelled, stats, now)
	}
	return f.Partition.Stats(ctx, stats, now)
}

func (f faultyPartition) ScanForActions(ctx context.Context, scan *types.LifeCycleScan, now clock.Time) error {
	if queue := f.fault.scanFailing.Load(); queue != nil && *queue == f.Info().Queue.Name {
		if only := f.fault.scanFailingOnly.Load(); only != 0 && int(only)-1 != f.Info().PartitionNum {
			return f.Partition.ScanForActions(ctx, scan, now)
		}
		return errScanFailed
	}
	return f.Partition.ScanForActions(ctx, scan, now)
}

// lifecycleSpy wraps a partition store. It records each lifecycle page read and each produce,
// complete and retry in the order the partition sees them, can hold the first page of a pass
// until the test releases it, and can make the lifecycle's reads fail. With watchScheduled set
// it records and holds the scheduled pass's pages instead of the lifecycle's.
type lifecycleSpy struct {
	mu             sync.Mutex
	watchScheduled bool
	armed          bool
	reached        chan struct{}
	release        chan struct{}
	released       sync.Once
	events         []string
	actions        []int
	pages          int
	failScans      atomic.Bool
	failScheduled  atomic.Bool
	failDeadLetter atomic.Bool
	failDelete     atomic.Bool
	holdProduce    atomic.Bool
	produceHeld    chan struct{}
	produceRelease chan struct{}
}

func (s *lifecycleSpy) Wrap(conf store.Config) store.Config {
	for i := range conf.PartitionStorage {
		conf.PartitionStorage[i].PartitionStore = spyStore{PartitionStore: conf.PartitionStorage[i].PartitionStore, spy: s}
	}
	return conf
}

// HoldFirstPage starts recording and makes the next page read wait until Release.
func (s *lifecycleSpy) HoldFirstPage() {
	defer s.mu.Unlock()
	s.mu.Lock()
	s.armed = true
	s.reached = make(chan struct{})
	s.release = make(chan struct{})
}

// WaitForFirstPage returns once the lifecycle is waiting to read the held page.
func (s *lifecycleSpy) WaitForFirstPage(t *testing.T, ctx context.Context) {
	t.Helper()
	select {
	case <-s.reached:
	case <-ctx.Done():
		require.FailNow(t, "the daemon stopped before the lifecycle read its first page")
	case <-clock.After(holdWait):
		require.FailNowf(t, "the lifecycle never read its first page", "no page read within %s", holdWait)
	}
}

// WhileFirstPageHeld runs call while the lifecycle waits to read the held page, gives the request
// call makes time to reach the queue, then lets the pass go on. The goroutine starts in a named
// function because Tiger's TS-C02 does not see a Wait that follows Go inside a function literal.
func (s *lifecycleSpy) WhileFirstPageHeld(t *testing.T, ctx context.Context, call func()) {
	t.Helper()
	s.WaitForFirstPage(t, ctx)
	var calls sync.WaitGroup
	calls.Go(call)
	clock.Sleep(requestSettle)
	s.Release()
	calls.Wait()
}

func (s *lifecycleSpy) Release() {
	s.released.Do(func() { close(s.release) })
}

func (s *lifecycleSpy) FailScans() {
	s.failScans.Store(true)
}

func (s *lifecycleSpy) StopFailingScans() {
	s.failScans.Store(false)
}

// FailScheduledWritesAfterFirstItem makes each scheduled write enqueue only its first item and
// then fail, as a backend does when a write commits partway.
func (s *lifecycleSpy) FailScheduledWritesAfterFirstItem() {
	s.failScheduled.Store(true)
}

// FailNextDeadLetterWriteAfterFirstItem makes the next dead letter write store only its first
// item, report it stored, and then fail, as a BadgerDB move split across transactions does when
// a later transaction fails.
func (s *lifecycleSpy) FailNextDeadLetterWriteAfterFirstItem() {
	s.failDeadLetter.Store(true)
}

// FailNextItemDelete makes the next lifecycle write that deletes items fail without deleting any.
func (s *lifecycleSpy) FailNextItemDelete() {
	s.failDelete.Store(true)
}

// HoldNextProducePastDeadline makes the next produce write wait until ReleaseProduce even after
// its deadline passes, and then store its items, as a backend does when a write it already sent
// commits late. With FailNextDeadLetterWriteAfterFirstItem also set, the late write stores only
// its first item and then fails.
func (s *lifecycleSpy) HoldNextProducePastDeadline() {
	s.produceHeld = make(chan struct{})
	s.produceRelease = make(chan struct{})
	s.holdProduce.Store(true)
}

// WaitForProduceHeld returns once the held produce write has started.
func (s *lifecycleSpy) WaitForProduceHeld(t *testing.T, ctx context.Context) {
	t.Helper()
	select {
	case <-s.produceHeld:
	case <-ctx.Done():
		require.FailNow(t, "the daemon stopped before a produce write was held")
	case <-clock.After(holdWait):
		require.FailNowf(t, "no produce write was held", "no produce write started within %s", holdWait)
	}
}

func (s *lifecycleSpy) awaitRelease(ctx context.Context) error {
	select {
	case <-s.produceRelease:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *lifecycleSpy) ReleaseProduce() {
	close(s.produceRelease)
}

func (s *lifecycleSpy) Events() []string {
	defer s.mu.Unlock()
	s.mu.Lock()
	return append([]string(nil), s.events...)
}

func (s *lifecycleSpy) ActionCounts() []int {
	defer s.mu.Unlock()
	s.mu.Lock()
	return append([]int(nil), s.actions...)
}

func (s *lifecycleSpy) Pages() int {
	defer s.mu.Unlock()
	s.mu.Lock()
	return s.pages
}

func (s *lifecycleSpy) beforePage(ctx context.Context, scan *types.LifeCycleScan) {
	s.mu.Lock()
	if !s.armed {
		s.mu.Unlock()
		return
	}
	s.pages++
	first := s.pages == 1
	if scan.MidPass() {
		s.events = append(s.events, "page:resume")
	} else {
		s.events = append(s.events, "page:start")
	}
	s.mu.Unlock()
	if first {
		close(s.reached)
		select {
		case <-s.release:
		case <-ctx.Done():
		}
	}
}

func (s *lifecycleSpy) afterPage(scan *types.LifeCycleScan) {
	defer s.mu.Unlock()
	s.mu.Lock()
	if s.armed {
		s.actions = append(s.actions, len(scan.Actions))
	}
}

func (s *lifecycleSpy) produce() {
	s.record("produce")
}

func (s *lifecycleSpy) record(event string) {
	defer s.mu.Unlock()
	s.mu.Lock()
	if s.armed {
		s.events = append(s.events, event)
	}
}

type spyStore struct {
	store.PartitionStore
	spy *lifecycleSpy
}

func (s spyStore) Get(info types.PartitionInfo) store.Partition {
	return spyPartition{Partition: s.PartitionStore.Get(info), spy: s.spy}
}

type spyPartition struct {
	store.Partition
	spy *lifecycleSpy
}

func (p spyPartition) ScanForActions(ctx context.Context, scan *types.LifeCycleScan, now clock.Time) error {
	if p.spy.failScans.Load() {
		return errors.New("scan failed by test")
	}
	if p.spy.watchScheduled {
		return p.Partition.ScanForActions(ctx, scan, now)
	}
	p.spy.beforePage(ctx, scan)
	if err := p.Partition.ScanForActions(ctx, scan, now); err != nil {
		return err
	}
	p.spy.afterPage(scan)
	return nil
}

func (p spyPartition) Produce(ctx context.Context, batch types.ProduceBatch, now clock.Time) error {
	p.spy.produce()
	if p.spy.holdProduce.CompareAndSwap(true, false) {
		close(p.spy.produceHeld)
		late, cancel := context.WithTimeout(context.WithoutCancel(ctx), holdWait)
		defer cancel()
		if err := p.spy.awaitRelease(late); err != nil {
			return err
		}
		ctx = late
	}
	if len(batch.Requests) == 0 || !batch.Requests[0].DeadLetterHandOff || !p.spy.failDeadLetter.CompareAndSwap(true, false) {
		return p.Partition.Produce(ctx, batch, now)
	}
	first := *batch.Requests[0]
	first.Items = first.Items[:1]
	if err := p.Partition.Produce(ctx, types.ProduceBatch{Requests: []*types.ProduceRequest{&first}}, now); err != nil {
		return err
	}
	batch.Requests[0].Stored = 1
	return errors.New("dead letter write failed by test after its first item")
}

func (p spyPartition) TakeAction(ctx context.Context, batch types.LifeCycleBatch, state *types.PartitionState) error {
	if len(batch.Requests) != 0 && len(batch.Requests[0].Actions) != 0 &&
		batch.Requests[0].Actions[0].Action == types.ActionDeleteItem && p.spy.failDelete.CompareAndSwap(true, false) {
		return errors.New("item delete failed by test")
	}
	if !p.spy.failScheduled.Load() || len(batch.Requests) == 0 || len(batch.Requests[0].Actions) == 0 ||
		batch.Requests[0].Actions[0].Action != types.ActionQueueScheduledItem {
		return p.Partition.TakeAction(ctx, batch, state)
	}
	first := *batch.Requests[0]
	first.Actions = first.Actions[:1]
	if err := p.Partition.TakeAction(ctx, types.LifeCycleBatch{Requests: []*types.LifeCycleRequest{&first}}, state); err != nil {
		return err
	}
	return errors.New("scheduled write failed by test after its first item")
}

func (p spyPartition) ScanForScheduled(ctx context.Context, scan *types.LifeCycleScan, now clock.Time) error {
	if !p.spy.watchScheduled {
		return p.Partition.ScanForScheduled(ctx, scan, now)
	}
	p.spy.beforePage(ctx, scan)
	if err := p.Partition.ScanForScheduled(ctx, scan, now); err != nil {
		return err
	}
	p.spy.afterPage(scan)
	return nil
}

func (p spyPartition) Complete(ctx context.Context, batch types.CompleteBatch) error {
	if len(batch.Requests) != 0 {
		p.spy.record("complete")
	}
	return p.Partition.Complete(ctx, batch)
}

func (p spyPartition) Retry(ctx context.Context, batch types.RetryBatch) error {
	if len(batch.Requests) != 0 {
		p.spy.record("retry")
	}
	return p.Partition.Retry(ctx, batch)
}
