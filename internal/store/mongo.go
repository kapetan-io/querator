package store

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kapetan-io/errors"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport/reply"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/set"
	"github.com/segmentio/ksuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// The MongoDB backend is non-transactional and standalone-compatible (see ADR-0026): it uses no
// multi-document transactions, requires no replica set, and upholds querator's correctness contracts
// through single-document atomic operations, insert-before-delete ordering, and the single-writer-per
// -partition guarantee (ADR-0003/0009).
//
// Time precision: BSON Date is millisecond precision while Postgres TIMESTAMPTZ (and the conformance
// suite) work at microsecond precision. To remain bit-stable with the existing functional suite
// (storage_test CRUDCompare truncates to microsecond; compareStorageItem requires exact round-trip
// equality) item timestamps are stored as int64 microseconds-since-epoch rather than BSON Date. The
// partial-filter index design from the tech spec is unaffected: it relies on equality and $exists,
// both of which behave identically on an int64 field.

// ---------------------------------------------
// Global Client Manager
// ---------------------------------------------

// mongo.Client is itself a connection pool and is safe for concurrent use. A process-global manager
// keyed by connection string shares one client across all partitions/queues pointed at the same
// server, ref-counted for shutdown — a direct port of postgresPoolManager.
type mongoClientManager struct {
	client   *mongo.Client
	refCount atomic.Int32
}

var (
	globalMongoMu      sync.Mutex
	globalMongoClients = make(map[string]*mongoClientManager) // keyed by ConnectionString
)

func acquireClient(uri string, maxPool uint64, log *slog.Logger) (*mongo.Client, error) {
	globalMongoMu.Lock()
	defer globalMongoMu.Unlock()

	if manager, exists := globalMongoClients[uri]; exists {
		manager.refCount.Add(1)
		return manager.client, nil
	}

	opts := options.Client().ApplyURI(uri)
	if maxPool > 0 {
		opts.SetMaxPoolSize(maxPool)
	}

	var client *mongo.Client
	var err error
	delays := []time.Duration{0, 100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond}

	for attempt, delay := range delays {
		if delay > 0 {
			time.Sleep(delay)
		}

		// Connect is lazy and does not contact the server (ADR-0021); the connection is
		// established on first operation.
		client, err = mongo.Connect(context.Background(), opts)
		if err == nil {
			break
		}

		if attempt < len(delays)-1 && log != nil {
			log.Warn("failed to create mongo client, retrying",
				"attempt", attempt+1,
				"error", err,
				"next_delay", delays[attempt+1])
		}
	}

	if err != nil {
		return nil, errors.Errorf("create mongo client after retries: %w", err)
	}

	manager := &mongoClientManager{client: client}
	manager.refCount.Store(1)
	globalMongoClients[uri] = manager

	return client, nil
}

func releaseClient(uri string) {
	globalMongoMu.Lock()
	defer globalMongoMu.Unlock()

	manager, exists := globalMongoClients[uri]
	if !exists {
		return
	}

	if manager.refCount.Add(-1) == 0 {
		_ = manager.client.Disconnect(context.Background())
		delete(globalMongoClients, uri)
	}
}

// ---------------------------------------------
// MongoDB Configuration
// ---------------------------------------------

type MongoConfig struct {
	// ConnectionString is a MongoDB connection URI, e.g. "mongodb://user:pass@host:27017".
	ConnectionString string
	// Database is the logical database name within the Mongo server. Defaults to "querator".
	Database string
	// MaxPoolSize caps the driver connection pool per client. 0 = driver default.
	MaxPoolSize uint64
	// ScanBatchSize is the page size for ScanForActions/ScanForScheduled. Default 1000.
	ScanBatchSize int
	// Log defaults to slog.Default().
	Log *slog.Logger
	// OnQueryComplete is an optional observability hook invoked after operations complete.
	OnQueryComplete func(operation string, duration time.Duration, err error)

	// private: shared-client bookkeeping (mirrors PostgresConfig.connString/poolAcquired)
	connString     string
	clientAcquired bool
}

func (c *MongoConfig) database() string {
	if c.Database == "" {
		return "querator"
	}
	return c.Database
}

func (c *MongoConfig) getOrCreateClient(_ context.Context) (*mongo.Client, error) {
	if c.ConnectionString == "" {
		return nil, errors.New("connection string is required")
	}

	if c.connString == "" {
		c.connString = c.ConnectionString
	}

	// Only acquire the client once per config instance
	if !c.clientAcquired {
		client, err := acquireClient(c.ConnectionString, c.MaxPoolSize, c.Log)
		if err != nil {
			return nil, err
		}
		c.clientAcquired = true
		return client, nil
	}

	// Return the existing client without incrementing refCount
	globalMongoMu.Lock()
	defer globalMongoMu.Unlock()

	manager, exists := globalMongoClients[c.connString]
	if !exists {
		return nil, errors.New("client was released")
	}

	return manager.client, nil
}

func (c *MongoConfig) Close() {
	if c.connString != "" && c.clientAcquired {
		releaseClient(c.connString)
		c.clientAcquired = false
	}
}

func (c *MongoConfig) Ping(ctx context.Context) error {
	client, err := c.getOrCreateClient(ctx)
	if err != nil {
		return err
	}
	return client.Ping(ctx, nil)
}

// ---------------------------------------------
// Time helpers
// ---------------------------------------------

// toMicros truncates a timestamp to microsecond precision and returns it as an int64 number of
// microseconds since the Unix epoch. This matches the precision Postgres uses (TIMESTAMPTZ) and the
// precision the conformance suite asserts against.
func toMicros(t clock.Time) int64 {
	return t.UnixMicro()
}

// fromMicros converts an int64 microsecond timestamp back into a UTC clock.Time.
func fromMicros(v int64) clock.Time {
	return time.UnixMicro(v).UTC()
}

// normMicros normalizes a time to the same microsecond/UTC representation a round-trip through the
// store would produce, so returned items compare equal to items read back via List.
func normMicros(t clock.Time) clock.Time {
	return fromMicros(toMicros(t))
}

// ---------------------------------------------
// Item document (BSON)
// ---------------------------------------------

type mongoItem struct {
	ID             string  `bson:"_id"`
	SourceID       *string `bson:"source_id,omitempty"`
	IsLeased       bool    `bson:"is_leased"`
	LeaseDeadline  *int64  `bson:"lease_deadline"`
	ExpireDeadline int64   `bson:"expire_deadline"`
	EnqueueAt      *int64  `bson:"enqueue_at,omitempty"`
	CreatedAt      int64   `bson:"created_at"`
	Attempts       int32   `bson:"attempts"`
	MaxAttempts    int32   `bson:"max_attempts"`
	Reference      string  `bson:"reference"`
	Encoding       string  `bson:"encoding"`
	Kind           string  `bson:"kind"`
	Payload        []byte  `bson:"payload"`
}

func (d *mongoItem) toItem() *types.Item {
	item := &types.Item{
		ID:             types.ItemID(d.ID),
		IsLeased:       d.IsLeased,
		ExpireDeadline: fromMicros(d.ExpireDeadline),
		CreatedAt:      fromMicros(d.CreatedAt),
		Attempts:       int(d.Attempts),
		MaxAttempts:    int(d.MaxAttempts),
		Reference:      d.Reference,
		Encoding:       d.Encoding,
		Kind:           d.Kind,
		Payload:        d.Payload,
	}
	if d.SourceID != nil {
		item.SourceID = types.ItemID(*d.SourceID)
	}
	if d.LeaseDeadline != nil {
		item.LeaseDeadline = fromMicros(*d.LeaseDeadline)
	}
	if d.EnqueueAt != nil {
		item.EnqueueAt = fromMicros(*d.EnqueueAt)
	}
	return item
}

// itemToDoc builds the BSON document for an item, applying the field-representation conventions:
// source_id is omitted when nil, enqueue_at is omitted when the item is not scheduled, and
// lease_deadline is BSON null when the item carries no lease deadline.
func itemToDoc(id string, item *types.Item) bson.M {
	doc := bson.M{
		"_id":             id,
		"is_leased":       item.IsLeased,
		"expire_deadline": toMicros(item.ExpireDeadline),
		"created_at":      toMicros(item.CreatedAt),
		"attempts":        int32(item.Attempts),
		"max_attempts":    int32(item.MaxAttempts),
		"reference":       item.Reference,
		"encoding":        item.Encoding,
		"kind":            item.Kind,
		"payload":         item.Payload,
	}
	if item.SourceID != nil {
		doc["source_id"] = string(item.SourceID)
	}
	if item.LeaseDeadline.IsZero() {
		doc["lease_deadline"] = nil
	} else {
		doc["lease_deadline"] = toMicros(item.LeaseDeadline)
	}
	if !item.EnqueueAt.IsZero() {
		doc["enqueue_at"] = toMicros(item.EnqueueAt)
	}
	return doc
}

// ---------------------------------------------
// MongoDB Queues Implementation
// ---------------------------------------------

type MongoQueues struct {
	QueuesValidation
	conf MongoConfig
}

var _ Queues = &MongoQueues{}

func NewMongoQueues(conf MongoConfig) *MongoQueues {
	set.Default(&conf.Log, slog.Default())
	set.Default(&conf.ScanBatchSize, 1000)
	set.Default(&conf.Database, "querator")
	return &MongoQueues{conf: conf}
}

func (q *MongoQueues) Config() MongoConfig {
	return q.conf
}

func (q *MongoQueues) collection(client *mongo.Client) *mongo.Collection {
	return client.Database(q.conf.database()).Collection("queues")
}

type queueDoc struct {
	Name                string `bson:"_id"`
	Namespace           string `bson:"namespace"`
	LeaseTimeoutNs      int64  `bson:"lease_timeout_ns"`
	ExpireTimeoutNs     int64  `bson:"expire_timeout_ns"`
	DeadQueue           string `bson:"dead_queue"`
	MaxAttempts         int    `bson:"max_attempts"`
	Reference           string `bson:"reference"`
	RequestedPartitions int    `bson:"requested_partitions"`
	PartitionInfoJSON   []byte `bson:"partition_info"`
	CreatedAt           int64  `bson:"created_at"`
	UpdatedAt           int64  `bson:"updated_at"`
}

func queueToDoc(info types.QueueInfo) (bson.M, error) {
	partitionInfoJSON, err := json.Marshal(info.PartitionInfo)
	if err != nil {
		return nil, errors.Errorf("marshal partition_info: %w", err)
	}
	return bson.M{
		"_id":                  info.Name,
		"namespace":            info.Namespace,
		"lease_timeout_ns":     info.LeaseTimeout.Nanoseconds(),
		"expire_timeout_ns":    info.ExpireTimeout.Nanoseconds(),
		"dead_queue":           info.DeadQueue,
		"max_attempts":         info.MaxAttempts,
		"reference":            info.Reference,
		"requested_partitions": info.RequestedPartitions,
		"partition_info":       partitionInfoJSON,
		"created_at":           toMicros(info.CreatedAt),
		"updated_at":           toMicros(info.UpdatedAt),
	}, nil
}

func (d *queueDoc) toInfo(info *types.QueueInfo) error {
	info.Name = d.Name
	info.Namespace = d.Namespace
	info.LeaseTimeout = clock.Duration(d.LeaseTimeoutNs)
	info.ExpireTimeout = clock.Duration(d.ExpireTimeoutNs)
	info.DeadQueue = d.DeadQueue
	info.MaxAttempts = d.MaxAttempts
	info.Reference = d.Reference
	info.RequestedPartitions = d.RequestedPartitions
	info.CreatedAt = fromMicros(d.CreatedAt)
	info.UpdatedAt = fromMicros(d.UpdatedAt)
	if len(d.PartitionInfoJSON) > 0 {
		if err := json.Unmarshal(d.PartitionInfoJSON, &info.PartitionInfo); err != nil {
			return errors.Errorf("unmarshal partition_info: %w", err)
		}
	}
	return nil
}

func (q *MongoQueues) Get(ctx context.Context, name string, queue *types.QueueInfo) error {
	if err := q.validateGet(name); err != nil {
		return err
	}

	client, err := q.conf.getOrCreateClient(ctx)
	if err != nil {
		return err
	}

	var doc queueDoc
	err = q.collection(client).FindOne(ctx, bson.M{"_id": name}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return ErrQueueNotExist
		}
		return errors.Errorf("query queue: %w", err)
	}

	return doc.toInfo(queue)
}

func (q *MongoQueues) Add(ctx context.Context, info types.QueueInfo) error {
	if err := q.validateAdd(info); err != nil {
		return err
	}

	client, err := q.conf.getOrCreateClient(ctx)
	if err != nil {
		return err
	}

	doc, err := queueToDoc(info)
	if err != nil {
		return err
	}

	_, err = q.collection(client).InsertOne(ctx, doc)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return reply.NewInvalidOption("queue '%s' already exists", info.Name)
		}
		return errors.Errorf("insert queue: %w", err)
	}
	return nil
}

func (q *MongoQueues) Update(ctx context.Context, info types.QueueInfo) error {
	if err := q.validateQueueName(info); err != nil {
		return err
	}

	client, err := q.conf.getOrCreateClient(ctx)
	if err != nil {
		return err
	}

	var found types.QueueInfo
	if err := q.Get(ctx, info.Name, &found); err != nil {
		return err
	}

	found.Update(info)

	if err := q.validateUpdate(found); err != nil {
		return err
	}

	if found.LeaseTimeout > found.ExpireTimeout {
		return reply.NewInvalidOption("lease timeout is too long; %s cannot be greater than the "+
			"expire timeout %s", found.LeaseTimeout.String(), found.ExpireTimeout.String())
	}

	partitionInfoJSON, err := json.Marshal(found.PartitionInfo)
	if err != nil {
		return errors.Errorf("marshal partition_info: %w", err)
	}

	_, err = q.collection(client).UpdateOne(ctx,
		bson.M{"_id": found.Name},
		bson.M{"$set": bson.M{
			"lease_timeout_ns":  found.LeaseTimeout.Nanoseconds(),
			"expire_timeout_ns": found.ExpireTimeout.Nanoseconds(),
			"dead_queue":        found.DeadQueue,
			"max_attempts":      found.MaxAttempts,
			"reference":         found.Reference,
			"partition_info":    partitionInfoJSON,
			"updated_at":        toMicros(found.UpdatedAt),
		}})
	if err != nil {
		return errors.Errorf("update queue: %w", err)
	}
	return nil
}

func (q *MongoQueues) List(ctx context.Context, queues *[]types.QueueInfo, opts types.ListOptions) error {
	if err := q.validateList(opts); err != nil {
		return err
	}

	client, err := q.conf.getOrCreateClient(ctx)
	if err != nil {
		return err
	}

	pivot := ""
	if opts.Pivot != nil {
		pivot = string(opts.Pivot)
	}

	cur, err := q.collection(client).Find(ctx,
		bson.M{"_id": bson.M{"$gte": pivot}},
		options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetLimit(int64(opts.Limit)))
	if err != nil {
		return errors.Errorf("query queues: %w", err)
	}
	defer func() { _ = cur.Close(ctx) }()

	for cur.Next(ctx) {
		var doc queueDoc
		if err := cur.Decode(&doc); err != nil {
			return errors.Errorf("decode queue: %w", err)
		}

		var info types.QueueInfo
		if err := doc.toInfo(&info); err != nil {
			return err
		}

		if opts.Namespace != "" && info.Namespace != opts.Namespace {
			continue
		}
		*queues = append(*queues, info)
	}

	return cur.Err()
}

func (q *MongoQueues) Delete(ctx context.Context, name string) error {
	if err := q.validateDelete(name); err != nil {
		return err
	}

	client, err := q.conf.getOrCreateClient(ctx)
	if err != nil {
		return err
	}

	_, err = q.collection(client).DeleteOne(ctx, bson.M{"_id": name})
	if err != nil {
		return errors.Errorf("delete queue: %w", err)
	}
	return nil
}

func (q *MongoQueues) Close(_ context.Context) error {
	q.conf.Close()
	return nil
}

// ---------------------------------------------
// MongoDB Partition Store Implementation
// ---------------------------------------------

type MongoPartitionStore struct {
	conf MongoConfig
}

var _ PartitionStore = &MongoPartitionStore{}

func NewMongoPartitionStore(conf MongoConfig) *MongoPartitionStore {
	set.Default(&conf.Log, slog.Default())
	set.Default(&conf.ScanBatchSize, 1000)
	set.Default(&conf.Database, "querator")
	return &MongoPartitionStore{conf: conf}
}

func (s *MongoPartitionStore) Config() MongoConfig {
	return s.conf
}

func (s *MongoPartitionStore) Get(info types.PartitionInfo) Partition {
	return &MongoPartition{
		uid:  ksuid.New(),
		conf: s.conf,
		info: info,
	}
}

// ---------------------------------------------
// MongoDB Partition Implementation
// ---------------------------------------------

type MongoPartition struct {
	info     types.PartitionInfo
	conf     MongoConfig
	mu       sync.RWMutex
	uid      ksuid.KSUID
	initMu   sync.Mutex
	initDone bool
	initErr  error
}

var _ Partition = &MongoPartition{}

func (p *MongoPartition) collectionName() string {
	return fmt.Sprintf("items_%s_%d", hashQueueName(p.info.Queue.Name), p.info.PartitionNum)
}

func (p *MongoPartition) collection(client *mongo.Client) *mongo.Collection {
	return client.Database(p.conf.database()).Collection(p.collectionName())
}

// nextID advances the KSUID seed under mu and returns the lexicographic successor, preserving FIFO
// order (insertion order == _id order, see ADR-0014).
func (p *MongoPartition) nextID() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.uid = p.uid.Next()
	return p.uid.String()
}

// ensureCollection creates the collection (implicitly) and its indexes once per MongoPartition,
// caching the result (ADR-0021). Clear with a destructive whole-queue request resets the guard.
func (p *MongoPartition) ensureCollection(_ context.Context, client *mongo.Client) error {
	p.initMu.Lock()
	defer p.initMu.Unlock()
	if p.initDone {
		return p.initErr
	}
	bgCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	p.initErr = p.createIndexes(bgCtx, client)
	p.initDone = true
	return p.initErr
}

func (p *MongoPartition) resetCollectionGuard() {
	p.initMu.Lock()
	p.initDone = false
	p.initErr = nil
	p.initMu.Unlock()
}

func (p *MongoPartition) createIndexes(ctx context.Context, client *mongo.Client) error {
	if p.conf.Log != nil {
		p.conf.Log.Debug("Creating MongoDB collection and indexes",
			"collection", p.collectionName(), "queue", p.info.Queue.Name, "partition", p.info.PartitionNum)
	}

	// MongoDB partialFilterExpression supports only equality, $exists:true, comparison, $type and
	// $and of these. The field-representation conventions are chosen so the mappable predicates use
	// equality / $exists:true; the remaining predicate is applied as a residual query filter.
	indexes := []mongo.IndexModel{
		{
			// Lease selection: query adds residual enqueue_at:{$exists:false} and sorts _id.
			Keys:    bson.D{{Key: "is_leased", Value: 1}, {Key: "_id", Value: 1}},
			Options: options.Index().SetName("lease").SetPartialFilterExpression(bson.M{"is_leased": false}),
		},
		{
			// Scheduled: only scheduled items carry enqueue_at.
			Keys:    bson.D{{Key: "enqueue_at", Value: 1}},
			Options: options.Index().SetName("sched").SetPartialFilterExpression(bson.M{"enqueue_at": bson.M{"$exists": true}}),
		},
		{
			// Lease expiry: equality on is_leased.
			Keys:    bson.D{{Key: "lease_deadline", Value: 1}},
			Options: options.Index().SetName("deadline").SetPartialFilterExpression(bson.M{"is_leased": true}),
		},
		{
			// Item expiry: plain index.
			Keys:    bson.D{{Key: "expire_deadline", Value: 1}},
			Options: options.Index().SetName("expire"),
		},
		{
			// Dedup lookup: NON-unique (see Correctness / ADR-0026).
			Keys:    bson.D{{Key: "source_id", Value: 1}},
			Options: options.Index().SetName("src").SetPartialFilterExpression(bson.M{"source_id": bson.M{"$exists": true}}),
		},
	}

	if _, err := p.collection(client).Indexes().CreateMany(ctx, indexes); err != nil {
		if p.conf.Log != nil {
			p.conf.Log.Error("Failed to create MongoDB indexes", "error", err, "collection", p.collectionName())
		}
		return errors.Errorf("create indexes: %w", err)
	}
	return nil
}

func (p *MongoPartition) validateID(id []byte) error {
	_, err := ksuid.Parse(string(id))
	return err
}

func (p *MongoPartition) Produce(ctx context.Context, batch types.ProduceBatch, now clock.Time) error {
	client, err := p.conf.getOrCreateClient(ctx)
	if err != nil {
		return err
	}

	if err := p.ensureCollection(ctx, client); err != nil {
		return err
	}

	coll := p.collection(client)
	var docs []interface{}
	seen := make(map[string]struct{})

	for i := range batch.Requests {
		for _, item := range batch.Requests[i].Items {
			item.ID = []byte(p.nextID())
			item.CreatedAt = now

			if item.EnqueueAt.Before(now.Add(time.Millisecond * 100)) {
				item.EnqueueAt = clock.Time{}
			}

			// Normalize the returned item to the microsecond/UTC representation a read-back produces.
			item.ExpireDeadline = normMicros(item.ExpireDeadline)
			item.CreatedAt = normMicros(item.CreatedAt)
			if !item.LeaseDeadline.IsZero() {
				item.LeaseDeadline = normMicros(item.LeaseDeadline)
			}
			if !item.EnqueueAt.IsZero() {
				item.EnqueueAt = normMicros(item.EnqueueAt)
			}

			// Application-level dedup (check-before-insert), mirroring InMemory/Badger. A Partition is
			// driven by a single logical-queue goroutine (ADR-0003/0009) so this is race-free.
			if item.SourceID != nil {
				sid := string(item.SourceID)
				if _, ok := seen[sid]; ok {
					continue
				}
				seen[sid] = struct{}{}
				exists, err := p.sourceIDExists(ctx, coll, sid)
				if err != nil {
					return err
				}
				if exists {
					continue
				}
			}

			docs = append(docs, itemToDoc(string(item.ID), item))
		}
	}

	if len(docs) == 0 {
		return nil
	}

	if _, err := coll.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false)); err != nil {
		return errors.Errorf("failed to insert items: %w", err)
	}
	return nil
}

func (p *MongoPartition) sourceIDExists(ctx context.Context, coll *mongo.Collection, sourceID string) (bool, error) {
	err := coll.FindOne(ctx, bson.M{"source_id": sourceID},
		options.FindOne().SetProjection(bson.M{"_id": 1})).Err()
	if err == nil {
		return true, nil
	}
	if errors.Is(err, mongo.ErrNoDocuments) {
		return false, nil
	}
	return false, errors.Errorf("dedup lookup: %w", err)
}

func (p *MongoPartition) Lease(ctx context.Context, batch types.LeaseBatch, opts LeaseOptions) error {
	client, err := p.conf.getOrCreateClient(ctx)
	if err != nil {
		return err
	}

	if err := p.ensureCollection(ctx, client); err != nil {
		return err
	}

	// MongoDB treats SetLimit(0) as "no limit" (unlike SQL LIMIT 0), so guard the empty batch to avoid
	// scanning the whole collection.
	if batch.TotalRequested == 0 {
		return nil
	}

	coll := p.collection(client)

	// Candidate select: unleased and not scheduled, FIFO order, limited to the requested total.
	cur, err := coll.Find(ctx,
		bson.M{"is_leased": false, "enqueue_at": bson.M{"$exists": false}},
		options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetLimit(int64(batch.TotalRequested)))
	if err != nil {
		return errors.Errorf("query items for lease: %w", err)
	}

	var docs []mongoItem
	if err := cur.All(ctx, &docs); err != nil {
		return errors.Errorf("decode lease candidates: %w", err)
	}

	if len(docs) == 0 {
		return nil
	}

	batchIter := batch.Iterator()
	var models []mongo.WriteModel

	for i := range docs {
		item := *docs[i].toItem()
		item.LeaseDeadline = opts.LeaseDeadline
		item.IsLeased = true
		item.Attempts++

		itemPtr := new(types.Item)
		*itemPtr = item

		if !batchIter.Next(itemPtr) {
			break
		}

		// Conditional claim: filtered on the unleased state so a lost race (should the single-writer
		// invariant ever break) claims zero documents. This is the standalone-Mongo equivalent of
		// Postgres SELECT ... FOR UPDATE SKIP LOCKED.
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.M{"_id": string(item.ID), "is_leased": false}).
			SetUpdate(bson.M{
				"$set": bson.M{"is_leased": true, "lease_deadline": toMicros(opts.LeaseDeadline)},
				"$inc": bson.M{"attempts": int32(1)},
			}))
	}

	if len(models) == 0 {
		return nil
	}

	if _, err := coll.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false)); err != nil {
		return errors.Errorf("update leased items: %w", err)
	}
	return nil
}

func (p *MongoPartition) Complete(ctx context.Context, batch types.CompleteBatch) error {
	client, err := p.conf.getOrCreateClient(ctx)
	if err != nil {
		return err
	}

	if err := p.ensureCollection(ctx, client); err != nil {
		return err
	}

	coll := p.collection(client)

nextBatch:
	for i := range batch.Requests {
		for _, id := range batch.Requests[i].Ids {
			if err := p.validateID(id); err != nil {
				batch.Requests[i].Err = reply.NewInvalidOption("invalid storage id; '%s': %s", id, err)
				continue nextBatch
			}

			// FindOneAndDelete atomically checks is_leased and deletes in one round trip.
			// On success (err == nil) the item was leased and is now deleted.
			// On ErrNoDocuments the item was either missing or present-but-not-leased;
			// a cheap follow-up projection read disambiguates for the error path only.
			res := coll.FindOneAndDelete(ctx, bson.M{"_id": string(id), "is_leased": true})
			switch err := res.Err(); {
			case err == nil:
				// deleted atomically; success — proceed to next id
			case errors.Is(err, mongo.ErrNoDocuments):
				// Ambiguous: missing OR present-but-unleased. One cheap read to distinguish.
				e := coll.FindOne(ctx, bson.M{"_id": string(id)},
					options.FindOne().SetProjection(bson.M{"_id": 1})).Err()
				if errors.Is(e, mongo.ErrNoDocuments) {
					batch.Requests[i].Err = reply.NewInvalidOption("invalid storage id; '%s' does not exist", id)
					continue nextBatch
				} else if e != nil {
					return errors.Errorf("mongo partition %s/%d: failed to check item: %w",
						p.info.Queue.Name, p.info.PartitionNum, e)
				}
				batch.Requests[i].Err = reply.NewConflict("item(s) cannot be completed; '%s' is not marked as leased", id)
				continue nextBatch
			default:
				return errors.Errorf("mongo partition %s/%d: failed to delete item: %w",
					p.info.Queue.Name, p.info.PartitionNum, err)
			}
		}
	}
	return nil
}

func (p *MongoPartition) Retry(ctx context.Context, batch types.RetryBatch) error {
	client, err := p.conf.getOrCreateClient(ctx)
	if err != nil {
		return err
	}

	if err := p.ensureCollection(ctx, client); err != nil {
		return err
	}

	coll := p.collection(client)

nextBatch:
	for i := range batch.Requests {
		for _, retryItem := range batch.Requests[i].Items {
			if err := p.validateID(retryItem.ID); err != nil {
				batch.Requests[i].Err = reply.NewInvalidOption("invalid storage id; '%s': %s", retryItem.ID, err)
				continue nextBatch
			}

			var doc mongoItem
			err := coll.FindOne(ctx, bson.M{"_id": string(retryItem.ID)}).Decode(&doc)
			if errors.Is(err, mongo.ErrNoDocuments) {
				batch.Requests[i].Err = reply.NewInvalidOption("invalid storage id; '%s' does not exist", retryItem.ID)
				continue nextBatch
			}
			if err != nil {
				return errors.Errorf("failed to check lease status: %w", err)
			}

			item := doc.toItem()

			if !item.EnqueueAt.IsZero() {
				if p.conf.Log != nil {
					p.conf.Log.LogAttrs(ctx, slog.LevelWarn, "attempted to retry a scheduled item; reported does not exist",
						slog.String("id", string(retryItem.ID)))
				}
				batch.Requests[i].Err = reply.NewInvalidOption("invalid storage id; '%s' does not exist", retryItem.ID)
				continue nextBatch
			}

			if !item.IsLeased {
				batch.Requests[i].Err = reply.NewConflict("item(s) cannot be retried; '%s' is not marked as leased", retryItem.ID)
				continue nextBatch
			}

			switch {
			case retryItem.Dead:
				if _, err := coll.DeleteOne(ctx, bson.M{"_id": string(retryItem.ID)}); err != nil {
					return errors.Errorf("during Retry() delete dead: %w", err)
				}
			case !retryItem.RetryAt.IsZero() && !retryItem.RetryAt.Before(clock.Now().UTC().Add(time.Millisecond*100)):
				// Schedule for future retry — position is irrelevant while scheduled.
				if _, err := coll.UpdateOne(ctx, bson.M{"_id": string(retryItem.ID)},
					bson.M{"$set": bson.M{
						"is_leased":      false,
						"lease_deadline": nil,
						"enqueue_at":     toMicros(retryItem.RetryAt),
					}}); err != nil {
					return errors.Errorf("during Retry() schedule: %w", err)
				}
			default:
				// Immediate retry: assign a new KSUID and place at the tail (ADR-0022). Insert the new
				// tail document first, then delete the old (ADR-0026 — a crash degrades to a duplicate,
				// never a loss).
				newItem := &types.Item{
					IsLeased:       false,
					ExpireDeadline: item.ExpireDeadline,
					CreatedAt:      item.CreatedAt,
					Attempts:       item.Attempts,
					MaxAttempts:    item.MaxAttempts,
					Reference:      item.Reference,
					Encoding:       item.Encoding,
					Kind:           item.Kind,
					Payload:        item.Payload,
					SourceID:       item.SourceID,
				}
				newID := p.nextID()
				if _, err := coll.InsertOne(ctx, itemToDoc(newID, newItem)); err != nil {
					return errors.Errorf("during Retry() insert at tail: %w", err)
				}
				if _, err := coll.DeleteOne(ctx, bson.M{"_id": string(retryItem.ID)}); err != nil {
					return errors.Errorf("during Retry() delete for tail placement: %w", err)
				}
			}
		}
	}
	return nil
}

func (p *MongoPartition) List(ctx context.Context, items *[]*types.Item, opts types.ListOptions) error {
	client, err := p.conf.getOrCreateClient(ctx)
	if err != nil {
		return err
	}

	if err := p.ensureCollection(ctx, client); err != nil {
		return err
	}

	pivot, err := p.pivot(opts)
	if err != nil {
		return err
	}

	cur, err := p.collection(client).Find(ctx,
		bson.M{"enqueue_at": bson.M{"$exists": false}, "_id": bson.M{"$gte": pivot}},
		options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetLimit(int64(opts.Limit)))
	if err != nil {
		return errors.Errorf("query items: %w", err)
	}
	return p.appendItems(ctx, cur, items)
}

func (p *MongoPartition) ListScheduled(ctx context.Context, items *[]*types.Item, opts types.ListOptions) error {
	client, err := p.conf.getOrCreateClient(ctx)
	if err != nil {
		return err
	}

	if err := p.ensureCollection(ctx, client); err != nil {
		return err
	}

	pivot, err := p.pivot(opts)
	if err != nil {
		return err
	}

	cur, err := p.collection(client).Find(ctx,
		bson.M{"enqueue_at": bson.M{"$exists": true}, "_id": bson.M{"$gte": pivot}},
		options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetLimit(int64(opts.Limit)))
	if err != nil {
		return errors.Errorf("query scheduled items: %w", err)
	}
	return p.appendItems(ctx, cur, items)
}

func (p *MongoPartition) pivot(opts types.ListOptions) (string, error) {
	if opts.Pivot != nil {
		if err := p.validateID(opts.Pivot); err != nil {
			return "", reply.NewInvalidOption("invalid storage id; '%s': %s", opts.Pivot, err)
		}
		return string(opts.Pivot), nil
	}
	return "", nil
}

func (p *MongoPartition) appendItems(ctx context.Context, cur *mongo.Cursor, items *[]*types.Item) error {
	defer func() { _ = cur.Close(ctx) }()
	for cur.Next(ctx) {
		var doc mongoItem
		if err := cur.Decode(&doc); err != nil {
			return errors.Errorf("decode item: %w", err)
		}
		*items = append(*items, doc.toItem())
	}
	return cur.Err()
}

func (p *MongoPartition) Add(ctx context.Context, items []*types.Item, now clock.Time) error {
	if len(items) == 0 {
		return reply.NewInvalidOption("items is invalid; cannot be empty")
	}

	client, err := p.conf.getOrCreateClient(ctx)
	if err != nil {
		return err
	}

	if err := p.ensureCollection(ctx, client); err != nil {
		return err
	}

	coll := p.collection(client)
	var docs []interface{}
	seen := make(map[string]struct{})

	for _, item := range items {
		item.ID = []byte(p.nextID())
		item.CreatedAt = now

		item.ExpireDeadline = normMicros(item.ExpireDeadline)
		item.CreatedAt = normMicros(item.CreatedAt)
		if !item.LeaseDeadline.IsZero() {
			item.LeaseDeadline = normMicros(item.LeaseDeadline)
		}
		if !item.EnqueueAt.IsZero() {
			item.EnqueueAt = normMicros(item.EnqueueAt)
		}

		if item.SourceID != nil {
			sid := string(item.SourceID)
			if _, ok := seen[sid]; ok {
				continue
			}
			seen[sid] = struct{}{}
			exists, err := p.sourceIDExists(ctx, coll, sid)
			if err != nil {
				return err
			}
			if exists {
				continue
			}
		}

		docs = append(docs, itemToDoc(string(item.ID), item))
	}

	if len(docs) == 0 {
		return nil
	}

	if _, err := coll.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false)); err != nil {
		return errors.Errorf("failed to insert items: %w", err)
	}
	return nil
}

func (p *MongoPartition) Delete(ctx context.Context, ids []types.ItemID) error {
	if len(ids) == 0 {
		return reply.NewInvalidOption("ids is invalid; cannot be empty")
	}

	client, err := p.conf.getOrCreateClient(ctx)
	if err != nil {
		return err
	}

	if err := p.ensureCollection(ctx, client); err != nil {
		return err
	}

	strIDs := make([]string, 0, len(ids))
	for _, id := range ids {
		if err := p.validateID(id); err != nil {
			return reply.NewInvalidOption("invalid storage id; '%s': %s", id, err)
		}
		strIDs = append(strIDs, string(id))
	}

	if _, err := p.collection(client).DeleteMany(ctx, bson.M{"_id": bson.M{"$in": strIDs}}); err != nil {
		return errors.Errorf("failed to delete items: %w", err)
	}
	return nil
}

func (p *MongoPartition) Clear(ctx context.Context, req types.ClearRequest) error {
	client, err := p.conf.getOrCreateClient(ctx)
	if err != nil {
		return err
	}

	if err := p.ensureCollection(ctx, client); err != nil {
		return err
	}

	coll := p.collection(client)
	now := toMicros(clock.Now().UTC())

	// Destructive whole-queue clear: Drop the collection (O(1)) and reset the lazy-init guard so the
	// collection and indexes are recreated on next use (ADR-0021).
	if req.Queue && req.Destructive && req.Scheduled {
		if err := coll.Drop(ctx); err != nil {
			return errors.Errorf("clear drop failed: %w", err)
		}
		p.resetCollectionGuard()
		return nil
	}

	var conditions []bson.M

	if req.Scheduled {
		conditions = append(conditions, bson.M{"enqueue_at": bson.M{"$exists": true}})
	}

	if req.Queue {
		ready := bson.M{"$or": bson.A{
			bson.M{"enqueue_at": bson.M{"$exists": false}},
			bson.M{"enqueue_at": bson.M{"$lte": now}},
		}}
		if req.Destructive {
			conditions = append(conditions, ready)
		} else {
			conditions = append(conditions, bson.M{"$and": bson.A{ready, bson.M{"is_leased": false}}})
		}
	}

	if len(conditions) == 0 {
		return nil
	}

	filter := conditions[0]
	if len(conditions) > 1 {
		filter = bson.M{"$or": conditions}
	}

	if _, err := coll.DeleteMany(ctx, filter); err != nil {
		return errors.Errorf("clear failed: %w", err)
	}
	return nil
}

func (p *MongoPartition) Stats(ctx context.Context, stats *types.PartitionStats, now clock.Time) error {
	client, err := p.conf.getOrCreateClient(ctx)
	if err != nil {
		return err
	}

	if err := p.ensureCollection(ctx, client); err != nil {
		return err
	}

	nowMicros := toMicros(now)
	// "active" = not scheduled or scheduled in the past (enqueue_at missing or <= now).
	active := bson.M{"$or": bson.A{
		bson.M{"$eq": bson.A{bson.M{"$type": "$enqueue_at"}, "missing"}},
		bson.M{"$lte": bson.A{"$enqueue_at", nowMicros}},
	}}
	scheduled := bson.M{"$ne": bson.A{bson.M{"$type": "$enqueue_at"}, "missing"}}
	leased := bson.M{"$eq": bson.A{"$is_leased", true}}

	pipeline := mongo.Pipeline{
		{{Key: "$group", Value: bson.M{
			"_id":         nil,
			"total":       bson.M{"$sum": bson.M{"$cond": bson.A{active, 1, 0}}},
			"num_leased":  bson.M{"$sum": bson.M{"$cond": bson.A{bson.M{"$and": bson.A{leased, active}}, 1, 0}}},
			"scheduled":   bson.M{"$sum": bson.M{"$cond": bson.A{scheduled, 1, 0}}},
			"avg_created": bson.M{"$avg": bson.M{"$cond": bson.A{active, "$created_at", nil}}},
			"avg_lease":   bson.M{"$avg": bson.M{"$cond": bson.A{leased, "$lease_deadline", nil}}},
		}}},
	}

	cur, err := p.collection(client).Aggregate(ctx, pipeline)
	if err != nil {
		return errors.Errorf("query stats: %w", err)
	}
	defer func() { _ = cur.Close(ctx) }()

	var res struct {
		Total      int      `bson:"total"`
		NumLeased  int      `bson:"num_leased"`
		Scheduled  int      `bson:"scheduled"`
		AvgCreated *float64 `bson:"avg_created"`
		AvgLease   *float64 `bson:"avg_lease"`
	}

	if cur.Next(ctx) {
		if err := cur.Decode(&res); err != nil {
			return errors.Errorf("decode stats: %w", err)
		}
	}
	if err := cur.Err(); err != nil {
		return errors.Errorf("query stats: %w", err)
	}

	stats.Total = res.Total
	stats.NumLeased = res.NumLeased
	stats.Scheduled = res.Scheduled
	if res.AvgCreated != nil {
		stats.AverageAge = clock.Duration((float64(nowMicros) - *res.AvgCreated) * float64(time.Microsecond))
	}
	if res.AvgLease != nil {
		stats.AverageLeasedAge = clock.Duration((*res.AvgLease - float64(nowMicros)) * float64(time.Microsecond))
	}
	return nil
}

func (p *MongoPartition) ScanForScheduled(ctx context.Context, now clock.Time) iter.Seq2[types.Action, error] {
	return func(yield func(types.Action, error) bool) {
		batchSize := p.conf.ScanBatchSize
		if batchSize == 0 {
			batchSize = 1000
		}
		nowMicros := toMicros(now)
		var lastID string

		for {
			if ctx.Err() != nil {
				yield(types.Action{}, ctx.Err())
				return
			}

			client, err := p.conf.getOrCreateClient(ctx)
			if err != nil {
				yield(types.Action{}, err)
				return
			}

			if err := p.ensureCollection(ctx, client); err != nil {
				yield(types.Action{}, err)
				return
			}

			cur, err := p.collection(client).Find(ctx,
				bson.M{
					"enqueue_at": bson.M{"$exists": true, "$lte": nowMicros},
					"_id":        bson.M{"$gt": lastID},
				},
				options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetLimit(int64(batchSize)))
			if err != nil {
				yield(types.Action{}, err)
				return
			}

			var docs []mongoItem
			if err := cur.All(ctx, &docs); err != nil {
				yield(types.Action{}, err)
				return
			}

			if len(docs) == 0 {
				return
			}

			for i := range docs {
				item := docs[i].toItem()
				lastID = string(item.ID)
				if !yield(types.Action{
					Action:       types.ActionQueueScheduledItem,
					PartitionNum: p.info.PartitionNum,
					Queue:        p.info.Queue.Name,
					Item:         *item,
				}, nil) {
					return
				}
			}

			if len(docs) < batchSize {
				return
			}
		}
	}
}

func (p *MongoPartition) ScanForActions(ctx context.Context, now clock.Time) iter.Seq2[types.Action, error] {
	return func(yield func(types.Action, error) bool) {
		batchSize := p.conf.ScanBatchSize
		if batchSize == 0 {
			batchSize = 1000
		}
		nowMicros := toMicros(now)
		var lastID string

		for {
			if ctx.Err() != nil {
				yield(types.Action{}, ctx.Err())
				return
			}

			client, err := p.conf.getOrCreateClient(ctx)
			if err != nil {
				yield(types.Action{}, err)
				return
			}

			if err := p.ensureCollection(ctx, client); err != nil {
				yield(types.Action{}, err)
				return
			}

			cur, err := p.collection(client).Find(ctx,
				bson.M{
					"$or": bson.A{
						bson.M{"enqueue_at": bson.M{"$exists": false}},
						bson.M{"enqueue_at": bson.M{"$lte": nowMicros}},
					},
					"_id": bson.M{"$gt": lastID},
				},
				options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetLimit(int64(batchSize)))
			if err != nil {
				yield(types.Action{}, err)
				return
			}

			var docs []mongoItem
			if err := cur.All(ctx, &docs); err != nil {
				yield(types.Action{}, err)
				return
			}

			if len(docs) == 0 {
				return
			}

			p.mu.RLock()
			maxAttempts := p.info.Queue.MaxAttempts
			p.mu.RUnlock()

			for i := range docs {
				item := docs[i].toItem()
				lastID = string(item.ID)

				if item.IsLeased && now.After(item.LeaseDeadline) {
					if !yield(types.Action{
						Action:       types.ActionLeaseExpired,
						PartitionNum: p.info.PartitionNum,
						Queue:        p.info.Queue.Name,
						Item:         *item,
					}, nil) {
						return
					}
					continue
				}

				if item.IsLeased && maxAttempts != 0 && item.Attempts >= maxAttempts {
					if !yield(types.Action{
						Action:       types.ActionItemMaxAttempts,
						PartitionNum: p.info.PartitionNum,
						Queue:        p.info.Queue.Name,
						Item:         *item,
					}, nil) {
						return
					}
					continue
				}

				if now.After(item.ExpireDeadline) {
					if !yield(types.Action{
						Action:       types.ActionItemExpired,
						PartitionNum: p.info.PartitionNum,
						Queue:        p.info.Queue.Name,
						Item:         *item,
					}, nil) {
						return
					}
				}
			}

			if len(docs) < batchSize {
				return
			}
		}
	}
}

func (p *MongoPartition) TakeAction(ctx context.Context, batch types.LifeCycleBatch, _ *types.PartitionState) error {
	client, err := p.conf.getOrCreateClient(ctx)
	if err != nil {
		return err
	}

	if err := p.ensureCollection(ctx, client); err != nil {
		return err
	}

	coll := p.collection(client)

	for i := range batch.Requests {
		for _, action := range batch.Requests[i].Actions {
			switch action.Action {
			case types.ActionLeaseExpired:
				// Confirm the item still exists; the next scan re-finds it if we crash mid-move.
				err := coll.FindOne(ctx, bson.M{"_id": string(action.Item.ID)},
					options.FindOne().SetProjection(bson.M{"_id": 1})).Err()
				if errors.Is(err, mongo.ErrNoDocuments) {
					if p.conf.Log != nil {
						p.conf.Log.Warn("unable to find item while processing action; ignoring action",
							"id", string(action.Item.ID), "action", types.ActionToString(action.Action))
					}
					continue
				}
				if err != nil {
					return errors.Errorf("check item existence: %w", err)
				}

				// Insert the requeued tail document first, then delete the old (ADR-0026). Copy the
				// whole item and reset only the lease state so every payload/provenance field
				// (including SourceID) is carried forward without per-field enumeration.
				requeued := action.Item
				requeued.IsLeased = false
				requeued.LeaseDeadline = clock.Time{}
				newID := p.nextID()
				if _, err := coll.InsertOne(ctx, itemToDoc(newID, &requeued)); err != nil {
					return errors.Errorf("insert requeued item: %w", err)
				}
				if _, err := coll.DeleteOne(ctx, bson.M{"_id": string(action.Item.ID)}); err != nil {
					return errors.Errorf("delete expired item: %w", err)
				}

			case types.ActionItemExpired:
				if _, err := coll.DeleteOne(ctx, bson.M{"_id": string(action.Item.ID)}); err != nil {
					return errors.Errorf("delete expired item: %w", err)
				}

			case types.ActionDeleteItem:
				if _, err := coll.DeleteOne(ctx, bson.M{"_id": string(action.Item.ID)}); err != nil {
					return errors.Errorf("delete item: %w", err)
				}

			case types.ActionQueueScheduledItem:
				if _, err := coll.UpdateOne(ctx, bson.M{"_id": string(action.Item.ID)},
					bson.M{"$unset": bson.M{"enqueue_at": ""}}); err != nil {
					return errors.Errorf("queue scheduled item: %w", err)
				}
			}
		}
	}
	return nil
}

func (p *MongoPartition) LifeCycleInfo(ctx context.Context, info *types.LifeCycleInfo) error {
	client, err := p.conf.getOrCreateClient(ctx)
	if err != nil {
		return err
	}

	if err := p.ensureCollection(ctx, client); err != nil {
		return err
	}

	coll := p.collection(client)
	now := toMicros(clock.Now().UTC())

	// Next lease expiry: minimum lease_deadline among leased items.
	var leaseDoc struct {
		LeaseDeadline *int64 `bson:"lease_deadline"`
	}
	err = coll.FindOne(ctx, bson.M{"is_leased": true},
		options.FindOne().
			SetSort(bson.D{{Key: "lease_deadline", Value: 1}}).
			SetProjection(bson.M{"lease_deadline": 1})).Decode(&leaseDoc)
	if err == nil && leaseDoc.LeaseDeadline != nil {
		info.NextLeaseExpiry = fromMicros(*leaseDoc.LeaseDeadline)
	} else if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return errors.Errorf("query lifecycle lease info: %w", err)
	}

	// Next item expiry: minimum expire_deadline among active items.
	var expireDoc struct {
		ExpireDeadline int64 `bson:"expire_deadline"`
	}
	err = coll.FindOne(ctx,
		bson.M{"$or": bson.A{
			bson.M{"enqueue_at": bson.M{"$exists": false}},
			bson.M{"enqueue_at": bson.M{"$lte": now}},
		}},
		options.FindOne().
			SetSort(bson.D{{Key: "expire_deadline", Value: 1}}).
			SetProjection(bson.M{"expire_deadline": 1})).Decode(&expireDoc)
	if err == nil {
		info.NextExpireDeadline = fromMicros(expireDoc.ExpireDeadline)
	} else if !errors.Is(err, mongo.ErrNoDocuments) {
		return errors.Errorf("query lifecycle expire info: %w", err)
	}

	return nil
}

func (p *MongoPartition) Info() types.PartitionInfo {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.info
}

func (p *MongoPartition) UpdateQueueInfo(info types.QueueInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.info.Queue = info
}

func (p *MongoPartition) Close(_ context.Context) error {
	p.conf.Close()
	return nil
}
