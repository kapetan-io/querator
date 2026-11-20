package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kapetan-io/errors"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport/reply"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/set"
	"github.com/kapetan-io/tiger/assert"
	"github.com/segmentio/ksuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type mongoClientManager struct {
	client   *mongo.Client
	refCount atomic.Int32
}

var (
	globalMongoMu      sync.Mutex
	globalMongoClients = make(map[string]*mongoClientManager)
)

func acquireClientLocked(uri string, maxPool uint64, log *slog.Logger) (*mongo.Client, error) {
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

func releaseClientLocked(uri string, log *slog.Logger) {
	manager, exists := globalMongoClients[uri]
	if !exists {
		return
	}

	if manager.refCount.Add(-1) == 0 {
		if err := manager.client.Disconnect(context.Background()); err != nil && log != nil {
			log.Warn("failed to disconnect mongo client", "error", err)
		}
		delete(globalMongoClients, uri)
	}
}

type MongoConfig struct {
	ConnectionString string
	Database         string
	MaxPoolSize      uint64
	ScanBatchSize    int
	Log              *slog.Logger
	OnQueryComplete  func(operation string, duration time.Duration, err error)

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

	globalMongoMu.Lock()
	defer globalMongoMu.Unlock()

	if c.connString == "" {
		c.connString = c.ConnectionString
	}

	if !c.clientAcquired {
		client, err := acquireClientLocked(c.ConnectionString, c.MaxPoolSize, c.Log)
		if err != nil {
			return nil, err
		}
		c.clientAcquired = true
		return client, nil
	}

	manager, exists := globalMongoClients[c.connString]
	if !exists {
		return nil, errors.New("client was released")
	}

	return manager.client, nil
}

func (c *MongoConfig) Close() {
	globalMongoMu.Lock()
	defer globalMongoMu.Unlock()

	if c.connString != "" && c.clientAcquired {
		releaseClientLocked(c.connString, c.Log)
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

func toMicros(t clock.Time) int64 {
	return t.UnixMicro()
}

func fromMicros(v int64) clock.Time {
	return time.UnixMicro(v).UTC()
}

func normMicros(t clock.Time) clock.Time {
	return fromMicros(toMicros(t))
}

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
			"lease_timeout_ns":     found.LeaseTimeout.Nanoseconds(),
			"expire_timeout_ns":    found.ExpireTimeout.Nanoseconds(),
			"dead_queue":           found.DeadQueue,
			"max_attempts":         found.MaxAttempts,
			"reference":            found.Reference,
			"requested_partitions": found.RequestedPartitions,
			"partition_info":       partitionInfoJSON,
			"updated_at":           toMicros(found.UpdatedAt),
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

	for ctx.Err() == nil && cur.Next(ctx) {
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
	if err := ctx.Err(); err != nil {
		return err
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

func (p *MongoPartition) nextID() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.uid = p.uid.Next()
	return p.uid.String()
}

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

	indexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "is_leased", Value: 1}, {Key: "_id", Value: 1}},
			Options: options.Index().SetName("lease").SetPartialFilterExpression(bson.M{"is_leased": false}),
		},
		{
			Keys:    bson.D{{Key: "enqueue_at", Value: 1}},
			Options: options.Index().SetName("sched").SetPartialFilterExpression(bson.M{"enqueue_at": bson.M{"$exists": true}}),
		},
		{
			Keys:    bson.D{{Key: "lease_deadline", Value: 1}},
			Options: options.Index().SetName("deadline").SetPartialFilterExpression(bson.M{"is_leased": true}),
		},
		{
			Keys:    bson.D{{Key: "expire_deadline", Value: 1}},
			Options: options.Index().SetName("expire"),
		},
		{
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

	if batch.TotalRequested == 0 {
		return nil
	}

	coll := p.collection(client)

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
	var placed []*types.Item

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
		placed = append(placed, itemPtr)

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

	result, err := coll.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	if err != nil {
		batch.Unassign(unsavedLeases(err, result, placed))
		return errors.Errorf("update leased items: %w", err)
	}
	if result.MatchedCount != int64(len(models)) {
		batch.Unassign(placed)
		return errors.Errorf("update leased items: matched %d of %d items", result.MatchedCount, len(models))
	}
	return nil
}

func unsavedLeases(err error, result *mongo.BulkWriteResult, placed []*types.Item) []*types.Item {
	var bulk mongo.BulkWriteException
	if !errors.As(err, &bulk) || bulk.WriteConcernError != nil || len(bulk.WriteErrors) == 0 {
		return placed
	}
	if result == nil || result.MatchedCount+int64(len(bulk.WriteErrors)) < int64(len(placed)) {
		return placed
	}
	unsaved := make([]*types.Item, 0, len(bulk.WriteErrors))
	for _, writeErr := range bulk.WriteErrors {
		if writeErr.Index < 0 || writeErr.Index >= len(placed) {
			return placed
		}
		unsaved = append(unsaved, placed[writeErr.Index])
	}
	return unsaved
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

	for _, req := range batch.Requests {
		if err := p.completeItems(ctx, coll, req); err != nil {
			return err
		}
	}
	return nil
}

func (p *MongoPartition) completeItems(ctx context.Context, coll *mongo.Collection, req *types.CompleteRequest) error {
	for _, id := range req.Ids {
		if err := p.validateID(id); err != nil {
			req.Err = reply.NewInvalidOption("invalid storage id; '%s': %s", id, err)
			return nil
		}

		res := coll.FindOneAndDelete(ctx, bson.M{"_id": string(id), "is_leased": true})
		switch err := res.Err(); {
		case err == nil:
			req.Completed++
		case errors.Is(err, mongo.ErrNoDocuments):
			e := coll.FindOne(ctx, bson.M{"_id": string(id)},
				options.FindOne().SetProjection(bson.M{"_id": 1})).Err()
			if errors.Is(e, mongo.ErrNoDocuments) {
				req.Err = reply.NewInvalidOption("invalid storage id; '%s' does not exist", id)
				return nil
			} else if e != nil {
				return errors.Errorf("mongo partition %s/%d: failed to check item: %w",
					p.info.Queue.Name, p.info.PartitionNum, e)
			}
			req.Err = reply.NewConflict("item(s) cannot be completed; '%s' is not marked as leased", id)
			return nil
		default:
			return errors.Errorf("mongo partition %s/%d: failed to delete item: %w",
				p.info.Queue.Name, p.info.PartitionNum, err)
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

	for _, req := range batch.Requests {
		if err := p.retryItems(ctx, coll, req); err != nil {
			return err
		}
	}
	return nil
}

func (p *MongoPartition) retryItems(ctx context.Context, coll *mongo.Collection, req *types.RetryRequest) error {
	for _, retryItem := range req.Items {
		if err := p.validateID(retryItem.ID); err != nil {
			req.Err = reply.NewInvalidOption("invalid storage id; '%s': %s", retryItem.ID, err)
			return nil
		}

		var doc mongoItem
		err := coll.FindOne(ctx, bson.M{"_id": string(retryItem.ID)}).Decode(&doc)
		if errors.Is(err, mongo.ErrNoDocuments) {
			req.Err = reply.NewInvalidOption("invalid storage id; '%s' does not exist", retryItem.ID)
			return nil
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
			req.Err = reply.NewInvalidOption("invalid storage id; '%s' does not exist", retryItem.ID)
			return nil
		}

		if !item.IsLeased {
			req.Err = reply.NewConflict("item(s) cannot be retried; '%s' is not marked as leased", retryItem.ID)
			return nil
		}

		switch {
		case retryItem.Dead:
			if _, err := coll.DeleteOne(ctx, bson.M{"_id": string(retryItem.ID)}); err != nil {
				return errors.Errorf("during Retry() delete dead: %w", err)
			}
		case !retryItem.RetryAt.IsZero() && !retryItem.RetryAt.Before(clock.Now().UTC().Add(time.Millisecond*100)):
			if _, err := coll.UpdateOne(ctx, bson.M{"_id": string(retryItem.ID)},
				bson.M{"$set": bson.M{
					"is_leased":      false,
					"lease_deadline": nil,
					"enqueue_at":     toMicros(retryItem.RetryAt),
				}}); err != nil {
				return errors.Errorf("during Retry() schedule: %w", err)
			}
		default:
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
			req.UnLeased++
		}
		req.Retried++
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
	for ctx.Err() == nil && cur.Next(ctx) {
		var doc mongoItem
		if err := cur.Decode(&doc); err != nil {
			return errors.Errorf("decode item: %w", err)
		}
		*items = append(*items, doc.toItem())
	}
	if err := ctx.Err(); err != nil {
		return err
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

func (p *MongoPartition) ScanForScheduled(ctx context.Context, scan *types.LifeCycleScan, now clock.Time) error {
	return p.readLifeCyclePage(ctx, scan, bson.M{"enqueue_at": bson.M{"$exists": true}}, func(item types.Item) {
		if item.EnqueueAt.After(now) {
			scan.SeeEnqueueAt(item.EnqueueAt)
			return
		}
		scan.Add(types.Action{
			Action:       types.ActionQueueScheduledItem,
			PartitionNum: p.info.PartitionNum,
			Queue:        p.info.Queue.Name,
			Item:         item,
		})
	})
}

func (p *MongoPartition) ScanForActions(ctx context.Context, scan *types.LifeCycleScan, now clock.Time) error {
	p.mu.RLock()
	maxAttempts := p.info.Queue.MaxAttempts
	p.mu.RUnlock()

	filter := bson.M{"$or": bson.A{
		bson.M{"enqueue_at": bson.M{"$exists": false}},
		bson.M{"enqueue_at": bson.M{"$lte": toMicros(now)}},
	}}
	return p.readLifeCyclePage(ctx, scan, filter, func(item types.Item) {
		if item.IsLeased && !item.LeaseDeadline.After(now) {
			scan.Add(types.Action{
				Action:       types.ActionLeaseExpired,
				PartitionNum: p.info.PartitionNum,
				Queue:        p.info.Queue.Name,
				Item:         item,
			})
			return
		}

		if item.IsLeased && maxAttempts != 0 && item.Attempts >= maxAttempts {
			scan.Add(types.Action{
				Action:       types.ActionItemMaxAttempts,
				PartitionNum: p.info.PartitionNum,
				Queue:        p.info.Queue.Name,
				Item:         item,
			})
			return
		}
		if item.IsLeased {
			scan.SeeLeaseDeadline(item.LeaseDeadline)
		}

		if !item.ExpireDeadline.After(now) {
			scan.Add(types.Action{
				Action:       types.ActionItemExpired,
				PartitionNum: p.info.PartitionNum,
				Queue:        p.info.Queue.Name,
				Item:         item,
			})
			return
		}
		scan.SeeExpireDeadline(item.ExpireDeadline)
	})
}

func (p *MongoPartition) readLifeCyclePage(ctx context.Context, scan *types.LifeCycleScan,
	filter bson.M, visit func(types.Item)) error {

	client, err := p.conf.getOrCreateClient(ctx)
	if err != nil {
		return err
	}

	if err := p.ensureCollection(ctx, client); err != nil {
		return err
	}
	coll := p.collection(client)

	tail := string(scan.Last)
	if !scan.MidPass() {
		var last struct {
			ID string `bson:"_id"`
		}
		err := coll.FindOne(ctx, bson.M{}, options.FindOne().
			SetSort(bson.D{{Key: "_id", Value: -1}}).
			SetProjection(bson.M{"_id": 1})).Decode(&last)
		if errors.Is(err, mongo.ErrNoDocuments) {
			scan.EndPass()
			return nil
		}
		if err != nil {
			return errors.Errorf("query partition tail: %w", err)
		}
		tail = last.ID
	}

	limit := min(LifeCyclePageSize, p.conf.ScanBatchSize)

	filter["_id"] = bson.M{"$gt": string(scan.Pivot), "$lte": tail}
	cur, err := coll.Find(ctx, filter,
		options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetLimit(int64(limit)))
	if err != nil {
		return err
	}

	var docs []mongoItem
	if err := cur.All(ctx, &docs); err != nil {
		return err
	}

	for i := range docs {
		visit(*docs[i].toItem())
	}

	if len(docs) < limit {
		scan.EndPass()
		return nil
	}
	scan.Pivot, scan.Last = docs[len(docs)-1].toItem().ID, types.ItemID(tail)
	return nil
}

func (p *MongoPartition) TakeAction(ctx context.Context, batch types.LifeCycleBatch, state *types.PartitionState) error {
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
				state.LeasesExpired(1)

			case types.ActionItemExpired, types.ActionItemMaxAttempts:
				if _, err := coll.DeleteOne(ctx, bson.M{"_id": string(action.Item.ID)}); err != nil {
					return errors.Errorf("delete dead item: %w", err)
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

			default:
				assert.Unreachable(fmt.Sprintf("undefined action kind '0x%X'", int(action.Action)))
			}
		}
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
