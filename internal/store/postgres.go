package store

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"fmt"
	"iter"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kapetan-io/errors"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/set"
	"github.com/segmentio/ksuid"
)

// ---------------------------------------------
// Global Pool Manager
// ---------------------------------------------

type postgresPoolManager struct {
	pool     *pgxpool.Pool
	refCount atomic.Int32
}

var (
	globalPoolMu sync.Mutex
	globalPools  = make(map[string]*postgresPoolManager)
)

func acquirePool(connString string, maxConns int32, log *slog.Logger) (*pgxpool.Pool, error) {
	globalPoolMu.Lock()
	defer globalPoolMu.Unlock()

	if manager, exists := globalPools[connString]; exists {
		manager.refCount.Add(1)
		return manager.pool, nil
	}

	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, errors.Errorf("parse connection string: %w", err)
	}

	if maxConns > 0 {
		config.MaxConns = maxConns
	}

	var pool *pgxpool.Pool
	delays := []time.Duration{0, 100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond}

	for attempt, delay := range delays {
		if delay > 0 {
			time.Sleep(delay)
		}

		pool, err = pgxpool.NewWithConfig(context.Background(), config)
		if err == nil {
			break
		}

		if attempt < len(delays)-1 && log != nil {
			log.Warn("failed to create pool, retrying",
				"attempt", attempt+1,
				"error", err,
				"next_delay", delays[attempt+1])
		}
	}

	if err != nil {
		return nil, errors.Errorf("create pool after retries: %w", err)
	}

	manager := &postgresPoolManager{pool: pool}
	manager.refCount.Store(1)
	globalPools[connString] = manager

	return pool, nil
}

func releasePool(connString string) {
	globalPoolMu.Lock()
	defer globalPoolMu.Unlock()

	manager, exists := globalPools[connString]
	if !exists {
		return
	}

	if manager.refCount.Add(-1) == 0 {
		manager.pool.Close()
		delete(globalPools, connString)
	}
}

// ---------------------------------------------
// PostgreSQL Configuration
// ---------------------------------------------

type PostgresConfig struct {
	ConnectionString string
	MaxConns         int32
	ScanBatchSize    int
	Log              *slog.Logger
	OnQueryComplete  func(operation string, duration time.Duration, err error)
	connString       string
	poolAcquired     bool
}

func (c *PostgresConfig) getOrCreatePool(_ context.Context) (*pgxpool.Pool, error) {
	if c.ConnectionString == "" {
		return nil, errors.New("connection string is required")
	}

	if c.connString == "" {
		c.connString = c.ConnectionString
	}

	// Only acquire the pool once per config instance
	if !c.poolAcquired {
		pool, err := acquirePool(c.ConnectionString, c.MaxConns, c.Log)
		if err != nil {
			return nil, err
		}
		c.poolAcquired = true
		return pool, nil
	}

	// Return the existing pool without incrementing refCount
	globalPoolMu.Lock()
	defer globalPoolMu.Unlock()

	manager, exists := globalPools[c.connString]
	if !exists {
		return nil, errors.New("pool was released")
	}

	return manager.pool, nil
}

func (c *PostgresConfig) Close() {
	if c.connString != "" && c.poolAcquired {
		releasePool(c.connString)
		c.poolAcquired = false
	}
}

func (c *PostgresConfig) Ping(ctx context.Context) error {
	pool, err := c.getOrCreatePool(ctx)
	if err != nil {
		return err
	}
	return pool.Ping(ctx)
}

// ---------------------------------------------
// PostgreSQL Queues Implementation
// ---------------------------------------------

type PostgresQueues struct {
	QueuesValidation
	conf PostgresConfig
}

var _ Queues = &PostgresQueues{}

func NewPostgresQueues(conf PostgresConfig) *PostgresQueues {
	set.Default(&conf.Log, slog.Default())
	set.Default(&conf.ScanBatchSize, 1000)
	return &PostgresQueues{conf: conf}
}

func (p *PostgresQueues) ensureTable(_ context.Context, pool *pgxpool.Pool) error {
	// Use a context with reasonable timeout for table creation
	bgCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	query := `
		CREATE TABLE IF NOT EXISTS queues (
			name TEXT PRIMARY KEY,
			lease_timeout_ns BIGINT NOT NULL,
			expire_timeout_ns BIGINT NOT NULL,
			dead_queue TEXT NOT NULL DEFAULT '',
			max_attempts INTEGER NOT NULL DEFAULT 0,
			reference TEXT NOT NULL DEFAULT '',
			requested_partitions INTEGER NOT NULL,
			partition_info JSONB NOT NULL DEFAULT '[]'::jsonb,
			created_at TIMESTAMPTZ NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL
		)`

	_, err := pool.Exec(bgCtx, query)
	if err != nil {
		return errors.Errorf("create queues table: %w", err)
	}
	return nil
}

func (p *PostgresQueues) Get(ctx context.Context, name string, queue *types.QueueInfo) error {
	if err := p.validateGet(name); err != nil {
		return err
	}

	pool, err := p.conf.getOrCreatePool(ctx)
	if err != nil {
		return err
	}

	if err := p.ensureTable(ctx, pool); err != nil {
		return err
	}

	query := `SELECT name, lease_timeout_ns, expire_timeout_ns, dead_queue, max_attempts,
	                 reference, requested_partitions, partition_info, created_at, updated_at
	          FROM queues WHERE name = $1`

	var leaseNs, expireNs int64
	var partitionInfoJSON []byte
	err = pool.QueryRow(ctx, query, name).Scan(
		&queue.Name,
		&leaseNs,
		&expireNs,
		&queue.DeadQueue,
		&queue.MaxAttempts,
		&queue.Reference,
		&queue.RequestedPartitions,
		&partitionInfoJSON,
		&queue.CreatedAt,
		&queue.UpdatedAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrQueueNotExist
		}
		return errors.Errorf("query queue: %w", err)
	}

	queue.LeaseTimeout = clock.Duration(leaseNs)
	queue.ExpireTimeout = clock.Duration(expireNs)

	if len(partitionInfoJSON) > 0 {
		if err := json.Unmarshal(partitionInfoJSON, &queue.PartitionInfo); err != nil {
			return errors.Errorf("unmarshal partition_info: %w", err)
		}
	}

	return nil
}

func (p *PostgresQueues) Add(ctx context.Context, info types.QueueInfo) error {
	if err := p.validateAdd(info); err != nil {
		return err
	}

	pool, err := p.conf.getOrCreatePool(ctx)
	if err != nil {
		return err
	}

	if err := p.ensureTable(ctx, pool); err != nil {
		return err
	}

	partitionInfoJSON, err := json.Marshal(info.PartitionInfo)
	if err != nil {
		return errors.Errorf("marshal partition_info: %w", err)
	}

	query := `INSERT INTO queues (name, lease_timeout_ns, expire_timeout_ns, dead_queue, max_attempts,
	                              reference, requested_partitions, partition_info, created_at, updated_at)
	          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`

	_, err = pool.Exec(ctx, query,
		info.Name,
		info.LeaseTimeout.Nanoseconds(),
		info.ExpireTimeout.Nanoseconds(),
		info.DeadQueue,
		info.MaxAttempts,
		info.Reference,
		info.RequestedPartitions,
		partitionInfoJSON,
		timeToMicroseconds(info.CreatedAt),
		timeToMicroseconds(info.UpdatedAt),
	)

	if err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "23505" {
			return transport.NewInvalidOption("queue '%s' already exists", info.Name)
		}
		return errors.Errorf("insert queue: %w", err)
	}

	return nil
}

func (p *PostgresQueues) Update(ctx context.Context, info types.QueueInfo) error {
	if err := p.validateQueueName(info); err != nil {
		return err
	}

	pool, err := p.conf.getOrCreatePool(ctx)
	if err != nil {
		return err
	}

	if err := p.ensureTable(ctx, pool); err != nil {
		return err
	}

	var found types.QueueInfo
	if err := p.Get(ctx, info.Name, &found); err != nil {
		return err
	}

	found.Update(info)

	if err := p.validateUpdate(found); err != nil {
		return err
	}

	if found.LeaseTimeout > found.ExpireTimeout {
		return transport.NewInvalidOption("lease timeout is too long; %s cannot be greater than the "+
			"expire timeout %s", found.LeaseTimeout.String(), found.ExpireTimeout.String())
	}

	partitionInfoJSON, err := json.Marshal(found.PartitionInfo)
	if err != nil {
		return errors.Errorf("marshal partition_info: %w", err)
	}

	query := `UPDATE queues SET lease_timeout_ns = $1, expire_timeout_ns = $2, dead_queue = $3,
	                           max_attempts = $4, reference = $5, partition_info = $6, updated_at = $7
	          WHERE name = $8`

	_, err = pool.Exec(ctx, query,
		found.LeaseTimeout.Nanoseconds(),
		found.ExpireTimeout.Nanoseconds(),
		found.DeadQueue,
		found.MaxAttempts,
		found.Reference,
		partitionInfoJSON,
		timeToMicroseconds(found.UpdatedAt),
		found.Name,
	)

	if err != nil {
		return errors.Errorf("update queue: %w", err)
	}

	return nil
}

func (p *PostgresQueues) List(ctx context.Context, queues *[]types.QueueInfo, opts types.ListOptions) error {
	if err := p.validateList(opts); err != nil {
		return err
	}

	pool, err := p.conf.getOrCreatePool(ctx)
	if err != nil {
		return err
	}

	if err := p.ensureTable(ctx, pool); err != nil {
		return err
	}

	pivot := ""
	if opts.Pivot != nil {
		pivot = string(opts.Pivot)
	}

	query := `SELECT name, lease_timeout_ns, expire_timeout_ns, dead_queue, max_attempts,
	                 reference, requested_partitions, partition_info, created_at, updated_at
	          FROM queues WHERE name >= $1 ORDER BY name LIMIT $2`

	rows, err := pool.Query(ctx, query, pivot, opts.Limit)
	if err != nil {
		return errors.Errorf("query queues: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var info types.QueueInfo
		var leaseNs, expireNs int64
		var partitionInfoJSON []byte

		err := rows.Scan(
			&info.Name,
			&leaseNs,
			&expireNs,
			&info.DeadQueue,
			&info.MaxAttempts,
			&info.Reference,
			&info.RequestedPartitions,
			&partitionInfoJSON,
			&info.CreatedAt,
			&info.UpdatedAt,
		)

		if err != nil {
			return errors.Errorf("scan queue row: %w", err)
		}

		info.LeaseTimeout = clock.Duration(leaseNs)
		info.ExpireTimeout = clock.Duration(expireNs)

		if len(partitionInfoJSON) > 0 {
			if err := json.Unmarshal(partitionInfoJSON, &info.PartitionInfo); err != nil {
				return errors.Errorf("unmarshal partition_info: %w", err)
			}
		}

		*queues = append(*queues, info)
	}

	return rows.Err()
}

func (p *PostgresQueues) Delete(ctx context.Context, name string) error {
	if err := p.validateDelete(name); err != nil {
		return err
	}

	pool, err := p.conf.getOrCreatePool(ctx)
	if err != nil {
		return err
	}

	if err := p.ensureTable(ctx, pool); err != nil {
		return err
	}

	query := `DELETE FROM queues WHERE name = $1`

	_, err = pool.Exec(ctx, query, name)
	if err != nil {
		return errors.Errorf("delete queue: %w", err)
	}

	return nil
}

func (p *PostgresQueues) Close(ctx context.Context) error {
	p.conf.Close()
	return nil
}

// ---------------------------------------------
// PostgreSQL Partition Store Implementation
// ---------------------------------------------

type PostgresPartitionStore struct {
	conf PostgresConfig
}

var _ PartitionStore = &PostgresPartitionStore{}

func NewPostgresPartitionStore(conf PostgresConfig) *PostgresPartitionStore {
	set.Default(&conf.Log, slog.Default())
	set.Default(&conf.ScanBatchSize, 1000)
	return &PostgresPartitionStore{conf: conf}
}

func (p *PostgresPartitionStore) Get(info types.PartitionInfo) Partition {
	return &PostgresPartition{
		uid:  ksuid.New(),
		conf: p.conf,
		info: info,
	}
}

// ---------------------------------------------
// PostgreSQL Partition Implementation
// ---------------------------------------------

type PostgresPartition struct {
	info      types.PartitionInfo
	conf      PostgresConfig
	mu        sync.RWMutex
	uid       ksuid.KSUID
	tableOnce sync.Once
	tableErr  error
}

var _ Partition = &PostgresPartition{}

func hashQueueName(name string) string {
	h := md5.Sum([]byte(name))

	var num uint64
	for i := 0; i < 8; i++ {
		num = (num << 8) | uint64(h[i])
	}

	const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

	if num == 0 {
		return "0000000000"
	}

	result := make([]byte, 0, 11)
	for num > 0 {
		result = append(result, alphabet[num%62])
		num /= 62
	}

	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}

	for len(result) < 10 {
		result = append([]byte{'0'}, result...)
	}

	return string(result[:10])
}

func (p *PostgresPartition) tableName() string {
	hash := hashQueueName(p.info.Queue.Name)
	name := fmt.Sprintf("items_%s_%d", hash, p.info.PartitionNum)
	return pgx.Identifier{name}.Sanitize()
}

func (p *PostgresPartition) ensureTable(_ context.Context, pool *pgxpool.Pool) error {
	p.tableOnce.Do(func() {
		// Use a separate context with a reasonable timeout for table creation
		// Don't use the passed context as it might have a short timeout
		bgCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		p.tableErr = p.createTableAndIndexes(bgCtx, pool)
	})
	return p.tableErr
}

func (p *PostgresPartition) createTableAndIndexes(ctx context.Context, pool *pgxpool.Pool) error {
	table := p.tableName()
	hash := hashQueueName(p.info.Queue.Name)
	leaseName := pgx.Identifier{fmt.Sprintf("idx_%s_%d_lse", hash, p.info.PartitionNum)}.Sanitize()
	schedName := pgx.Identifier{fmt.Sprintf("idx_%s_%d_sch", hash, p.info.PartitionNum)}.Sanitize()
	deadlineName := pgx.Identifier{fmt.Sprintf("idx_%s_%d_dln", hash, p.info.PartitionNum)}.Sanitize()
	expireName := pgx.Identifier{fmt.Sprintf("idx_%s_%d_exp", hash, p.info.PartitionNum)}.Sanitize()

	if p.conf.Log != nil {
		p.conf.Log.Debug("Creating PostgreSQL table and indexes", "table", table, "queue", p.info.Queue.Name, "partition", p.info.PartitionNum)
	}

	_, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id TEXT COLLATE "C" PRIMARY KEY,
			source_id TEXT,
			is_leased BOOLEAN NOT NULL DEFAULT false,
			lease_deadline TIMESTAMPTZ,
			expire_deadline TIMESTAMPTZ NOT NULL,
			enqueue_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL,
			attempts INTEGER NOT NULL DEFAULT 0,
			max_attempts INTEGER NOT NULL DEFAULT 0,
			reference TEXT NOT NULL DEFAULT '',
			encoding TEXT NOT NULL DEFAULT '',
			kind TEXT NOT NULL DEFAULT '',
			payload BYTEA
		)`, table))
	if err != nil {
		if p.conf.Log != nil {
			p.conf.Log.Error("Failed to create PostgreSQL table", "error", err, "table", table)
		}
		return errors.Errorf("create table: %w", err)
	}

	// Add partial unique index for source_id
	sourceIdxName := pgx.Identifier{fmt.Sprintf("idx_%s_%d_src", hash, p.info.PartitionNum)}.Sanitize()
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE UNIQUE INDEX IF NOT EXISTS %s ON %s (source_id) WHERE source_id IS NOT NULL`, sourceIdxName, table))
	if err != nil {
		return errors.Errorf("create source_id index: %w", err)
	}

	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE INDEX IF NOT EXISTS %s ON %s(is_leased, enqueue_at)
		WHERE is_leased = false AND enqueue_at IS NULL`, leaseName, table))
	if err != nil {
		return errors.Errorf("create lease index: %w", err)
	}

	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE INDEX IF NOT EXISTS %s ON %s(enqueue_at)
		WHERE enqueue_at IS NOT NULL`, schedName, table))
	if err != nil {
		return errors.Errorf("create scheduled index: %w", err)
	}

	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE INDEX IF NOT EXISTS %s ON %s(lease_deadline)
		WHERE is_leased = true`, deadlineName, table))
	if err != nil {
		return errors.Errorf("create deadline index: %w", err)
	}

	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE INDEX IF NOT EXISTS %s ON %s(expire_deadline)`, expireName, table))
	if err != nil {
		return errors.Errorf("create expire index: %w", err)
	}

	return nil
}

func (p *PostgresPartition) validateID(id []byte) error {
	_, err := ksuid.Parse(string(id))
	return err
}

func timeToNullable(t time.Time) interface{} {
	if t.IsZero() {
		return nil
	}
	// Truncate to microseconds to match PostgreSQL TIMESTAMPTZ precision
	return t.Truncate(time.Microsecond)
}

func timeToMicroseconds(t time.Time) time.Time {
	// Truncate to microseconds to match PostgreSQL TIMESTAMPTZ precision
	return t.Truncate(time.Microsecond)
}

func (p *PostgresPartition) Produce(ctx context.Context, batch types.ProduceBatch, now clock.Time) error {
	pool, err := p.conf.getOrCreatePool(ctx)
	if err != nil {
		return err
	}

	if err := p.ensureTable(ctx, pool); err != nil {
		return err
	}

	pgxBatch := &pgx.Batch{}
	requestIndexes := []int{}

	p.mu.Lock()
	for i := range batch.Requests {
		for _, item := range batch.Requests[i].Items {
			p.uid = p.uid.Next()
			item.ID = []byte(p.uid.String())
			item.CreatedAt = now

			if item.EnqueueAt.Before(now.Add(time.Millisecond * 100)) {
				item.EnqueueAt = time.Time{}
			}

			// Truncate timestamps to microseconds to match PostgreSQL precision
			item.ExpireDeadline = timeToMicroseconds(item.ExpireDeadline)
			item.CreatedAt = timeToMicroseconds(item.CreatedAt)
			if !item.LeaseDeadline.IsZero() {
				item.LeaseDeadline = timeToMicroseconds(item.LeaseDeadline)
			}
			if !item.EnqueueAt.IsZero() {
				item.EnqueueAt = timeToMicroseconds(item.EnqueueAt)
			}

			var sourceID interface{}
			if item.SourceID != nil {
				sourceID = string(item.SourceID)
			}

			var query string
			if sourceID != nil {
				query = `
					INSERT INTO ` + p.tableName() + ` (
						id, source_id, is_leased, lease_deadline, expire_deadline, enqueue_at, created_at,
						attempts, max_attempts, reference, encoding, kind, payload
					) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
					ON CONFLICT (source_id) WHERE source_id IS NOT NULL DO NOTHING`
			} else {
				query = `
					INSERT INTO ` + p.tableName() + ` (
						id, source_id, is_leased, lease_deadline, expire_deadline, enqueue_at, created_at,
						attempts, max_attempts, reference, encoding, kind, payload
					) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`
			}

			pgxBatch.Queue(query,
				string(item.ID),
				sourceID,
				item.IsLeased,
				timeToNullable(item.LeaseDeadline),
				item.ExpireDeadline,
				timeToNullable(item.EnqueueAt),
				item.CreatedAt,
				item.Attempts,
				item.MaxAttempts,
				item.Reference,
				item.Encoding,
				item.Kind,
				item.Payload,
			)

			requestIndexes = append(requestIndexes, i)
		}
	}
	p.mu.Unlock()

	br := pool.SendBatch(ctx, pgxBatch)
	defer func() { _ = br.Close() }()

	for range requestIndexes {
		_, err := br.Exec()
		if err != nil {
			return errors.Errorf("failed to insert item: %w", err)
		}
	}

	return nil
}

func (p *PostgresPartition) Lease(ctx context.Context, batch types.LeaseBatch, opts LeaseOptions) error {
	pool, err := p.conf.getOrCreatePool(ctx)
	if err != nil {
		return err
	}

	if err := p.ensureTable(ctx, pool); err != nil {
		return err
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return errors.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	query := `
		SELECT id, source_id, is_leased, lease_deadline, expire_deadline, enqueue_at, created_at,
		       attempts, max_attempts, reference, encoding, kind, payload
		FROM ` + p.tableName() + `
		WHERE is_leased = false AND enqueue_at IS NULL
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED`

	limit := batch.TotalRequested

	rows, err := tx.Query(ctx, query, limit)
	if err != nil {
		return errors.Errorf("query items for lease: %w", err)
	}
	defer rows.Close()

	var items []types.Item
	for rows.Next() {
		var item types.Item
		var leaseDeadline, enqueueAt sql.NullTime
		var sourceID sql.NullString

		err := rows.Scan(
			&item.ID,
			&sourceID,
			&item.IsLeased,
			&leaseDeadline,
			&item.ExpireDeadline,
			&enqueueAt,
			&item.CreatedAt,
			&item.Attempts,
			&item.MaxAttempts,
			&item.Reference,
			&item.Encoding,
			&item.Kind,
			&item.Payload,
		)
		if err != nil {
			return errors.Errorf("scan item: %w", err)
		}

		if sourceID.Valid {
			item.SourceID = []byte(sourceID.String)
		}
		if leaseDeadline.Valid {
			item.LeaseDeadline = leaseDeadline.Time
		}
		if enqueueAt.Valid {
			item.EnqueueAt = enqueueAt.Time
		}

		items = append(items, item)
	}
	rows.Close()

	if len(items) == 0 {
		return tx.Commit(ctx)
	}

	batchIter := batch.Iterator()
	pgxBatch := &pgx.Batch{}

	for _, item := range items {
		item.LeaseDeadline = opts.LeaseDeadline
		item.IsLeased = true
		item.Attempts++

		itemPtr := new(types.Item)
		*itemPtr = item

		if !batchIter.Next(itemPtr) {
			break
		}

		pgxBatch.Queue(`
			UPDATE `+p.tableName()+`
			SET is_leased = true,
				lease_deadline = $1,
				attempts = attempts + 1
			WHERE id = $2`,
			timeToMicroseconds(opts.LeaseDeadline),
			string(item.ID),
		)
	}

	br := tx.SendBatch(ctx, pgxBatch)
	for i := 0; i < pgxBatch.Len(); i++ {
		_, err := br.Exec()
		if err != nil {
			_ = br.Close()
			return errors.Errorf("update leased item: %w", err)
		}
	}
	_ = br.Close()

	return tx.Commit(ctx)
}

func (p *PostgresPartition) Complete(ctx context.Context, batch types.CompleteBatch) error {
	pool, err := p.conf.getOrCreatePool(ctx)
	if err != nil {
		return err
	}

	if err := p.ensureTable(ctx, pool); err != nil {
		return err
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return errors.Errorf("postgres partition %s/%d: begin transaction: %w",
			p.info.Queue.Name, p.info.PartitionNum, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

nextBatch:
	for i := range batch.Requests {
		for _, id := range batch.Requests[i].Ids {
			if err := p.validateID(id); err != nil {
				batch.Requests[i].Err = transport.NewInvalidOption("invalid storage id; '%s': %s", id, err)
				continue nextBatch
			}

			var isLeased bool
			err := tx.QueryRow(ctx,
				`SELECT is_leased FROM `+p.tableName()+` WHERE id = $1 FOR UPDATE`,
				string(id)).Scan(&isLeased)

			if err == pgx.ErrNoRows {
				batch.Requests[i].Err = transport.NewInvalidOption("invalid storage id; '%s' does not exist", id)
				continue nextBatch
			}
			if err != nil {
				return errors.Errorf("postgres partition %s/%d: failed to check item: %w",
					p.info.Queue.Name, p.info.PartitionNum, err)
			}

			if !isLeased {
				batch.Requests[i].Err = transport.NewConflict("item(s) cannot be completed; '%s' is not marked as leased", id)
				continue nextBatch
			}

			_, err = tx.Exec(ctx, `DELETE FROM `+p.tableName()+` WHERE id = $1`, string(id))
			if err != nil {
				return errors.Errorf("postgres partition %s/%d: failed to delete item: %w",
					p.info.Queue.Name, p.info.PartitionNum, err)
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return errors.Errorf("postgres partition %s/%d: commit transaction: %w",
			p.info.Queue.Name, p.info.PartitionNum, err)
	}

	return nil
}

func (p *PostgresPartition) Retry(ctx context.Context, batch types.RetryBatch) error {
	pool, err := p.conf.getOrCreatePool(ctx)
	if err != nil {
		return err
	}

	if err := p.ensureTable(ctx, pool); err != nil {
		return err
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return errors.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

nextBatch:
	for i := range batch.Requests {
		for _, retryItem := range batch.Requests[i].Items {
			if err := p.validateID(retryItem.ID); err != nil {
				batch.Requests[i].Err = transport.NewInvalidOption("invalid storage id; '%s': %s", retryItem.ID, err)
				continue nextBatch
			}

			var isLeased bool
			var enqueueAt sql.NullTime
			err := tx.QueryRow(ctx,
				`SELECT is_leased, enqueue_at FROM `+p.tableName()+` WHERE id = $1 FOR UPDATE`,
				retryItem.ID).Scan(&isLeased, &enqueueAt)

			if err == pgx.ErrNoRows {
				batch.Requests[i].Err = transport.NewInvalidOption("id does not exist")
				continue nextBatch
			}
			if err != nil {
				return errors.Errorf("failed to check lease status: %w", err)
			}

			if enqueueAt.Valid && !enqueueAt.Time.IsZero() {
				if p.conf.Log != nil {
					p.conf.Log.LogAttrs(ctx, slog.LevelWarn, "attempted to retry a scheduled item; reported does not exist",
						slog.String("id", string(retryItem.ID)))
				}
				batch.Requests[i].Err = transport.NewInvalidOption("invalid storage id; '%s' does not exist", retryItem.ID)
				continue nextBatch
			}

			if !isLeased {
				batch.Requests[i].Err = transport.NewConflict("item not leased")
				continue nextBatch
			}

			switch {
			case retryItem.Dead:
				_, err = tx.Exec(ctx, `DELETE FROM `+p.tableName()+` WHERE id = $1`, retryItem.ID)
			case !retryItem.RetryAt.IsZero():
				// If RetryAt is in the past or less than 100ms from now, treat as immediate retry
				now := clock.Now().UTC()
				if retryItem.RetryAt.Before(now.Add(time.Millisecond * 100)) {
					// Immediate retry - enqueue_at stays NULL
					_, err = tx.Exec(ctx, `
						UPDATE `+p.tableName()+`
						SET is_leased = false,
							lease_deadline = NULL,
							enqueue_at = NULL
						WHERE id = $1`,
						retryItem.ID)
				} else {
					// Schedule for future retry
					_, err = tx.Exec(ctx, `
						UPDATE `+p.tableName()+`
						SET is_leased = false,
							lease_deadline = NULL,
							enqueue_at = $1
						WHERE id = $2`,
						timeToMicroseconds(retryItem.RetryAt), retryItem.ID)
				}
			default:
				_, err = tx.Exec(ctx, `
					UPDATE `+p.tableName()+`
					SET is_leased = false,
						lease_deadline = NULL
					WHERE id = $1`,
					retryItem.ID)
			}

			if err != nil {
				batch.Requests[i].Err = transport.NewInvalidOption("failed to retry item: %s", err)
				continue nextBatch
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return errors.Errorf("commit transaction: %w", err)
	}

	return nil
}

func (p *PostgresPartition) List(ctx context.Context, items *[]*types.Item, opts types.ListOptions) error {
	pool, err := p.conf.getOrCreatePool(ctx)
	if err != nil {
		return err
	}

	if err := p.ensureTable(ctx, pool); err != nil {
		return err
	}

	pivot := ""
	if opts.Pivot != nil {
		if err := p.validateID(opts.Pivot); err != nil {
			return transport.NewInvalidOption("invalid storage id; '%s': %s", opts.Pivot, err)
		}
		pivot = string(opts.Pivot)
	}

	query := `
		SELECT id, source_id, is_leased, lease_deadline, expire_deadline, enqueue_at, created_at,
		       attempts, max_attempts, reference, encoding, kind, payload
		FROM ` + p.tableName() + `
		WHERE enqueue_at IS NULL AND id >= $1
		ORDER BY id
		LIMIT $2`

	rows, err := pool.Query(ctx, query, pivot, opts.Limit)
	if err != nil {
		return errors.Errorf("query items: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		item := new(types.Item)
		var leaseDeadline, enqueueAt sql.NullTime
		var sourceID sql.NullString

		err := rows.Scan(
			&item.ID,
			&sourceID,
			&item.IsLeased,
			&leaseDeadline,
			&item.ExpireDeadline,
			&enqueueAt,
			&item.CreatedAt,
			&item.Attempts,
			&item.MaxAttempts,
			&item.Reference,
			&item.Encoding,
			&item.Kind,
			&item.Payload,
		)
		if err != nil {
			return errors.Errorf("scan item: %w", err)
		}

		if sourceID.Valid {
			item.SourceID = []byte(sourceID.String)
		}
		if leaseDeadline.Valid {
			item.LeaseDeadline = leaseDeadline.Time
		}
		if enqueueAt.Valid {
			item.EnqueueAt = enqueueAt.Time
		}

		*items = append(*items, item)
	}

	return rows.Err()
}

func (p *PostgresPartition) ListScheduled(ctx context.Context, items *[]*types.Item, opts types.ListOptions) error {
	pool, err := p.conf.getOrCreatePool(ctx)
	if err != nil {
		return err
	}

	if err := p.ensureTable(ctx, pool); err != nil {
		return err
	}

	pivot := ""
	if opts.Pivot != nil {
		if err := p.validateID(opts.Pivot); err != nil {
			return transport.NewInvalidOption("invalid storage id; '%s': %s", opts.Pivot, err)
		}
		pivot = string(opts.Pivot)
	}

	query := `
		SELECT id, source_id, is_leased, lease_deadline, expire_deadline, enqueue_at, created_at,
		       attempts, max_attempts, reference, encoding, kind, payload
		FROM ` + p.tableName() + `
		WHERE enqueue_at IS NOT NULL AND id >= $1
		ORDER BY id
		LIMIT $2`

	rows, err := pool.Query(ctx, query, pivot, opts.Limit)
	if err != nil {
		return errors.Errorf("query scheduled items: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		item := new(types.Item)
		var leaseDeadline, enqueueAt sql.NullTime
		var sourceID sql.NullString

		err := rows.Scan(
			&item.ID,
			&sourceID,
			&item.IsLeased,
			&leaseDeadline,
			&item.ExpireDeadline,
			&enqueueAt,
			&item.CreatedAt,
			&item.Attempts,
			&item.MaxAttempts,
			&item.Reference,
			&item.Encoding,
			&item.Kind,
			&item.Payload,
		)
		if err != nil {
			return errors.Errorf("scan item: %w", err)
		}

		if sourceID.Valid {
			item.SourceID = []byte(sourceID.String)
		}
		if leaseDeadline.Valid {
			item.LeaseDeadline = leaseDeadline.Time
		}
		if enqueueAt.Valid {
			item.EnqueueAt = enqueueAt.Time
		}

		*items = append(*items, item)
	}

	return rows.Err()
}

func (p *PostgresPartition) Add(ctx context.Context, items []*types.Item, now clock.Time) error {
	if len(items) == 0 {
		return transport.NewInvalidOption("items is invalid; cannot be empty")
	}

	pool, err := p.conf.getOrCreatePool(ctx)
	if err != nil {
		return err
	}

	if err := p.ensureTable(ctx, pool); err != nil {
		return err
	}

	pgxBatch := &pgx.Batch{}

	p.mu.Lock()
	for _, item := range items {
		p.uid = p.uid.Next()
		item.ID = []byte(p.uid.String())
		item.CreatedAt = now

		// Truncate timestamps to microseconds to match PostgreSQL precision
		item.ExpireDeadline = timeToMicroseconds(item.ExpireDeadline)
		item.CreatedAt = timeToMicroseconds(item.CreatedAt)
		if !item.LeaseDeadline.IsZero() {
			item.LeaseDeadline = timeToMicroseconds(item.LeaseDeadline)
		}
		if !item.EnqueueAt.IsZero() {
			item.EnqueueAt = timeToMicroseconds(item.EnqueueAt)
		}

		var sourceID interface{}
		if item.SourceID != nil {
			sourceID = string(item.SourceID)
		}

		var query string
		if sourceID != nil {
			query = `
				INSERT INTO ` + p.tableName() + ` (
					id, source_id, is_leased, lease_deadline, expire_deadline, enqueue_at, created_at,
					attempts, max_attempts, reference, encoding, kind, payload
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
				ON CONFLICT (source_id) WHERE source_id IS NOT NULL DO NOTHING`
		} else {
			query = `
				INSERT INTO ` + p.tableName() + ` (
					id, source_id, is_leased, lease_deadline, expire_deadline, enqueue_at, created_at,
					attempts, max_attempts, reference, encoding, kind, payload
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`
		}

		pgxBatch.Queue(query,
			string(item.ID),
			sourceID,
			item.IsLeased,
			timeToNullable(item.LeaseDeadline),
			item.ExpireDeadline,
			timeToNullable(item.EnqueueAt),
			item.CreatedAt,
			item.Attempts,
			item.MaxAttempts,
			item.Reference,
			item.Encoding,
			item.Kind,
			item.Payload,
		)
	}
	p.mu.Unlock()

	br := pool.SendBatch(ctx, pgxBatch)
	defer func() { _ = br.Close() }()

	for range items {
		_, err := br.Exec()
		if err != nil {
			return errors.Errorf("failed to insert item: %w", err)
		}
	}

	return nil
}

func (p *PostgresPartition) Delete(ctx context.Context, ids []types.ItemID) error {
	if len(ids) == 0 {
		return transport.NewInvalidOption("ids is invalid; cannot be empty")
	}

	pool, err := p.conf.getOrCreatePool(ctx)
	if err != nil {
		return err
	}

	if err := p.ensureTable(ctx, pool); err != nil {
		return err
	}

	pgxBatch := &pgx.Batch{}

	for _, id := range ids {
		if err := p.validateID(id); err != nil {
			return transport.NewInvalidOption("invalid storage id; '%s': %s", id, err)
		}

		pgxBatch.Queue(`DELETE FROM `+p.tableName()+` WHERE id = $1`, string(id))
	}

	br := pool.SendBatch(ctx, pgxBatch)
	defer func() { _ = br.Close() }()

	for range ids {
		_, err := br.Exec()
		if err != nil {
			return errors.Errorf("failed to delete item: %w", err)
		}
	}

	return nil
}

func (p *PostgresPartition) Clear(ctx context.Context, req types.ClearRequest) error {
	pool, err := p.conf.getOrCreatePool(ctx)
	if err != nil {
		return err
	}

	if err := p.ensureTable(ctx, pool); err != nil {
		return err
	}

	var conditions []string

	if req.Scheduled {
		conditions = append(conditions, `enqueue_at IS NOT NULL`)
	}

	if req.Queue {
		if req.Destructive {
			conditions = append(conditions, `(enqueue_at IS NULL OR enqueue_at <= NOW())`)
		} else {
			conditions = append(conditions, `(enqueue_at IS NULL OR enqueue_at <= NOW()) AND is_leased = false`)
		}
	}

	if len(conditions) == 0 {
		return nil
	}

	query := `DELETE FROM ` + p.tableName() + ` WHERE ` + conditions[0]
	for i := 1; i < len(conditions); i++ {
		query += ` OR ` + conditions[i]
	}

	_, err = pool.Exec(ctx, query)
	if err != nil {
		return errors.Errorf("clear failed: %w", err)
	}
	return nil
}

func (p *PostgresPartition) Stats(ctx context.Context, stats *types.PartitionStats, now clock.Time) error {
	pool, err := p.conf.getOrCreatePool(ctx)
	if err != nil {
		return err
	}

	if err := p.ensureTable(ctx, pool); err != nil {
		return err
	}

	query := `
		SELECT
			COUNT(*) FILTER (WHERE enqueue_at IS NULL OR enqueue_at <= $1) as total,
			COUNT(*) FILTER (WHERE is_leased = true AND (enqueue_at IS NULL OR enqueue_at <= $1)) as num_leased,
			COUNT(*) FILTER (WHERE enqueue_at IS NOT NULL) as scheduled,
			COALESCE(AVG(EXTRACT(EPOCH FROM ($1 - created_at))) FILTER (WHERE enqueue_at IS NULL OR enqueue_at <= $1), 0) as avg_age_seconds,
			COALESCE(AVG(EXTRACT(EPOCH FROM (lease_deadline - $1))) FILTER (WHERE is_leased = true), 0) as avg_leased_age_seconds
		FROM ` + p.tableName()

	var total, numLeased, scheduled int
	var avgAgeSeconds, avgLeasedAgeSeconds float64

	err = pool.QueryRow(ctx, query, now).Scan(&total, &numLeased, &scheduled, &avgAgeSeconds, &avgLeasedAgeSeconds)
	if err != nil {
		return errors.Errorf("query stats: %w", err)
	}

	stats.Total = total
	stats.NumLeased = numLeased
	stats.Scheduled = scheduled
	stats.AverageAge = clock.Duration(avgAgeSeconds * float64(time.Second))
	stats.AverageLeasedAge = clock.Duration(avgLeasedAgeSeconds * float64(time.Second))

	return nil
}

func (p *PostgresPartition) ScanForScheduled(ctx context.Context, now clock.Time) iter.Seq2[types.Action, error] {
	return func(yield func(types.Action, error) bool) {
		batchSize := p.conf.ScanBatchSize
		if batchSize == 0 {
			batchSize = 1000
		}
		var lastID string

		for {
			if ctx.Err() != nil {
				yield(types.Action{}, ctx.Err())
				return
			}

			pool, err := p.conf.getOrCreatePool(ctx)
			if err != nil {
				yield(types.Action{}, err)
				return
			}

			if err := p.ensureTable(ctx, pool); err != nil {
				yield(types.Action{}, err)
				return
			}

			query := `
				SELECT id, attempts, lease_deadline, expire_deadline
				FROM ` + p.tableName() + `
				WHERE enqueue_at IS NOT NULL
				  AND enqueue_at <= $1
				  AND id > $2
				ORDER BY id
				LIMIT $3`

			rows, err := pool.Query(ctx, query, now, lastID, batchSize)
			if err != nil {
				yield(types.Action{}, err)
				return
			}

			var items []types.Item
			for rows.Next() {
				var item types.Item
				var leaseDeadline, expireDeadline sql.NullTime

				err := rows.Scan(
					&item.ID,
					&item.Attempts,
					&leaseDeadline,
					&expireDeadline,
				)
				if err != nil {
					rows.Close()
					yield(types.Action{}, err)
					return
				}

				if leaseDeadline.Valid {
					item.LeaseDeadline = leaseDeadline.Time
				}
				if expireDeadline.Valid {
					item.ExpireDeadline = expireDeadline.Time
				}

				items = append(items, item)
				lastID = string(item.ID)
			}
			rows.Close()

			if len(items) == 0 {
				return
			}

			for _, item := range items {
				if !yield(types.Action{
					Action:       types.ActionQueueScheduledItem,
					PartitionNum: p.info.PartitionNum,
					Queue:        p.info.Queue.Name,
					Item:         item,
				}, nil) {
					return
				}
			}

			if len(items) < batchSize {
				return
			}
		}
	}
}

func (p *PostgresPartition) ScanForActions(ctx context.Context, now clock.Time) iter.Seq2[types.Action, error] {
	return func(yield func(types.Action, error) bool) {
		batchSize := p.conf.ScanBatchSize
		if batchSize == 0 {
			batchSize = 1000
		}
		var lastID string

		for {
			if ctx.Err() != nil {
				yield(types.Action{}, ctx.Err())
				return
			}

			pool, err := p.conf.getOrCreatePool(ctx)
			if err != nil {
				yield(types.Action{}, err)
				return
			}

			if err := p.ensureTable(ctx, pool); err != nil {
				yield(types.Action{}, err)
				return
			}

			query := `
				SELECT id, is_leased, attempts, lease_deadline, expire_deadline,
				       enqueue_at, created_at, max_attempts, reference, encoding, kind, payload
				FROM ` + p.tableName() + `
				WHERE (enqueue_at IS NULL OR enqueue_at <= $1)
				  AND id > $2
				ORDER BY id
				LIMIT $3`

			rows, err := pool.Query(ctx, query, now, lastID, batchSize)
			if err != nil {
				yield(types.Action{}, err)
				return
			}

			var items []types.Item
			for rows.Next() {
				var item types.Item
				var leaseDeadline, expireDeadline, enqueueAt, createdAt sql.NullTime

				err := rows.Scan(
					&item.ID,
					&item.IsLeased,
					&item.Attempts,
					&leaseDeadline,
					&expireDeadline,
					&enqueueAt,
					&createdAt,
					&item.MaxAttempts,
					&item.Reference,
					&item.Encoding,
					&item.Kind,
					&item.Payload,
				)
				if err != nil {
					rows.Close()
					yield(types.Action{}, err)
					return
				}

				if leaseDeadline.Valid {
					item.LeaseDeadline = leaseDeadline.Time
				}
				if expireDeadline.Valid {
					item.ExpireDeadline = expireDeadline.Time
				}
				if enqueueAt.Valid {
					item.EnqueueAt = enqueueAt.Time
				}
				if createdAt.Valid {
					item.CreatedAt = createdAt.Time
				}

				items = append(items, item)
				lastID = string(item.ID)
			}
			rows.Close()

			if len(items) == 0 {
				return
			}

			for _, item := range items {
				if item.IsLeased && now.After(item.LeaseDeadline) {
					if !yield(types.Action{
						Action:       types.ActionLeaseExpired,
						PartitionNum: p.info.PartitionNum,
						Queue:        p.info.Queue.Name,
						Item:         item,
					}, nil) {
						return
					}
					continue
				}

				p.mu.RLock()
				maxAttempts := p.info.Queue.MaxAttempts
				p.mu.RUnlock()

				if item.IsLeased && maxAttempts != 0 && item.Attempts >= maxAttempts {
					if !yield(types.Action{
						Action:       types.ActionItemMaxAttempts,
						PartitionNum: p.info.PartitionNum,
						Queue:        p.info.Queue.Name,
						Item:         item,
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
						Item:         item,
					}, nil) {
						return
					}
				}
			}

			if len(items) < batchSize {
				return
			}
		}
	}
}

func (p *PostgresPartition) TakeAction(ctx context.Context, batch types.LifeCycleBatch, state *types.PartitionState) error {
	pool, err := p.conf.getOrCreatePool(ctx)
	if err != nil {
		return err
	}

	if err := p.ensureTable(ctx, pool); err != nil {
		return err
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return errors.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	p.mu.Lock()
	for i := range batch.Requests {
		for _, action := range batch.Requests[i].Actions {
			switch action.Action {
			case types.ActionLeaseExpired:
				// Check if the item still exists before processing
				var exists bool
				err := tx.QueryRow(ctx,
					`SELECT EXISTS(SELECT 1 FROM `+p.tableName()+` WHERE id = $1)`,
					string(action.Item.ID)).Scan(&exists)
				if err != nil {
					p.mu.Unlock()
					return errors.Errorf("check item existence: %w", err)
				}

				if !exists {
					if p.conf.Log != nil {
						p.conf.Log.Warn("unable to find item while processing action; ignoring action",
							"id", string(action.Item.ID), "action", types.ActionToString(action.Action))
					}
					continue
				}

				// Generate new ID for the requeued item
				p.uid = p.uid.Next()
				newID := p.uid.String()

				// Truncate timestamps to microseconds to match PostgreSQL precision
				createdAt := timeToMicroseconds(action.Item.CreatedAt)
				expireDeadline := timeToMicroseconds(action.Item.ExpireDeadline)
				enqueueAt := action.Item.EnqueueAt
				if !enqueueAt.IsZero() {
					enqueueAt = timeToMicroseconds(enqueueAt)
				}

				// Delete the old row
				_, err = tx.Exec(ctx, `DELETE FROM `+p.tableName()+` WHERE id = $1`, action.Item.ID)
				if err != nil {
					p.mu.Unlock()
					return errors.Errorf("delete expired item: %w", err)
				}

				// Insert new row with new ID and reset lease fields
				_, err = tx.Exec(ctx, `
					INSERT INTO `+p.tableName()+` (
						id, is_leased, lease_deadline, enqueue_at,
						created_at, expire_deadline, attempts, max_attempts,
						reference, encoding, kind, payload
					)
					VALUES ($1, false, NULL, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
					newID,
					timeToNullable(enqueueAt),
					createdAt,
					expireDeadline,
					action.Item.Attempts,
					action.Item.MaxAttempts,
					action.Item.Reference,
					action.Item.Encoding,
					action.Item.Kind,
					action.Item.Payload)
				if err != nil {
					p.mu.Unlock()
					return errors.Errorf("insert requeued item: %w", err)
				}

			case types.ActionItemExpired:
				_, err = tx.Exec(ctx, `DELETE FROM `+p.tableName()+` WHERE id = $1`, action.Item.ID)
				if err != nil {
					p.mu.Unlock()
					return errors.Errorf("delete expired item: %w", err)
				}

			case types.ActionDeleteItem:
				_, err = tx.Exec(ctx, `DELETE FROM `+p.tableName()+` WHERE id = $1`, action.Item.ID)
				if err != nil {
					p.mu.Unlock()
					return errors.Errorf("delete item: %w", err)
				}

			case types.ActionQueueScheduledItem:
				_, err = tx.Exec(ctx, `UPDATE `+p.tableName()+` SET enqueue_at = NULL WHERE id = $1`, action.Item.ID)
				if err != nil {
					p.mu.Unlock()
					return errors.Errorf("queue scheduled item: %w", err)
				}
			}
		}
	}
	p.mu.Unlock()

	if err := tx.Commit(ctx); err != nil {
		return errors.Errorf("commit transaction: %w", err)
	}

	return nil
}

func (p *PostgresPartition) LifeCycleInfo(ctx context.Context, info *types.LifeCycleInfo) error {
	pool, err := p.conf.getOrCreatePool(ctx)
	if err != nil {
		return err
	}

	if err := p.ensureTable(ctx, pool); err != nil {
		return err
	}

	// Query for both lease expiry and item expiry
	query := `
		SELECT
			COALESCE(MIN(lease_deadline) FILTER (WHERE is_leased = true), '9999-12-31 23:59:59.999999+00'::timestamptz),
			COALESCE(MIN(expire_deadline) FILTER (WHERE enqueue_at IS NULL OR enqueue_at <= NOW()), '9999-12-31 23:59:59.999999+00'::timestamptz)
		FROM ` + p.tableName()

	var nextLeaseExpiry, nextExpireDeadline time.Time
	err = pool.QueryRow(ctx, query).Scan(&nextLeaseExpiry, &nextExpireDeadline)
	if err != nil {
		return errors.Errorf("query lifecycle info: %w", err)
	}

	sentinel := time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
	if nextLeaseExpiry.Before(sentinel) {
		info.NextLeaseExpiry = nextLeaseExpiry
	}

	if nextExpireDeadline.Before(sentinel) {
		info.NextExpireDeadline = nextExpireDeadline
	}

	return nil
}

func (p *PostgresPartition) Info() types.PartitionInfo {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.info
}

func (p *PostgresPartition) UpdateQueueInfo(info types.QueueInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.info.Queue = info
}

func (p *PostgresPartition) Close(ctx context.Context) error {
	p.conf.Close()
	return nil
}
