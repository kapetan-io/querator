package store

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"github.com/kapetan-io/errors"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/tackle/clock"
	"github.com/segmentio/ksuid"
	bolt "go.etcd.io/bbolt"
	"iter"
	"log/slog"
	"path/filepath"
	"sync"
)

var bucketName = []byte("queue")

type BoltConfig struct {
	// StorageDir is the directory where bolt will store its data
	StorageDir string
	// Log is used to log warnings and errors
	Log *slog.Logger
	// Clock is a time provider used to preform time related calculations. It is configurable so that it can
	// be overridden for testing.
	Clock *clock.Provider
}

// ---------------------------------------------
// PartitionStore Implementation
// ---------------------------------------------

type BoltPartitionStore struct {
	conf BoltConfig
}

var _ PartitionStore = &BoltPartitionStore{}

func NewBoltPartitionStore(conf BoltConfig) *BoltPartitionStore {
	return &BoltPartitionStore{conf: conf}
}

func (b BoltPartitionStore) Get(info types.PartitionInfo) Partition {
	return &BoltPartition{
		uid:  ksuid.New(),
		conf: b.conf,
		info: info,
	}
}

// ---------------------------------------------
// Partition Implementation
// ---------------------------------------------

type BoltPartition struct {
	info types.PartitionInfo
	mu   sync.RWMutex
	uid  ksuid.KSUID
	conf BoltConfig
	db   *bolt.DB
}

func (b *BoltPartition) Produce(_ context.Context, batch types.Batch[types.ProduceRequest]) error {
	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			return errors.Errorf("bucket does not exist in data file: %w", err)
		}

		for _, r := range batch.Requests {
			for _, item := range r.Items {
				b.uid = b.uid.Next()
				item.ID = []byte(b.uid.String())
				item.CreatedAt = b.conf.Clock.Now().UTC()

				// TODO: GetByPartition buffers from memory pool
				var buf bytes.Buffer
				if err := gob.NewEncoder(&buf).Encode(item); err != nil {
					return errors.Errorf("during gob.Encode(): %w", err)
				}

				if err := bucket.Put(item.ID, buf.Bytes()); err != nil {
					return errors.Errorf("during Put(): %w", err)
				}
			}
		}
		return nil
	})
}

func (b *BoltPartition) Reserve(_ context.Context, batch types.ReserveBatch, opts ReserveOptions) error {
	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			return errors.Error("bucket does not exist in data file")
		}

		batchIter := batch.Iterator()
		c := bucket.Cursor()
		var count int

		// We preform a full scan of the entire bucket to find our reserved items.
		// I might entertain using an index for this if Bolt becomes a popular choice
		// in production.
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if count >= batch.TotalRequested {
				break
			}

			item := new(types.Item) // TODO: memory pool
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(item); err != nil {
				return errors.Errorf("during Decode(): %w", err)
			}

			if item.IsReserved {
				continue
			}

			item.ReserveDeadline = opts.ReserveDeadline
			item.IsReserved = true
			count++

			// Assign the item to the next waiting reservation in the batch,
			// returns false if there are no more reservations available to fill
			if batchIter.Next(item) {
				// If assignment was a success, then we put the updated item into the db
				var buf bytes.Buffer // TODO: memory pool
				if err := gob.NewEncoder(&buf).Encode(item); err != nil {
					return errors.Errorf("during gob.Encode(): %w", err)
				}

				if err := bucket.Put(item.ID, buf.Bytes()); err != nil {
					return errors.Errorf("during Put(): %w", err)
				}
				continue
			}
			break
		}
		return nil
	})
}

func (b *BoltPartition) Complete(_ context.Context, batch types.Batch[types.CompleteRequest]) error {
	var done bool

	db, err := b.getDB()
	if err != nil {
		return err
	}

	tx, err := db.Begin(true)
	if err != nil {
		return errors.Errorf("during Begin(): %w", err)
	}

	defer func() {
		if !done {
			if err := tx.Rollback(); err != nil {
				b.conf.Log.Error("during Rollback()", "error", err)
			}
		}
	}()

	bucket := tx.Bucket(bucketName)
	if bucket == nil {
		return errors.Error("bucket does not exist in data file")
	}

nextBatch:
	for i := range batch.Requests {
		for _, id := range batch.Requests[i].Ids {
			if err = b.validateID(id); err != nil {
				batch.Requests[i].Err = transport.NewInvalidOption("invalid storage id; '%s': %s", id, err)
				continue nextBatch
			}

			// TODO: Test complete with id's that do not exist in the database
			value := bucket.Get(id)
			if value == nil {
				batch.Requests[i].Err = transport.NewInvalidOption("invalid storage id; '%s' does not exist", id)
				continue nextBatch
			}

			item := new(types.Item) // TODO: memory pool
			if err = gob.NewDecoder(bytes.NewReader(value)).Decode(item); err != nil {
				return errors.Errorf("during Decode(): %w", err)
			}

			if !item.IsReserved {
				batch.Requests[i].Err = transport.NewConflict("item(s) cannot be completed; '%s' is not "+
					"marked as reserved", id)
				continue nextBatch
			}

			if err = bucket.Delete(id); err != nil {
				return errors.Errorf("during Delete(%s): %w", id, err)
			}
		}
	}

	err = tx.Commit()
	if err != nil {
		return errors.Errorf("during Commit(): %w", err)
	}

	done = true
	return nil
}

func (b *BoltPartition) List(_ context.Context, items *[]*types.Item, opts types.ListOptions) error {

	db, err := b.getDB()
	if err != nil {
		return err
	}

	if opts.Pivot != nil {
		if err := b.validateID(opts.Pivot); err != nil {
			return transport.NewInvalidOption("invalid storage id; '%s': %s", opts.Pivot, err)
		}
	}

	return db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			return errors.Error("bucket does not exist in data file")
		}

		c := bucket.Cursor()
		var count int
		var k, v []byte
		if opts.Pivot != nil {
			k, v = c.Seek(opts.Pivot)
			if k == nil {
				return transport.NewInvalidOption("invalid pivot; '%s' does not exist", opts.Pivot)
			}
		} else {
			k, v = c.First()
			if k == nil {
				// TODO: Add a test for this code path, attempt to list an empty queue
				// we get here if the bucket is empty
				return nil
			}
		}

		item := new(types.Item) // TODO: memory pool
		if err := gob.NewDecoder(bytes.NewReader(v)).Decode(item); err != nil {
			return errors.Errorf("during Decode(): %w", err)
		}

		*items = append(*items, item)
		count++

		for k, v = c.Next(); k != nil; k, v = c.Next() {
			if count >= opts.Limit {
				return nil
			}

			item := new(types.Item) // TODO: memory pool
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(item); err != nil {
				return errors.Errorf("during Decode(): %w", err)
			}

			*items = append(*items, item)
			count++
		}
		return nil
	})
}

func (b *BoltPartition) Add(_ context.Context, items []*types.Item) error {

	if len(items) == 0 {
		return transport.NewInvalidOption("items is invalid; cannot be empty")
	}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			return errors.Error("bucket does not exist in data file")
		}

		for _, item := range items {
			b.uid = b.uid.Next()
			item.ID = []byte(b.uid.String())
			item.CreatedAt = b.conf.Clock.Now().UTC()

			// TODO: GetByPartition buffers from memory pool
			var buf bytes.Buffer
			if err := gob.NewEncoder(&buf).Encode(item); err != nil {
				return errors.Errorf("during gob.Encode(): %w", err)
			}

			if err := bucket.Put(item.ID, buf.Bytes()); err != nil {
				return errors.Errorf("during Put(): %w", err)
			}
		}
		return nil
	})
}

func (b *BoltPartition) Delete(_ context.Context, ids []types.ItemID) error {

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			return errors.Error("bucket does not exist in data file")
		}

		for _, id := range ids {
			if err := b.validateID(id); err != nil {
				return transport.NewInvalidOption("invalid storage id; '%s': %s", id, err)
			}
			if err := bucket.Delete(id); err != nil {
				return fmt.Errorf("during delete: %w", err)
			}
		}
		return nil
	})
}

func (b *BoltPartition) Clear(_ context.Context, destructive bool) error {

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(tx *bolt.Tx) error {
		if destructive {
			if err := tx.DeleteBucket(bucketName); err != nil {
				return errors.Errorf("during destructive DeleteBucket(): %w", err)
			}
			if _, err := tx.CreateBucket(bucketName); err != nil {
				return errors.Errorf("while re-creating with CreateBucket()): %w", err)
			}
			return nil
		}

		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			return errors.Error("bucket does not exist in data file")
		}
		c := bucket.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			item := new(types.Item) // TODO: memory pool
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(item); err != nil {
				return errors.Errorf("during Decode(): %w", err)
			}

			// Skip reserved items
			if item.IsReserved {
				continue
			}

			if err := bucket.Delete(k); err != nil {
				return errors.Errorf("during Delete(): %w", err)
			}
		}
		return nil
	})
}

func (b *BoltPartition) Info() types.PartitionInfo {
	defer b.mu.RUnlock()
	b.mu.RLock()
	return b.info
}

func (b *BoltPartition) UpdateQueueInfo(info types.QueueInfo) {
	defer b.mu.Unlock()
	b.mu.Lock()
	b.info.Queue = info
}

func (b *BoltPartition) ActionScan(timeout clock.Duration, now clock.Time) iter.Seq[types.Action] {
	//info := b.Info()

	return func(yield func(types.Action) bool) {
		// TODO(lifecycle)
	}
}

func (b *BoltPartition) TakeAction(ctx context.Context, batch types.Batch[types.LifeCycleRequest]) error {
	return nil // TODO(lifecycle):
}

func (b *BoltPartition) LifeCycleInfo(ctx context.Context, info *types.LifeCycleInfo) error {
	// TODO(lifecycle)
	return nil
}

func (b *BoltPartition) Stats(_ context.Context, stats *types.PartitionStats) error {
	now := b.conf.Clock.Now().UTC()

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			return errors.Error("bucket does not exist in data file")
		}

		c := bucket.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			item := new(types.Item) // TODO: memory pool
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(item); err != nil {
				return errors.Errorf("during Decode(): %w", err)
			}

			stats.Total++
			stats.AverageAge += now.Sub(item.CreatedAt)
			if item.IsReserved {
				stats.AverageReservedAge += item.ReserveDeadline.Sub(now)
				stats.TotalReserved++
			}
		}
		if stats.Total != 0 {
			stats.AverageAge = clock.Duration(int64(stats.AverageAge) / int64(stats.Total))
		}
		if stats.TotalReserved != 0 {
			stats.AverageReservedAge = clock.Duration(int64(stats.AverageReservedAge) / int64(stats.TotalReserved))
		}
		return nil
	})
}

func (b *BoltPartition) Close(_ context.Context) error {
	if b.db != nil {
		return b.db.Close()
	}
	return nil
}

func (b *BoltPartition) validateID(id []byte) error {
	_, err := ksuid.Parse(string(id))
	if err != nil {
		return err
	}
	return nil
}

func (b *BoltPartition) getDB() (*bolt.DB, error) {
	if b.db != nil {
		return b.db, nil
	}

	file := filepath.Join(b.conf.StorageDir, fmt.Sprintf("%s-%06d.db", b.info.Queue.Name, b.info.PartitionNum))

	opts := &bolt.Options{
		FreelistType: bolt.FreelistArrayType,
		Timeout:      clock.Second,
		NoGrowSync:   false,
	}

	db, err := bolt.Open(file, 0600, opts)
	if err != nil {
		return nil, errors.With(
			"partition", b.info.PartitionNum,
			errors.OtelFileName, file).
			Errorf("while opening db: %w", err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		// If the bucket does not exist
		if bucket := tx.Bucket(bucketName); bucket == nil {
			// Create it
			_, err := tx.CreateBucket(bucketName)
			return err
		}
		return nil
	})
	if err != nil {
		return nil, errors.With(
			"partition", b.info.PartitionNum,
			errors.OtelFileName, file).
			Errorf("while creating bucket: %w", err)
	}

	b.db = db
	return db, nil
}

// ---------------------------------------------
// Queues Implementation
// ---------------------------------------------

func NewBoltQueues(conf BoltConfig) Queues {
	return &BoltQueues{
		conf: conf,
	}
}

type BoltQueues struct {
	QueuesValidation
	db   *bolt.DB
	conf BoltConfig
}

var _ Queues = &BoltQueues{}

func (b *BoltQueues) getDB() (*bolt.DB, error) {
	if b.db != nil {
		return b.db, nil
	}

	// We store info about the queues in a single db file. We prefix it with `~` to make it
	// impossible for someone to create a queue with the same name.
	file := filepath.Join(b.conf.StorageDir, "~queue-storage.db")
	db, err := bolt.Open(file, 0600, bolt.DefaultOptions)
	if err != nil {
		return nil, errors.With(errors.OtelFileName, file).Errorf("while opening db: %w", err)
	}

	// Create the bucket if it doesn't already exist
	err = db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			_, err := tx.CreateBucket(bucketName)
			if err != nil {
				if !errors.Is(err, bolt.ErrBucketExists) {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, errors.With(errors.OtelFileName, file).Errorf("while creating bucket: %w", err)
	}
	b.db = db
	return db, nil
}

func (b *BoltQueues) Get(_ context.Context, name string, queue *types.QueueInfo) error {
	if err := b.validateGet(name); err != nil {
		return err
	}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			return errors.Error("bucket does not exist in data file")
		}

		v := bucket.Get([]byte(name))
		if v == nil {
			return ErrQueueNotExist
		}

		if err := gob.NewDecoder(bytes.NewReader(v)).Decode(queue); err != nil {
			return errors.Errorf("during Decode(): %w", err)
		}
		return nil
	})
}

func (b *BoltQueues) Add(_ context.Context, info types.QueueInfo) error {
	if err := b.validateAdd(info); err != nil {
		return err
	}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			return errors.Error("bucket does not exist in data file")
		}

		// If the queue already exists in the store
		if bucket.Get([]byte(info.Name)) != nil {
			return transport.NewInvalidOption("invalid queue; '%s' already exists", info.Name)
		}

		var buf bytes.Buffer // TODO: memory pool
		if err := gob.NewEncoder(&buf).Encode(info); err != nil {
			return errors.Errorf("during gob.Encode(): %w", err)
		}

		if err := bucket.Put([]byte(info.Name), buf.Bytes()); err != nil {
			return errors.Errorf("during Put(): %w", err)
		}
		return nil
	})
}

func (b *BoltQueues) Update(_ context.Context, info types.QueueInfo) error {
	db, err := b.getDB()
	if err != nil {
		return err
	}

	if err := b.validateQueueName(info); err != nil {
		return err
	}

	return db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			return errors.Error("bucket does not exist in data file")
		}

		v := bucket.Get([]byte(info.Name))
		if v == nil {
			return ErrQueueNotExist
		}

		var found types.QueueInfo
		if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&found); err != nil {
			return errors.Errorf("during Decode(): %w", err)
		}

		found.Update(info)

		if err := b.validateUpdate(found); err != nil {
			return err
		}

		if found.ReserveTimeout > found.ExpireTimeout {
			return transport.NewInvalidOption("reserve timeout is too long; %s cannot be greater than the "+
				"expire timeout %s", info.ReserveTimeout.String(), found.ExpireTimeout.String())
		}

		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(found); err != nil {
			return errors.Errorf("during gob.Encode(): %w", err)
		}

		if err := bucket.Put([]byte(info.Name), buf.Bytes()); err != nil {
			return errors.Errorf("during Put(): %w", err)
		}
		return nil
	})
}

func (b *BoltQueues) List(_ context.Context, queues *[]types.QueueInfo, opts types.ListOptions) error {
	if err := b.validateList(opts); err != nil {
		return err
	}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			return errors.Error("bucket does not exist in data file")
		}

		c := bucket.Cursor()
		var count int
		var k, v []byte
		if opts.Pivot != nil {
			k, v = c.Seek(opts.Pivot)
			if k == nil {
				return transport.NewInvalidOption("invalid pivot; '%s' does not exist", opts.Pivot)
			}

		} else {
			k, v = c.First()
			if k == nil {
				// TODO: Add a test for this code path, attempt to list an empty queue
				// we get here if the bucket is empty
				return nil
			}
		}

		var info types.QueueInfo
		if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&info); err != nil {
			return errors.Errorf("during Decode(): %w", err)
		}
		*queues = append(*queues, info)
		count++

		for k, v = c.Next(); k != nil; k, v = c.Next() {
			if count >= opts.Limit {
				return nil
			}

			var info types.QueueInfo
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&info); err != nil {
				return errors.Errorf("during Decode(): %w", err)
			}
			*queues = append(*queues, info)
			count++
		}
		return nil
	})
}

func (b *BoltQueues) Delete(_ context.Context, name string) error {
	if err := b.validateDelete(name); err != nil {
		return err
	}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			return errors.Error("bucket does not exist in data file")
		}

		if err := bucket.Delete([]byte(name)); err != nil {
			return errors.Errorf("during Delete(%s): %w", name, err)
		}
		return nil
	})
}

func (b *BoltQueues) Close(_ context.Context) error {
	err := b.db.Close()
	b.db = nil
	return err
}
