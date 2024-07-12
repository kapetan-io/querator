package store

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"github.com/duh-rpc/duh-go"
	"github.com/kapetan-io/errors"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/tackle/set"
	"github.com/segmentio/ksuid"
	bolt "go.etcd.io/bbolt"
	"log/slog"
	"path/filepath"
	"strings"
	"time"
)

var ErrQueueNotExist = transport.NewRequestFailed("queue does not exist")
var bucketName = []byte("queue")

type BoltOptions struct {
	// StorageDir is the directory where bolt will store its data
	StorageDir string
	// Logger is used to log warnings and errors
	Logger duh.StandardLogger
}

// ---------------------------------------------
// Storage Implementation
// ---------------------------------------------

type BoltStorage struct {
	opts BoltOptions
}

var _ Storage = &BoltStorage{}

func (b *BoltStorage) ParseID(parse string, id *StorageID) error {
	parts := strings.Split(parse, "~")
	if len(parts) != 2 {
		return errors.New("expected format <queue_name>~<storage_id>")
	}
	id.Queue = parts[0]
	id.ID = []byte(parts[1])
	return nil
}

func (b *BoltStorage) BuildStorageID(queue, id string) string {
	return fmt.Sprintf("%s~%s", queue, id)
}

func (b *BoltStorage) Close(_ context.Context) error {
	return nil
}

func NewBoltStorage(opts BoltOptions) *BoltStorage {
	set.Default(&opts.Logger, slog.Default())
	set.Default(&opts.StorageDir, ".")

	return &BoltStorage{opts: opts}
}

// ---------------------------------------------
// Queue Implementation
// ---------------------------------------------

func (b *BoltStorage) NewQueue(info QueueInfo) (Queue, error) {
	f := errors.Fields{"category", "bolt", "func", "Storage.NewQueue"}

	file := filepath.Join(b.opts.StorageDir, fmt.Sprintf("%s.db", info.Name))
	db, err := bolt.Open(file, 0600, bolt.DefaultOptions)
	if err != nil {
		return nil, f.Errorf("while opening db '%s': %w", file, err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket(bucketName)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, f.Errorf("while creating bucket '%s': %w", file, err)
	}

	return &BoltQueue{
		uid:     ksuid.New(),
		info:    info,
		db:      db,
		storage: b,
	}, nil
}

type BoltQueue struct {
	uid     ksuid.KSUID
	storage *BoltStorage
	info    QueueInfo
	db      *bolt.DB
}

func (q *BoltQueue) Produce(_ context.Context, batch types.Batch[types.ProduceRequest]) error {
	f := errors.Fields{"category", "bolt", "func", "Queue.Produce"}

	return q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b == nil {
			return f.Error("bucket does not exist in data file")
		}

		for _, r := range batch.Requests {
			for _, item := range r.Items {
				q.uid = q.uid.Next()
				item.ID = q.uid.String()
				item.CreatedAt = time.Now().UTC()

				// TODO: Get buffers from memory pool
				var buf bytes.Buffer
				if err := gob.NewEncoder(&buf).Encode(item); err != nil {
					return f.Errorf("during gob.Encode(): %w", err)
				}

				if err := b.Put([]byte(item.ID), buf.Bytes()); err != nil {
					return f.Errorf("during Put(): %w", err)
				}

				item.ID = q.storage.BuildStorageID(q.info.Name, item.ID)
			}
		}
		return nil
	})

}

func (q *BoltQueue) Reserve(_ context.Context, batch types.ReserveBatch, opts ReserveOptions) error {
	f := errors.Fields{"category", "bolt", "func", "Queue.Reserve"}
	return q.db.Update(func(tx *bolt.Tx) error {

		b := tx.Bucket(bucketName)
		if b == nil {
			return f.Error("bucket does not exist in data file")
		}

		batchIter := batch.Iterator()
		c := b.Cursor()
		var count int

		// We preform a full scan of the entire bucket to find our reserved items.
		// I might entertain using an index for this if Bolt becomes a popular choice
		// in production.
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if count >= batch.Total {
				break
			}

			item := new(types.Item) // TODO: memory pool
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(item); err != nil {
				return f.Errorf("during Decode(): %w", err)
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
					return f.Errorf("during gob.Encode(): %w", err)
				}

				if err := b.Put([]byte(item.ID), buf.Bytes()); err != nil {
					return f.Errorf("during Put(): %w", err)
				}
				item.ID = q.storage.BuildStorageID(q.info.Name, item.ID)
				continue
			}
			break
		}
		return nil
	})
}

func (q *BoltQueue) Complete(_ context.Context, batch types.Batch[types.CompleteRequest]) error {
	f := errors.Fields{"category", "bolt", "func", "Queue.Complete"}
	var done bool

	tx, err := q.db.Begin(true)
	if err != nil {
		return f.Errorf("during Begin(): %w", err)
	}

	defer func() {
		if !done {
			if err := tx.Rollback(); err != nil {
				q.storage.opts.Logger.Error("during Rollback()", "error", err)
			}
		}
	}()

	b := tx.Bucket(bucketName)
	if b == nil {
		return f.Error("bucket does not exist in data file")
	}

nextBatch:
	for i := range batch.Requests {
		for _, id := range batch.Requests[i].Ids {
			var sid StorageID
			if err = q.storage.ParseID(id, &sid); err != nil {
				batch.Requests[i].Err = transport.NewInvalidOption("invalid storage id; '%s': %s", id, err)
				continue nextBatch
			}

			// TODO: Test complete with id's that do not exist in the database
			value := b.Get(sid.ID)
			if value == nil {
				batch.Requests[i].Err = transport.NewInvalidOption("invalid storage id; '%s' does not exist", id)
				continue nextBatch
			}

			item := new(types.Item) // TODO: memory pool
			if err = gob.NewDecoder(bytes.NewReader(value)).Decode(item); err != nil {
				return f.Errorf("during Decode(): %w", err)
			}

			if !item.IsReserved {
				batch.Requests[i].Err = transport.NewConflict("item(s) cannot be completed; '%s' is not "+
					"marked as reserved", sid.ID)
				continue nextBatch
			}

			if err = b.Delete(sid.ID); err != nil {
				return f.Errorf("during Delete(%s): %w", sid.ID, err)
			}
		}
	}

	err = tx.Commit()
	if err != nil {
		return f.Errorf("during Commit(): %w", err)
	}

	done = true
	return nil
}

func (q *BoltQueue) List(_ context.Context, items *[]*types.Item, opts types.ListOptions) error {
	f := errors.Fields{"category", "bolt", "func", "Queue.List"}

	var sid StorageID
	if opts.Pivot != "" {
		if err := q.storage.ParseID(opts.Pivot, &sid); err != nil {
			return transport.NewInvalidOption("invalid storage id; '%s': %s", opts.Pivot, err)
		}
	}

	return q.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b == nil {
			return f.Error("bucket does not exist in data file")
		}

		c := b.Cursor()
		var count int
		var k, v []byte
		if sid.ID != nil {
			k, v = c.Seek(sid.ID)
			if k == nil {
				return transport.NewInvalidOption("invalid pivot; '%s' does not exist", sid.String())
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
			return f.Errorf("during Decode(): %w", err)
		}

		item.ID = q.storage.BuildStorageID(q.info.Name, item.ID)
		*items = append(*items, item)
		count++

		for k, v = c.Next(); k != nil; k, v = c.Next() {
			if count >= opts.Limit {
				return nil
			}

			item := new(types.Item) // TODO: memory pool
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(item); err != nil {
				return f.Errorf("during Decode(): %w", err)
			}

			item.ID = q.storage.BuildStorageID(q.info.Name, item.ID)
			*items = append(*items, item)
			count++
		}
		return nil
	})
}

func (q *BoltQueue) Add(_ context.Context, items []*types.Item) error {
	f := errors.Fields{"category", "bolt", "func", "Queue.Add"}

	return q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b == nil {
			return f.Error("bucket does not exist in data file")
		}

		for _, item := range items {
			q.uid = q.uid.Next()
			item.ID = q.uid.String()
			item.CreatedAt = time.Now().UTC()

			// TODO: Get buffers from memory pool
			var buf bytes.Buffer
			if err := gob.NewEncoder(&buf).Encode(item); err != nil {
				return f.Errorf("during gob.Encode(): %w", err)
			}

			if err := b.Put([]byte(item.ID), buf.Bytes()); err != nil {
				return f.Errorf("during Put(): %w", err)
			}

			item.ID = q.storage.BuildStorageID(q.info.Name, item.ID)
		}
		return nil
	})
}

func (q *BoltQueue) Delete(_ context.Context, ids []string) error {
	f := errors.Fields{"category", "bolt", "func", "Queue.Delete"}

	return q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b == nil {
			return f.Error("bucket does not exist in data file")
		}

		for _, id := range ids {
			var sid StorageID
			if err := q.storage.ParseID(id, &sid); err != nil {
				return transport.NewInvalidOption("invalid storage id; '%s': %s", id, err)
			}
			if err := b.Delete(sid.ID); err != nil {
				return fmt.Errorf("during delete: %w", err)
			}
		}
		return nil
	})
}

func (q *BoltQueue) Stats(_ context.Context, stats *types.QueueStats) error {
	f := errors.Fields{"category", "bunt-db", "func", "Queue.Stats"}
	now := time.Now().UTC()

	return q.db.View(func(tx *bolt.Tx) error {

		b := tx.Bucket(bucketName)
		if b == nil {
			return f.Error("bucket does not exist in data file")
		}

		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			item := new(types.Item) // TODO: memory pool
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(item); err != nil {
				return f.Errorf("during Decode(): %w", err)
			}

			stats.Total++
			stats.AverageAge += item.CreatedAt.Sub(now)
			if item.IsReserved {
				stats.AverageReservedAge += item.ReserveDeadline.Sub(now)
				stats.TotalReserved++
			}
		}
		stats.AverageAge = time.Duration(int64(stats.AverageAge) / int64(stats.Total))
		stats.AverageReservedAge = time.Duration(int64(stats.AverageReservedAge) / int64(stats.TotalReserved))
		return nil
	})
}

func (q *BoltQueue) Close(_ context.Context) error {
	return q.db.Close()
}

// ---------------------------------------------
// Queue Repository Implementation
// ---------------------------------------------

func (b *BoltStorage) NewQueueStore(opts QueueStoreOptions) (QueueStore, error) {
	f := errors.Fields{"category", "bolt", "func", "Storage.NewQueueStore"}

	// We store info about the queues in a single db file. We prefix it with `~` to make it
	// impossible for someone to create a queue with the same name.
	file := filepath.Join(b.opts.StorageDir, "~queue-storage.db")
	db, err := bolt.Open(file, 0600, bolt.DefaultOptions)
	if err != nil {
		return nil, f.Errorf("while opening db '%s': %w", file, err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket(bucketName)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, f.Errorf("while creating bucket '%s': %w", file, err)
	}

	return &BoltQueueStore{
		db: db,
	}, nil
}

type BoltQueueStore struct {
	db *bolt.DB
}

var _ QueueStore = &BoltQueueStore{}

func (s BoltQueueStore) Get(_ context.Context, name string, queue *QueueInfo) error {
	f := errors.Fields{"category", "bolt", "func", "QueueStore.Get"}
	return s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b == nil {
			return f.Error("bucket does not exist in data file")
		}

		v := b.Get([]byte(name))
		if v == nil {
			return ErrQueueNotExist
		}

		if err := gob.NewDecoder(bytes.NewReader(v)).Decode(queue); err != nil {
			return f.Errorf("during Decode(): %w", err)
		}
		return nil
	})
}

func (s BoltQueueStore) Set(_ context.Context, info QueueInfo) error {
	f := errors.Fields{"category", "bolt", "func", "QueueStore.Set"}

	if strings.TrimSpace(info.Name) == "" {
		return f.Error("info.Name is required")
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b == nil {
			return f.Error("bucket does not exist in data file")
		}

		var buf bytes.Buffer // TODO: memory pool
		if err := gob.NewEncoder(&buf).Encode(info); err != nil {
			return f.Errorf("during gob.Encode(): %w", err)
		}

		if err := b.Put([]byte(info.Name), buf.Bytes()); err != nil {
			return f.Errorf("during Put(): %w", err)
		}
		return nil
	})
}

func (s BoltQueueStore) List(_ context.Context, queues *[]*QueueInfo, opts types.ListOptions) error {
	f := errors.Fields{"category", "bolt", "func", "QueueStore.List"}

	return s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b == nil {
			return f.Error("bucket does not exist in data file")
		}

		c := b.Cursor()
		var count int
		if opts.Pivot == "" {
			k, v := c.Seek([]byte(opts.Pivot))
			if k == nil {
				return transport.NewInvalidOption("invalid pivot; '%s' does not exist", opts.Pivot)
			}

			info := new(QueueInfo)
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(info); err != nil {
				return f.Errorf("during Decode(): %w", err)
			}
			*queues = append(*queues, info)
			count++
		}

		for k, v := c.Next(); k != nil; k, v = c.Next() {
			if count >= opts.Limit {
				return nil
			}

			info := new(QueueInfo)
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(info); err != nil {
				return f.Errorf("during Decode(): %w", err)
			}
			*queues = append(*queues, info)
			count++
		}
		return nil
	})
}

func (s BoltQueueStore) Delete(_ context.Context, name string) error {
	f := errors.Fields{"category", "bolt", "func", "QueueStore.Delete"}

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b == nil {
			return f.Error("bucket does not exist in data file")
		}

		if err := b.Delete([]byte(name)); err != nil {
			return f.Errorf("during Delete(%s): %w", name, err)
		}
		return nil
	})
}

func (s BoltQueueStore) Close(_ context.Context) error {
	return s.db.Close()
}
