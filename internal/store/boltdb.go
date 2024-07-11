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
)

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

func (b *BoltStorage) NewQueueStore(opts QueueStoreOptions) (QueueStore, error) {
	f := errors.Fields{"category", "bolt", "func", "NewQueueStore"}

	file := filepath.Join(b.opts.StorageDir, "~queue-storage.db")
	db, err := bolt.Open(file, 0600, bolt.DefaultOptions)
	if err != nil {
		return nil, f.Errorf("while opening db '%s': %w", file, err)
	}
	return &BoltQueueStore{
		db: db,
	}, nil
}

func (b *BoltStorage) ParseID(parse string, id *StorageID) error {
	parts := strings.Split(parse, "~")
	if len(parts) != 2 {
		return errors.New("expected format <queue_name>~<storage_id>")
	}
	id.Queue = parts[0]
	id.ID = []byte(parts[1])
	return nil
}

func (b *BoltStorage) CreateID(queue, id string) string {
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
	f := errors.Fields{"category", "bolt", "func", "NewQueue"}

	file := filepath.Join(b.opts.StorageDir, fmt.Sprintf("%s.db", info.Name))
	db, err := bolt.Open(file, 0600, bolt.DefaultOptions)
	if err != nil {
		return nil, f.Errorf("while opening db '%s': %w", file, err)
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
	f := errors.Fields{"category", "bolt", "func", "Produce"}

	tx, err := q.db.Begin(true)
	if err != nil {
		return f.Errorf("during Begin(): %w", err)
	}

	b := tx.Bucket(bucketName)
	if b == nil {
		return f.Error("bucket does not exist in data file")
	}

	for _, r := range batch.Requests {
		for _, item := range r.Items {
			q.uid = q.uid.Next()
			item.ID = q.uid.String()

			// TODO: Get buffers from memory pool
			var buf bytes.Buffer
			if err := gob.NewEncoder(&buf).Encode(item); err != nil {
				return f.Errorf("during gob.Encode(): %w", err)
			}

			if err := b.Put(q.uid.Bytes(), buf.Bytes()); err != nil {
				return f.Errorf("during Put(): %w", err)
			}

			item.ID = q.storage.CreateID(q.info.Name, item.ID)
		}
	}

	err = tx.Commit()
	if err != nil {
		return f.Errorf("during Commit(): %w", err)
	}
	return nil
}

func (q *BoltQueue) Reserve(_ context.Context, batch types.ReserveBatch, opts ReserveOptions) error {
	f := errors.Fields{"category", "bunt-db", "func", "Reserve"}

	tx, err := q.db.Begin(false)
	if err != nil {
		return f.Errorf("during Begin(false): %w", err)
	}

	b := tx.Bucket(bucketName)
	if b == nil {
		return f.Error("bucket does not exist in data file")
	}

	batchIter := batch.Iterator()
	c := b.Cursor()
	var count int

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
			item.ID = q.storage.CreateID(q.info.Name, item.ID)
			continue
		}
		break
	}

	if err = tx.Commit(); err != nil {
		return f.Errorf("during Commit(): %w", err)
	}

	return nil
}

func (q *BoltQueue) Complete(ctx context.Context, batch types.Batch[types.CompleteRequest]) error {
	f := errors.Fields{"category", "bunt-db", "func", "Complete"}

	tx, err := q.db.Begin(true)
	if err != nil {
		return f.Errorf("during Begin(): %w", err)
	}

	b := tx.Bucket(bucketName)
	if b == nil {
		return f.Error("bucket does not exist in data file")
	}

nextBatch:
	for i := range batch.Requests {
		for _, id := range batch.Requests[i].Ids {
			var sid StorageID
			if err := q.storage.ParseID(id, &sid); err != nil {
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
			if err := gob.NewDecoder(bytes.NewReader(value)).Decode(item); err != nil {
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
	return nil
}

func (q *BoltQueue) Read(ctx context.Context, items *[]*types.Item, pivot string, limit int) error {
	//TODO implement me
	panic("implement me")
}

func (q *BoltQueue) Write(ctx context.Context, items []*types.Item) error {
	//TODO implement me
	panic("implement me")
}

func (q *BoltQueue) Delete(ctx context.Context, ids []string) error {
	//TODO implement me
	panic("implement me")
}

func (q *BoltQueue) Stats(ctx context.Context, stats *Stats) error {
	//TODO implement me
	panic("implement me")
}

func (q *BoltQueue) Close(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

// ---------------------------------------------
// Queue Repository Implementation
// ---------------------------------------------

type BoltQueueStore struct {
	db *bolt.DB
}

var _ QueueStore = &BoltQueueStore{}

func (s BoltQueueStore) Get(ctx context.Context, name string, queue *QueueInfo) error {
	//TODO implement me
	panic("implement me")
}

func (s BoltQueueStore) Set(ctx context.Context, opts QueueInfo) error {
	//TODO implement me
	panic("implement me")
}

func (s BoltQueueStore) List(ctx context.Context, queues *[]*QueueInfo, opts types.ListOptions) error {
	//TODO implement me
	panic("implement me")
}

func (s BoltQueueStore) Delete(ctx context.Context, queueName string) error {
	//TODO implement me
	panic("implement me")
}

func (s BoltQueueStore) Close(_ context.Context) error {
	return s.db.Close()
}
