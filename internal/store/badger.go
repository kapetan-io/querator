package store

import (
	"bytes"
	"context"
	"encoding/gob"
	errors2 "errors"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"github.com/duh-rpc/duh-go"
	"github.com/kapetan-io/errors"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/kapetan-io/tackle/set"
	"github.com/segmentio/ksuid"
	"log/slog"
	"os"
	"path/filepath"
)

type BadgerConfig struct {
	// StorageDir is the directory where badger will store its data
	StorageDir string
	// Logger is used to log warnings and errors
	Logger duh.StandardLogger
	// Clock is a time provider used to preform time related calculations. It is configurable so that it can
	// be overridden for testing.
	Clock *clock.Provider
}

// ---------------------------------------------
// Storage Implementation
// ---------------------------------------------

type BadgerStorage struct {
	conf BadgerConfig
}

var _ Storage = &BadgerStorage{}

func (b *BadgerStorage) ParseID(parse types.ItemID, id *StorageID) error {
	parts := bytes.Split(parse, []byte("~"))
	if len(parts) != 2 {
		return errors.New("expected format <queue_name>~<storage_id>")
	}
	id.Queue = string(parts[0])
	id.ID = parts[1]
	return nil
}

func (b *BadgerStorage) BuildStorageID(queue string, id []byte) types.ItemID {
	return append([]byte(queue+"~"), id...)
}

func (b *BadgerStorage) Close(_ context.Context) error {
	return nil
}

func NewBadgerStorage(conf BadgerConfig) *BadgerStorage {
	set.Default(&conf.Logger, slog.Default())
	set.Default(&conf.StorageDir, ".")
	set.Default(&conf.Clock, clock.NewProvider())

	return &BadgerStorage{conf: conf}
}

// ---------------------------------------------
// Queue Implementation
// ---------------------------------------------

func (b *BadgerStorage) NewQueue(info types.QueueInfo) (Queue, error) {
	f := errors.Fields{"category", "badger", "func", "Storage.NewQueue"}

	dbDir := filepath.Join(b.conf.StorageDir, fmt.Sprintf("%s-%s", info.Name, bucketName))

	db, err := badger.Open(badger.DefaultOptions(dbDir))
	if err != nil {
		return nil, f.Errorf("while opening db '%s': %w", dbDir, err)
	}

	return &BadgerQueue{
		uid:    ksuid.New(),
		info:   info,
		db:     db,
		parent: b,
	}, nil
}

type BadgerQueue struct {
	info   types.QueueInfo
	parent *BadgerStorage
	uid    ksuid.KSUID
	db     *badger.DB
}

func (q *BadgerQueue) Produce(_ context.Context, batch types.Batch[types.ProduceRequest]) error {
	f := errors.Fields{"category", "badger", "func", "Queue.Produce"}

	return q.db.Update(func(txn *badger.Txn) error {

		for _, r := range batch.Requests {
			for _, item := range r.Items {
				q.uid = q.uid.Next()
				item.ID = []byte(q.uid.String())
				item.CreatedAt = q.parent.conf.Clock.Now().UTC()

				// TODO: Get buffers from memory pool
				var buf bytes.Buffer
				if err := gob.NewEncoder(&buf).Encode(item); err != nil {
					return f.Errorf("during gob.Encode(): %w", err)
				}

				if err := txn.Set(item.ID, buf.Bytes()); err != nil {
					return f.Errorf("during Put(): %w", err)
				}

				item.ID = q.parent.BuildStorageID(q.info.Name, item.ID)
			}
		}
		return nil
	})
}

func (q *BadgerQueue) Reserve(_ context.Context, batch types.ReserveBatch, opts ReserveOptions) error {
	f := errors.Fields{"category", "badger", "func", "Queue.Reserve"}
	return q.db.Update(func(txn *badger.Txn) error {

		batchIter := batch.Iterator()
		var count int

		err := q.db.Update(func(txn *badger.Txn) error {
			iter := txn.NewIterator(badger.DefaultIteratorOptions)
			defer iter.Close()
			for iter.Rewind(); iter.Valid(); iter.Next() {
				if count >= batch.Total {
					break
				}

				var v []byte
				v, err := iter.Item().ValueCopy(v)
				if err != nil {
					return f.Errorf("while getting value: %w", err)
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

					if err := txn.Set(item.ID, buf.Bytes()); err != nil {
						return f.Errorf("during Put(): %w", err)
					}
					item.ID = q.parent.BuildStorageID(q.info.Name, item.ID)
					continue
				}
				break
			}
			return nil
		})
		return err
	})
}

func (q *BadgerQueue) Complete(_ context.Context, batch types.Batch[types.CompleteRequest]) error {
	f := errors.Fields{"category", "badger", "func", "Queue.Complete"}
	var done bool
	var err error

	txn := q.db.NewTransaction(true)

	defer func() {
		if !done {
			txn.Discard()
		}
	}()

nextBatch:
	for i := range batch.Requests {
		for _, id := range batch.Requests[i].Ids {
			var sid StorageID
			if err = q.parent.ParseID(id, &sid); err != nil {
				batch.Requests[i].Err = transport.NewInvalidOption("invalid storage id; '%s': %s", id, err)
				continue nextBatch
			}

			// TODO: Test complete with id's that do not exist in the database
			kvItem, err := txn.Get(sid.ID)
			if err != nil {
				batch.Requests[i].Err = transport.NewInvalidOption("invalid storage id; '%s' does not exist", id)
				continue nextBatch
			}
			var v []byte
			v, err = kvItem.ValueCopy(v)
			if err != nil {
				batch.Requests[i].Err = transport.NewInvalidOption("invalid storage id; '%s' does not exist", id)
				continue nextBatch
			}

			item := new(types.Item) // TODO: memory pool
			if err = gob.NewDecoder(bytes.NewReader(v)).Decode(item); err != nil {
				return f.Errorf("during Decode(): %w", err)
			}

			if !item.IsReserved {
				batch.Requests[i].Err = transport.NewConflict("item(s) cannot be completed; '%s' is not "+
					"marked as reserved", sid.ID)
				continue nextBatch
			}

			if err = txn.Delete(sid.ID); err != nil {
				return f.Errorf("during Delete(%s): %w", sid.ID, err)
			}
		}
	}

	err = txn.Commit()
	if err != nil {
		return f.Errorf("during Commit(): %w", err)
	}

	done = true
	return nil
}

func (q *BadgerQueue) List(_ context.Context, items *[]*types.Item, opts types.ListOptions) error {
	f := errors.Fields{"category", "badger", "func", "Queue.List"}

	var sid StorageID
	if opts.Pivot != nil {
		if err := q.parent.ParseID(opts.Pivot, &sid); err != nil {
			return transport.NewInvalidOption("invalid storage id; '%s': %s", opts.Pivot, err)
		}
	}

	return q.db.View(func(txn *badger.Txn) error {

		var count int
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		if sid.ID != nil {
			iter.Seek(sid.ID)
			if !iter.Valid() {
				return transport.NewInvalidOption("invalid pivot; '%s' does not exist", sid.String())
			}
		} else {
			iter.Rewind()
		}

		for ; iter.Valid(); iter.Next() {
			var v []byte
			v, err := iter.Item().ValueCopy(v)
			if err != nil {
				return f.Errorf("during Get value: %w", err)
			}

			item := new(types.Item) // TODO: memory pool
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(item); err != nil {
				return f.Errorf("during Decode(): %w", err)
			}

			item.ID = q.parent.BuildStorageID(q.info.Name, item.ID)
			*items = append(*items, item)
			count++

			if count >= opts.Limit {
				return nil
			}
		}
		return nil
	})
}

func (q *BadgerQueue) Add(_ context.Context, items []*types.Item) error {
	f := errors.Fields{"category", "badger", "func", "Queue.Add"}

	return q.db.Update(func(txn *badger.Txn) error {

		for _, item := range items {
			q.uid = q.uid.Next()
			item.ID = []byte(q.uid.String())
			item.CreatedAt = q.parent.conf.Clock.Now().UTC()

			// TODO: Get buffers from memory pool
			var buf bytes.Buffer
			if err := gob.NewEncoder(&buf).Encode(item); err != nil {
				return f.Errorf("during gob.Encode(): %w", err)
			}

			if err := txn.Set(item.ID, buf.Bytes()); err != nil {
				return f.Errorf("during Put(): %w", err)
			}

			item.ID = q.parent.BuildStorageID(q.info.Name, item.ID)
		}
		return nil
	})
}

func (q *BadgerQueue) Delete(_ context.Context, ids []types.ItemID) error {
	f := errors.Fields{"category", "badger", "func", "Queue.Delete"}

	return q.db.Update(func(txn *badger.Txn) error {

		for _, id := range ids {
			var sid StorageID
			if err := q.parent.ParseID(id, &sid); err != nil {
				return transport.NewInvalidOption("invalid storage id; '%s': %s", id, err)
			}
			if err := txn.Delete(sid.ID); err != nil {
				return f.Errorf("during delete: %w", err)
			}
		}
		return nil
	})
}

func (q *BadgerQueue) Clear(_ context.Context, destructive bool) error {
	f := errors.Fields{"category", "badger", "func", "Queue.Delete"}

	return q.db.Update(func(txn *badger.Txn) error {
		if destructive {
			err := q.db.DropAll()
			if err != nil {
				return f.Errorf("during destructive DropAll(): %w", err)
			}
			return nil
		}

		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		for iter.Rewind(); iter.Valid(); iter.Next() {
			var k, v []byte
			k = iter.Item().KeyCopy(k)
			v, err := iter.Item().ValueCopy(v)
			if err != nil {
				return f.Errorf("during Get value: %w", err)
			}

			item := new(types.Item) // TODO: memory pool
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(item); err != nil {
				return f.Errorf("during Decode(): %w", err)
			}

			// Skip reserved items
			if item.IsReserved {
				continue
			}

			if err := txn.Delete(k); err != nil {
				return f.Errorf("during Delete(): %w", err)
			}
		}
		return nil
	})
}

func (q *BadgerQueue) Stats(_ context.Context, stats *types.QueueStats) error {
	f := errors.Fields{"category", "bunt-db", "func", "Queue.Stats"}
	now := q.parent.conf.Clock.Now().UTC()

	return q.db.View(func(txn *badger.Txn) error {

		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		for iter.Rewind(); iter.Valid(); iter.Next() {
			var v []byte
			v, err := iter.Item().ValueCopy(v)
			if err != nil {
				return f.Errorf("during Get value: %w", err)
			}

			item := new(types.Item) // TODO: memory pool
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(item); err != nil {
				return f.Errorf("during Decode(): %w", err)
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

func (q *BadgerQueue) Close(_ context.Context) error {
	return q.db.Close()
}

// ---------------------------------------------
// Queue Repository Implementation
// ---------------------------------------------

func (b *BadgerStorage) NewQueuesStore(conf QueuesStoreConfig) (QueuesStore, error) {
	f := errors.Fields{"category", "badger", "func", "Storage.NewQueuesStore"}

	dbDir := filepath.Join(b.conf.StorageDir, fmt.Sprintf("%s-%s", "queue-storage", bucketName))
	db, err := badger.Open(badger.DefaultOptions(dbDir))
	if err != nil {
		return nil, f.Errorf("while opening db '%s': %w", dbDir, err)
	}

	return &BadgerQueuesStore{
		db: db,
	}, nil
}

type BadgerQueuesStore struct {
	QueuesValidation
	db *badger.DB
}

var _ QueuesStore = &BadgerQueuesStore{}

func (s BadgerQueuesStore) Get(_ context.Context, name string, queue *types.QueueInfo) error {
	f := errors.Fields{"category", "badger", "func", "QueuesStore.Get"}

	if err := s.validateGet(name); err != nil {
		return err
	}

	return s.db.View(func(txn *badger.Txn) error {

		kvItem, err := txn.Get([]byte(name))
		if err != nil {
			return ErrQueueNotExist
		}

		var v []byte
		v, err = kvItem.ValueCopy(v)
		if err != nil {
			return f.Errorf("during Get value(): %w", err)
		}
		if err := gob.NewDecoder(bytes.NewReader(v)).Decode(queue); err != nil {
			return f.Errorf("during Decode(): %w", err)
		}
		return nil
	})
}

func (s BadgerQueuesStore) Add(_ context.Context, info types.QueueInfo) error {
	f := errors.Fields{"category", "badger", "func", "QueuesStore.Add"}

	if err := s.validateAdd(info); err != nil {
		return err
	}

	return s.db.Update(func(txn *badger.Txn) error {

		// If the queue already exists in the store
		kvItem, _ := txn.Get([]byte(info.Name))
		if kvItem != nil {
			return transport.NewInvalidOption("invalid queue; '%s' already exists", info.Name)
		}

		var buf bytes.Buffer // TODO: memory pool
		if err := gob.NewEncoder(&buf).Encode(info); err != nil {
			return f.Errorf("during gob.Encode(): %w", err)
		}

		if err := txn.Set([]byte(info.Name), buf.Bytes()); err != nil {
			return f.Errorf("during Put(): %w", err)
		}
		return nil
	})
}

func (s BadgerQueuesStore) Update(_ context.Context, info types.QueueInfo) error {
	f := errors.Fields{"category", "badger", "func", "QueuesStore.Update"}

	if err := s.validateUpdate(info); err != nil {
		return err
	}

	return s.db.Update(func(txn *badger.Txn) error {

		kvItem, err := txn.Get([]byte(info.Name))
		if err != nil {
			if errors2.Is(err, badger.ErrKeyNotFound) {
				return ErrQueueNotExist
			}
			return f.Errorf("during Get(): %w", err)
		}

		var v []byte
		v, err = kvItem.ValueCopy(v)
		if err != nil {
			return f.Errorf("during Get value(): %w", err)
		}

		var found types.QueueInfo
		if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&found); err != nil {
			return f.Errorf("during Decode(): %w", err)
		}

		found.Update(info)

		if found.ReserveTimeout > found.DeadTimeout {
			return transport.NewInvalidOption("reserve timeout is too long; %s cannot be greater than the "+
				"dead timeout %s", info.ReserveTimeout.String(), found.DeadTimeout.String())
		}

		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(found); err != nil {
			return f.Errorf("during gob.Encode(): %w", err)
		}

		if err := txn.Set([]byte(info.Name), buf.Bytes()); err != nil {
			return f.Errorf("during Put(): %w", err)
		}
		return nil
	})
}

func (s BadgerQueuesStore) List(_ context.Context, queues *[]types.QueueInfo, opts types.ListOptions) error {
	f := errors.Fields{"category", "badger", "func", "QueuesStore.List"}

	if err := s.validateList(opts); err != nil {
		return err
	}

	return s.db.View(func(txn *badger.Txn) error {
		var count int
		var v []byte
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		if opts.Pivot != nil {
			iter.Seek(opts.Pivot)
			if !iter.Valid() {
				return transport.NewInvalidOption("invalid pivot; '%s' does not exist", opts.Pivot)
			}
		} else {
			iter.Rewind()
		}

		for ; iter.Valid(); iter.Next() {
			v, err := iter.Item().ValueCopy(v)
			if err != nil {
				return f.Errorf("during Get value(): %w", err)
			}

			var info types.QueueInfo
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&info); err != nil {
				return f.Errorf("during Decode(): %w", err)
			}
			*queues = append(*queues, info)
			count++

			if count >= opts.Limit {
				return nil
			}
		}
		return nil
	})
}

func (s BadgerQueuesStore) Delete(_ context.Context, name string) error {
	f := errors.Fields{"category", "badger", "func", "QueuesStore.Delete"}

	if err := s.validateDelete(name); err != nil {
		return err
	}

	return s.db.Update(func(txn *badger.Txn) error {

		if err := txn.Delete([]byte(name)); err != nil {
			return f.Errorf("during Delete(%s): %w", name, err)
		}
		return nil
	})
}

func (s BadgerQueuesStore) Close(_ context.Context) error {
	return s.db.Close()
}

// ---------------------------------------------
// Test Helper
// ---------------------------------------------

type BadgerDBTesting struct {
	Dir string
}

func (b *BadgerDBTesting) Setup(conf BadgerConfig) Storage {
	if !dirExists(b.Dir) {
		if err := os.Mkdir(b.Dir, 0777); err != nil {
			panic(err)
		}
	}
	b.Dir = filepath.Join(b.Dir, random.String("test-data-", 10))
	if err := os.Mkdir(b.Dir, 0777); err != nil {
		panic(err)
	}
	conf.StorageDir = b.Dir
	return NewBadgerStorage(conf)
}

func (b *BadgerDBTesting) Teardown() {
	if err := os.RemoveAll(b.Dir); err != nil {
		panic(err)
	}
}
