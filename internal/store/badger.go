package store

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"github.com/duh-rpc/duh-go"
	"github.com/kapetan-io/errors"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/tackle/clock"
	"github.com/segmentio/ksuid"
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
// PartitionStore Implementation
// ---------------------------------------------

type BadgerPartitionStore struct {
	conf BadgerConfig
}

var _ PartitionStore = &BadgerPartitionStore{}

func NewBadgerPartitionStore(conf BadgerConfig) *BadgerPartitionStore {
	return &BadgerPartitionStore{conf: conf}
}

func (b *BadgerPartitionStore) Create(info types.PartitionInfo) error {
	f := errors.Fields{"category", "badger", "func", "BadgerPartitionStore.Create"}

	dir := filepath.Join(b.conf.StorageDir, fmt.Sprintf("%s-%06d-%s", info.QueueName, info.Partition, bucketName))

	db, err := badger.Open(badger.DefaultOptions(dir))
	if err != nil {
		return f.Errorf("while opening db '%s': %w", dir, err)
	}

	return db.Close()
}

func (b *BadgerPartitionStore) Get(info types.PartitionInfo) Partition {
	return &BadgerPartition{
		uid:  ksuid.New(),
		conf: b.conf,
		info: info,
	}
}

// ---------------------------------------------
// Partition Implementation
// ---------------------------------------------

type BadgerPartition struct {
	info types.PartitionInfo
	conf BadgerConfig
	uid  ksuid.KSUID
	db   *badger.DB
}

func (b *BadgerPartition) Produce(_ context.Context, batch types.Batch[types.ProduceRequest]) error {
	f := errors.Fields{"category", "badger", "func", "Queue.Produce"}
	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(txn *badger.Txn) error {
		for _, r := range batch.Requests {
			for _, item := range r.Items {
				b.uid = b.uid.Next()
				item.ID = []byte(b.uid.String())
				item.CreatedAt = b.conf.Clock.Now().UTC()

				// TODO: Get buffers from memory pool
				var buf bytes.Buffer
				if err := gob.NewEncoder(&buf).Encode(item); err != nil {
					return f.Errorf("during gob.Encode(): %w", err)
				}

				if err := txn.Set(item.ID, buf.Bytes()); err != nil {
					return f.Errorf("during Put(): %w", err)
				}
			}
		}
		return nil
	})
}

func (b *BadgerPartition) Reserve(_ context.Context, batch types.ReserveBatch, opts ReserveOptions) error {
	f := errors.Fields{"category", "badger", "func", "Queue.Reserve"}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(txn *badger.Txn) error {

		batchIter := batch.Iterator()
		var count int
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
				continue
			}
			break
		}
		return nil
	})
}

func (b *BadgerPartition) Complete(_ context.Context, batch types.Batch[types.CompleteRequest]) error {
	f := errors.Fields{"category", "badger", "func", "Queue.Complete"}
	var done bool

	db, err := b.getDB()
	if err != nil {
		return err
	}

	txn := db.NewTransaction(true)

	defer func() {
		if !done {
			// TODO: The /queue.complete request should not roll back item IDs that have been successfully marked as
			//  completed if a subset of item IDs fail.
			//  https://github.com/kapetan-io/querator/blob/main/doc/adr/0018-queue-complete-error-semantics.md
			txn.Discard()
		}
	}()

nextBatch:
	// TODO: if any of the provided item IDs cannot be completed, the entire /queue.complete call should return an error
	//  https://github.com/kapetan-io/querator/blob/main/doc/adr/0018-queue-complete-error-semantics.md
	for i := range batch.Requests {
		for _, id := range batch.Requests[i].Ids {
			if err = b.validateID(id); err != nil {
				batch.Requests[i].Err = transport.NewInvalidOption("invalid storage id; '%s': %s", id, err)
				continue nextBatch
			}

			// TODO: Test complete with id's that do not exist in the database
			kvItem, err := txn.Get(id)
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
					"marked as reserved", id)
				continue nextBatch
			}

			if err = txn.Delete(id); err != nil {
				return f.Errorf("during Delete(%s): %w", id, err)
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

func (b *BadgerPartition) List(_ context.Context, items *[]*types.Item, opts types.ListOptions) error {
	f := errors.Fields{"category", "badger", "func", "Queue.List"}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	if opts.Pivot != nil {
		if err := b.validateID(opts.Pivot); err != nil {
			return transport.NewInvalidOption("invalid storage id; '%s': %s", opts.Pivot, err)
		}
	}

	return db.View(func(txn *badger.Txn) error {

		var count int
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
			var v []byte
			v, err := iter.Item().ValueCopy(v)
			if err != nil {
				return f.Errorf("during Get value: %w", err)
			}

			item := new(types.Item) // TODO: memory pool
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(item); err != nil {
				return f.Errorf("during Decode(): %w", err)
			}

			*items = append(*items, item)
			count++

			if count >= opts.Limit {
				return nil
			}
		}
		return nil
	})
}

func (b *BadgerPartition) Add(_ context.Context, items []*types.Item) error {
	f := errors.Fields{"category", "badger", "func", "Queue.Add"}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(txn *badger.Txn) error {

		for _, item := range items {
			b.uid = b.uid.Next()
			item.ID = []byte(b.uid.String())
			item.CreatedAt = b.conf.Clock.Now().UTC()

			// TODO: Get buffers from memory pool
			var buf bytes.Buffer
			if err := gob.NewEncoder(&buf).Encode(item); err != nil {
				return f.Errorf("during gob.Encode(): %w", err)
			}

			if err := txn.Set(item.ID, buf.Bytes()); err != nil {
				return f.Errorf("during Put(): %w", err)
			}
		}
		return nil
	})
}

func (b *BadgerPartition) Delete(_ context.Context, ids []types.ItemID) error {
	f := errors.Fields{"category", "badger", "func", "Queue.Delete"}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(txn *badger.Txn) error {

		for _, id := range ids {
			if err := b.validateID(id); err != nil {
				return transport.NewInvalidOption("invalid storage id; '%s': %s", id, err)
			}
			if err := txn.Delete(id); err != nil {
				return f.Errorf("during delete: %w", err)
			}
		}
		return nil
	})
}

func (b *BadgerPartition) Clear(_ context.Context, destructive bool) error {
	f := errors.Fields{"category", "badger", "func", "Queue.Delete"}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(txn *badger.Txn) error {
		if destructive {
			err := db.DropAll()
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

func (b *BadgerPartition) Stats(_ context.Context, stats *types.QueueStats) error {
	f := errors.Fields{"category", "badger", "func", "Partition.Stats"}
	now := b.conf.Clock.Now().UTC()

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.View(func(txn *badger.Txn) error {

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

func (b *BadgerPartition) Close(_ context.Context) error {
	if b.db != nil {
		return b.db.Close()
	}
	return nil
}

func (b *BadgerPartition) validateID(id []byte) error {
	_, err := ksuid.Parse(string(id))
	if err != nil {
		return err
	}
	return nil
}

func (b *BadgerPartition) getDB() (*badger.DB, error) {
	if b.db != nil {
		return b.db, nil
	}

	f := errors.Fields{"category", "badger", "func", "BadgerPartition.getDB"}
	dir := filepath.Join(b.conf.StorageDir, fmt.Sprintf("%s-%06d-%s", b.info.QueueName, b.info.Partition, bucketName))

	db, err := badger.Open(badger.DefaultOptions(dir))
	if err != nil {
		return nil, f.Errorf("while opening db '%s': %w", dir, err)
	}

	b.db = db
	return db, nil
}

// ---------------------------------------------
// QueueStore Implementation
// ---------------------------------------------

func NewBadgerQueueStore(conf BadgerConfig) QueueStore {
	return &BadgerQueueStore{
		conf: conf,
	}
}

type BadgerQueueStore struct {
	QueuesValidation
	db   *badger.DB
	conf BadgerConfig
}

var _ QueueStore = &BadgerQueueStore{}

func (b *BadgerQueueStore) getDB() (*badger.DB, error) {
	if b.db != nil {
		return b.db, nil
	}

	f := errors.Fields{"category", "badger", "func", "StorageConfig.QueueStore"}
	// We store info about the queues in a single db file. We prefix it with `~` to make it
	// impossible for someone to create a queue with the same name.
	dir := filepath.Join(b.conf.StorageDir, fmt.Sprintf("~queue-storage-%s", bucketName))
	db, err := badger.Open(badger.DefaultOptions(dir))
	if err != nil {
		return nil, f.Errorf("while opening db '%s': %w", dir, err)
	}

	b.db = db
	return db, nil
}

func (b *BadgerQueueStore) Get(_ context.Context, name string, queue *types.QueueInfo) error {
	f := errors.Fields{"category", "badger", "func", "QueueStore.Get"}

	if err := b.validateGet(name); err != nil {
		return err
	}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.View(func(txn *badger.Txn) error {

		kvItem, err := txn.Get([]byte(name))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return ErrQueueNotExist
			}
			return f.Errorf("during Get(): %w", err)
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

func (b *BadgerQueueStore) Add(_ context.Context, info types.QueueInfo) error {
	f := errors.Fields{"category", "badger", "func", "QueueStore.Add"}

	if err := b.validateAdd(info); err != nil {
		return err
	}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(txn *badger.Txn) error {

		// If the queue already exists in the store
		_, err := txn.Get([]byte(info.Name))
		if err == nil {
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

func (b *BadgerQueueStore) Update(_ context.Context, info types.QueueInfo) error {
	f := errors.Fields{"category", "badger", "func", "QueueStore.Update"}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	if err := b.validateQueueName(info); err != nil {
		return err
	}

	return db.Update(func(txn *badger.Txn) error {

		kvItem, err := txn.Get([]byte(info.Name))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
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

		if err := b.validateUpdate(found); err != nil {
			return err
		}

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

func (b *BadgerQueueStore) List(_ context.Context, queues *[]types.QueueInfo, opts types.ListOptions) error {
	f := errors.Fields{"category", "badger", "func", "QueueStore.List"}

	if err := b.validateList(opts); err != nil {
		return err
	}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.View(func(txn *badger.Txn) error {
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

func (b *BadgerQueueStore) Delete(_ context.Context, name string) error {
	f := errors.Fields{"category", "badger", "func", "QueueStore.Delete"}

	if err := b.validateDelete(name); err != nil {
		return err
	}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(txn *badger.Txn) error {

		if err := txn.Delete([]byte(name)); err != nil {
			return f.Errorf("during Delete(%s): %w", name, err)
		}
		return nil
	})
}

func (b *BadgerQueueStore) Close(_ context.Context) error {
	err := b.db.Close()
	b.db = nil
	return err
}
