package store

import (
	"bytes"
	"context"
	"encoding/gob"
	"github.com/dgraph-io/badger/v4"
	"github.com/duh-rpc/duh-go"
	"github.com/kapetan-io/errors"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/tackle/clock"
	"github.com/segmentio/ksuid"
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

type BadgerPartitionStore struct {
	conf BadgerConfig
}

var _ PartitionStore = &BadgerPartitionStore{}

func NewBadgerPartitionStore(conf BadgerConfig) *BadgerPartitionStore {
	return &BadgerPartitionStore{conf: conf}
}

func (b *BadgerPartitionStore) Create(info types.PartitionInfo) error {
	//TODO implement me
	panic("implement me")
}

func (b *BadgerPartitionStore) Get(info types.PartitionInfo) Partition {
	//TODO implement me
	panic("implement me")
}

// ---------------------------------------------
// Partition Implementation
// ---------------------------------------------

type BadgerPartition struct {
	info types.QueueInfo
	conf BadgerConfig
	uid  ksuid.KSUID
	db   *badger.DB
}

func (b *BadgerPartition) Produce(_ context.Context, batch types.Batch[types.ProduceRequest]) error {
	f := errors.Fields{"category", "badger", "func", "Queue.Produce"}

	return b.db.Update(func(txn *badger.Txn) error {

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
	return b.db.Update(func(txn *badger.Txn) error {

		batchIter := batch.Iterator()
		var count int

		err := b.db.Update(func(txn *badger.Txn) error {
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
		return err
	})
}

func (b *BadgerPartition) Complete(_ context.Context, batch types.Batch[types.CompleteRequest]) error {
	f := errors.Fields{"category", "badger", "func", "Queue.Complete"}
	var done bool
	var err error

	txn := b.db.NewTransaction(true)

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

	if opts.Pivot != nil {
		if err := b.validateID(opts.Pivot); err != nil {
			return transport.NewInvalidOption("invalid storage id; '%s': %s", opts.Pivot, err)
		}
	}

	return b.db.View(func(txn *badger.Txn) error {

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

	return b.db.Update(func(txn *badger.Txn) error {

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

	return b.db.Update(func(txn *badger.Txn) error {

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

	return b.db.Update(func(txn *badger.Txn) error {
		if destructive {
			err := b.db.DropAll()
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
	f := errors.Fields{"category", "bunt-db", "func", "Queue.Stats"}
	now := b.conf.Clock.Now().UTC()

	return b.db.View(func(txn *badger.Txn) error {

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
	return b.db.Close()
}

func (b *BadgerPartition) validateID(id []byte) error {
	_, err := ksuid.Parse(string(id))
	if err != nil {
		return err
	}
	return nil
}

func (b *BadgerPartition) getDB() (*badger.DB, error) {
	// TODO: implement
	return nil, nil
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

func (s *BadgerQueueStore) getDB() (*badger.DB, error) {
	// TODO: implement
	return nil, nil
}

func (s *BadgerQueueStore) Get(_ context.Context, name string, queue *types.QueueInfo) error {
	f := errors.Fields{"category", "badger", "func", "QueuesStore.Get"}

	if err := s.validateGet(name); err != nil {
		return err
	}

	return s.db.View(func(txn *badger.Txn) error {

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

func (s *BadgerQueueStore) Add(_ context.Context, info types.QueueInfo) error {
	f := errors.Fields{"category", "badger", "func", "QueuesStore.Add"}

	if err := s.validateAdd(info); err != nil {
		return err
	}

	return s.db.Update(func(txn *badger.Txn) error {

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

func (s *BadgerQueueStore) Update(_ context.Context, info types.QueueInfo) error {
	f := errors.Fields{"category", "badger", "func", "QueuesStore.Update"}

	if err := s.validateUpdate(info); err != nil {
		return err
	}

	return s.db.Update(func(txn *badger.Txn) error {

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

func (s *BadgerQueueStore) List(_ context.Context, queues *[]types.QueueInfo, opts types.ListOptions) error {
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

func (s *BadgerQueueStore) Delete(_ context.Context, name string) error {
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

func (s *BadgerQueueStore) Close(_ context.Context) error {
	return s.db.Close()
}
