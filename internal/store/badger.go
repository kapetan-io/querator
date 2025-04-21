package store

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"github.com/kapetan-io/errors"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/set"
	"github.com/segmentio/ksuid"
	"iter"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
)

type BadgerConfig struct {
	// StorageDir is the directory where badger will store its data
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

type BadgerPartitionStore struct {
	conf BadgerConfig
}

var _ PartitionStore = &BadgerPartitionStore{}

func NewBadgerPartitionStore(conf BadgerConfig) *BadgerPartitionStore {
	return &BadgerPartitionStore{conf: conf}
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
	mu   sync.RWMutex
	uid  ksuid.KSUID
	db   *badger.DB
}

func (b *BadgerPartition) Produce(_ context.Context, batch types.ProduceBatch) error {
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

				// TODO: GetByPartition buffers from memory pool
				var buf bytes.Buffer
				if err := gob.NewEncoder(&buf).Encode(item); err != nil {
					return errors.Errorf("during gob.Encode(): %w", err)
				}

				if err := txn.Set(item.ID, buf.Bytes()); err != nil {
					return errors.Errorf("during Set(): %w", err)
				}
			}
		}
		return nil
	})
}

func (b *BadgerPartition) Lease(_ context.Context, batch types.LeaseBatch, opts LeaseOptions) error {
	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(txn *badger.Txn) error {

		batchIter := batch.Iterator()
		var count int
		iter := txn.NewIterator(badger.IteratorOptions{
			PrefetchValues: false,
			PrefetchSize:   100,
			Reverse:        false,
			AllVersions:    false,
		})

		defer iter.Close()

		for iter.Rewind(); iter.Valid(); iter.Next() {
			if count >= batch.TotalRequested {
				break
			}

			var v []byte
			v, err := iter.Item().ValueCopy(v)
			if err != nil {
				return errors.Errorf("while getting value: %w", err)
			}

			item := new(types.Item) // TODO: memory pool
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(item); err != nil {
				return errors.Errorf("during Decode(): %w", err)
			}

			if item.IsLeased {
				continue
			}

			item.LeaseDeadline = opts.LeaseDeadline
			item.IsLeased = true
			item.Attempts++
			count++

			// Assign the item to the next waiting lease in the batch,
			// returns false if there are no more leases available to fill
			if batchIter.Next(item) {
				// If assignment was a success, then we put the updated item into the db
				var buf bytes.Buffer // TODO: memory pool
				if err := gob.NewEncoder(&buf).Encode(item); err != nil {
					return errors.Errorf("during gob.Encode(): %w", err)
				}

				if err := txn.Set(item.ID, buf.Bytes()); err != nil {
					return errors.Errorf("during Put(): %w", err)
				}
				continue
			}
			break
		}
		return nil
	})
}

func (b *BadgerPartition) Complete(_ context.Context, batch types.Batch[types.CompleteRequest]) error {
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
				return errors.Errorf("during Decode(): %w", err)
			}

			if !item.IsLeased {
				batch.Requests[i].Err = transport.NewConflict("item(s) cannot be completed; '%s' is not "+
					"marked as leased", id)
				continue nextBatch
			}

			if err = txn.Delete(id); err != nil {
				return errors.Errorf("during Delete(%s): %w", id, err)
			}
		}
	}

	err = txn.Commit()
	if err != nil {
		return errors.Errorf("during Commit(): %w", err)
	}

	done = true
	return nil
}

func (b *BadgerPartition) List(_ context.Context, items *[]*types.Item, opts types.ListOptions) error {
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
		} else {
			iter.Rewind()
		}

		for ; iter.Valid(); iter.Next() {
			var v []byte
			v, err := iter.Item().ValueCopy(v)
			if err != nil {
				return errors.Errorf("during GetByPartition value: %w", err)
			}

			item := new(types.Item) // TODO: memory pool
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(item); err != nil {
				return errors.Errorf("during Decode(): %w", err)
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
	db, err := b.getDB()
	if err != nil {
		return err
	}

	if len(items) == 0 {
		return transport.NewInvalidOption("items is invalid; cannot be empty")
	}

	return db.Update(func(txn *badger.Txn) error {

		for _, item := range items {
			b.uid = b.uid.Next()
			item.ID = []byte(b.uid.String())
			item.CreatedAt = b.conf.Clock.Now().UTC()

			// TODO: GetByPartition buffers from memory pool
			var buf bytes.Buffer
			if err := gob.NewEncoder(&buf).Encode(item); err != nil {
				return errors.Errorf("during gob.Encode(): %w", err)
			}

			if err := txn.Set(item.ID, buf.Bytes()); err != nil {
				return errors.Errorf("during Put(): %w", err)
			}
		}
		return nil
	})
}

func (b *BadgerPartition) Delete(_ context.Context, ids []types.ItemID) error {
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
				return errors.Errorf("during delete: %w", err)
			}
		}
		return nil
	})
}

func (b *BadgerPartition) Clear(_ context.Context, destructive bool) error {
	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(txn *badger.Txn) error {
		if destructive {
			err := db.DropAll()
			if err != nil {
				return errors.Errorf("during destructive DropAll(): %w", err)
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
				return errors.Errorf("during GetByPartition value: %w", err)
			}

			item := new(types.Item) // TODO: memory pool
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(item); err != nil {
				return errors.Errorf("during Decode(): %w", err)
			}

			// Skip leased items
			if item.IsLeased {
				continue
			}

			if err := txn.Delete(k); err != nil {
				return errors.Errorf("during Delete(): %w", err)
			}
		}
		return nil
	})
}

func (b *BadgerPartition) Info() types.PartitionInfo {
	defer b.mu.RUnlock()
	b.mu.RLock()
	return b.info
}

func (b *BadgerPartition) UpdateQueueInfo(info types.QueueInfo) {
	defer b.mu.Unlock()
	b.mu.Lock()
	b.info.Queue = info
}

func (b *BadgerPartition) ScanForActions(_ clock.Duration, now clock.Time) iter.Seq[types.Action] {
	return func(yield func(types.Action) bool) {
		db, err := b.getDB()
		if err != nil {
			return
		}

		err = db.View(func(txn *badger.Txn) error {
			iter := txn.NewIterator(badger.DefaultIteratorOptions)
			defer iter.Close()

			for iter.Rewind(); iter.Valid(); iter.Next() {
				var v []byte
				v, err := iter.Item().ValueCopy(v)
				if err != nil {
					return err
				}

				item := new(types.Item)
				if err := gob.NewDecoder(bytes.NewReader(v)).Decode(item); err != nil {
					return errors.Errorf("during Decode(): %w", err)
				}

				// Is the leased item expired?
				if item.IsLeased {
					if now.After(item.LeaseDeadline) {
						if !yield(types.Action{
							Action:       types.ActionLeaseExpired,
							PartitionNum: b.info.PartitionNum,
							Queue:        b.info.Queue.Name,
							Item:         *item,
						}) {
							return nil
						}
						continue
					}
					if b.info.Queue.MaxAttempts != 0 && item.Attempts >= b.info.Queue.MaxAttempts {
						if !yield(types.Action{
							Action:       types.ActionItemMaxAttempts,
							PartitionNum: b.info.PartitionNum,
							Queue:        b.info.Queue.Name,
							Item:         *item,
						}) {
							return nil
						}
						continue
					}
				}
				// Is the item expired?
				if now.After(item.ExpireDeadline) {
					if !yield(types.Action{
						Action:       types.ActionItemExpired,
						PartitionNum: b.info.PartitionNum,
						Queue:        b.info.Queue.Name,
						Item:         *item,
					}) {
						return nil
					}
					continue
				}
			}
			return nil
		})

		if err != nil {
			b.conf.Log.Error("Error scanning for actions", "error", err)
		}
	}
}

func (b *BadgerPartition) TakeAction(_ context.Context, batch types.Batch[types.LifeCycleRequest]) error {
	if len(batch.Requests) == 0 {
		return nil
	}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(txn *badger.Txn) error {
		for _, req := range batch.Requests {
			for _, a := range req.Actions {
				switch a.Action {
				case types.ActionLeaseExpired:
					item := new(types.Item)
					kvItem, err := txn.Get(a.Item.ID)
					if err != nil {
						if errors.Is(err, badger.ErrKeyNotFound) {
							b.conf.Log.Warn("unable to find item while processing action; ignoring action",
								"id", a.Item.ID, "action", types.ActionToString(a.Action))
							continue
						}
						return err
					}

					err = kvItem.Value(func(val []byte) error {
						return gob.NewDecoder(bytes.NewReader(val)).Decode(item)
					})
					if err != nil {
						return err
					}

					// Update the item
					item.LeaseDeadline = clock.Time{}
					item.IsLeased = false

					// Assign a new ID to the item, as it is placed at the start of the queue
					b.uid = b.uid.Next()
					item.ID = []byte(b.uid.String())

					// Encode and store the updated item
					var buf bytes.Buffer
					if err := gob.NewEncoder(&buf).Encode(item); err != nil {
						return err
					}

					if err := txn.Set(item.ID, buf.Bytes()); err != nil {
						return err
					}

					// Delete the old item
					if err := txn.Delete(a.Item.ID); err != nil {
						return err
					}

				case types.ActionDeleteItem:
					// TODO(lifecycle): Find the item and remove it
					if err := txn.Delete(a.Item.ID); err != nil {
						return err
					}

				case types.ActionQueueScheduledItem:
					// TODO: Find the scheduled item and add it to the partition queue

				default:
					b.conf.Log.Warn("undefined action", "action", fmt.Sprintf("%d", int(a.Action)))
				}
			}
		}
		return nil
	})
}

func (b *BadgerPartition) LifeCycleInfo(_ context.Context, info *types.LifeCycleInfo) error {
	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.View(func(txn *badger.Txn) error {
		next := theFuture

		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		for iter.Rewind(); iter.Valid(); iter.Next() {
			var v []byte
			v, err := iter.Item().ValueCopy(v)
			if err != nil {
				return err
			}

			item := new(types.Item)
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(item); err != nil {
				return err
			}

			if item.LeaseDeadline.Before(next) {
				next = item.LeaseDeadline
			}
		}

		if next != theFuture {
			info.NextLeaseExpiry = next
		}
		return nil
	})
}

func (b *BadgerPartition) Stats(_ context.Context, stats *types.PartitionStats) error {
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
				return errors.Errorf("during GetByPartition value: %w", err)
			}

			item := new(types.Item) // TODO: memory pool
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(item); err != nil {
				return errors.Errorf("during Decode(): %w", err)
			}

			stats.Total++
			stats.AverageAge += now.Sub(item.CreatedAt)
			if item.IsLeased {
				stats.AverageLeasedAge += item.LeaseDeadline.Sub(now)
				stats.NumLeased++
			}
		}

		if stats.Total != 0 {
			stats.AverageAge = clock.Duration(int64(stats.AverageAge) / int64(stats.Total))
		}
		if stats.NumLeased != 0 {
			stats.AverageLeasedAge = clock.Duration(int64(stats.AverageLeasedAge) / int64(stats.NumLeased))
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
	b.mu.Lock()
	if b.db != nil {
		b.mu.Unlock()
		return b.db, nil
	}

	dir := filepath.Join(b.conf.StorageDir, fmt.Sprintf("%s-%06d-%s", b.info.Queue.Name, b.info.PartitionNum, bucketName))

	opts := badger.DefaultOptions(dir)
	opts.Logger = newBadgerLogger(b.conf.Log)
	db, err := badger.Open(opts)
	if err != nil {
		b.mu.Unlock()
		return nil, errors.Errorf("while opening db '%s': %w", dir, err)
	}
	b.db = db
	b.mu.Unlock()
	return db, nil
}

// ---------------------------------------------
// Queues Implementation
// ---------------------------------------------

func NewBadgerQueues(conf BadgerConfig) Queues {
	set.Default(conf.Log, slog.Default())
	return &BadgerQueues{
		conf: conf,
	}
}

type BadgerQueues struct {
	QueuesValidation
	db   *badger.DB
	conf BadgerConfig
}

var _ Queues = &BadgerQueues{}

func (b *BadgerQueues) getDB() (*badger.DB, error) {
	if b.db != nil {
		return b.db, nil
	}

	// We store info about the queues in a single db file. We prefix it with `~` to make it
	// impossible for someone to create a queue with the same name.
	dir := filepath.Join(b.conf.StorageDir, fmt.Sprintf("~queue-storage-%s", bucketName))

	opts := badger.DefaultOptions(dir)
	opts.Logger = newBadgerLogger(b.conf.Log)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, errors.Errorf("while opening db '%s': %w", dir, err)
	}

	b.db = db
	return db, nil
}

func (b *BadgerQueues) Get(_ context.Context, name string, queue *types.QueueInfo) error {
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
			return errors.Errorf("during GetByPartition(): %w", err)
		}

		var v []byte
		v, err = kvItem.ValueCopy(v)
		if err != nil {
			return errors.Errorf("during GetByPartition value(): %w", err)
		}

		if err := gob.NewDecoder(bytes.NewReader(v)).Decode(queue); err != nil {
			return errors.Errorf("during Decode(): %w", err)
		}
		return nil
	})
}

func (b *BadgerQueues) Add(_ context.Context, info types.QueueInfo) error {
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
			return errors.Errorf("during gob.Encode(): %w", err)
		}

		if err := txn.Set([]byte(info.Name), buf.Bytes()); err != nil {
			return errors.Errorf("during Put(): %w", err)
		}
		return nil
	})
}

func (b *BadgerQueues) Update(_ context.Context, info types.QueueInfo) error {
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
			return errors.Errorf("during GetByPartition(): %w", err)
		}

		var v []byte
		v, err = kvItem.ValueCopy(v)
		if err != nil {
			return errors.Errorf("during GetByPartition value(): %w", err)
		}

		var found types.QueueInfo
		if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&found); err != nil {
			return errors.Errorf("during Decode(): %w", err)
		}

		found.Update(info)

		if err := b.validateUpdate(found); err != nil {
			return err
		}

		if found.LeaseTimeout > found.ExpireTimeout {
			return transport.NewInvalidOption("lease timeout is too long; %s cannot be greater than the "+
				"expire timeout %s", info.LeaseTimeout.String(), found.ExpireTimeout.String())
		}

		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(found); err != nil {
			return errors.Errorf("during gob.Encode(): %w", err)
		}

		if err := txn.Set([]byte(info.Name), buf.Bytes()); err != nil {
			return errors.Errorf("during Put(): %w", err)
		}
		return nil
	})
}

func (b *BadgerQueues) List(_ context.Context, queues *[]types.QueueInfo, opts types.ListOptions) error {
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
		} else {
			iter.Rewind()
		}

		for ; iter.Valid(); iter.Next() {
			v, err := iter.Item().ValueCopy(v)
			if err != nil {
				return errors.Errorf("during GetByPartition value(): %w", err)
			}

			var info types.QueueInfo
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&info); err != nil {
				return errors.Errorf("during Decode(): %w", err)
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

func (b *BadgerQueues) Delete(_ context.Context, name string) error {
	if err := b.validateDelete(name); err != nil {
		return err
	}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(txn *badger.Txn) error {

		if err := txn.Delete([]byte(name)); err != nil {
			return errors.Errorf("during Delete(%s): %w", name, err)
		}
		return nil
	})
}

func (b *BadgerQueues) Close(_ context.Context) error {
	err := b.db.Close()
	b.db = nil
	return err
}

type badgerLogger struct {
	log *slog.Logger
}

func newBadgerLogger(log *slog.Logger) *badgerLogger {
	return &badgerLogger{log: log.With("code.namespace", "badger-lib")}
}

func (l *badgerLogger) Errorf(f string, v ...interface{}) {
	l.log.Error(fmt.Sprintf(strings.Trim(f, "\n"), v...))
}

func (l *badgerLogger) Warningf(f string, v ...interface{}) {
	l.log.Warn(fmt.Sprintf(strings.Trim(f, "\n"), v...))
}

func (l *badgerLogger) Infof(f string, v ...interface{}) {
	l.log.LogAttrs(context.Background(), LevelDebug, fmt.Sprintf(strings.Trim(f, "\n"), v...))
}

func (l *badgerLogger) Debugf(f string, v ...interface{}) {
	l.log.LogAttrs(context.Background(), LevelDebug, fmt.Sprintf(strings.Trim(f, "\n"), v...))
}
