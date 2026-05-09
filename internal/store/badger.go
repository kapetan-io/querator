package store

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"iter"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/kapetan-io/errors"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport/reply"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/set"
	"github.com/segmentio/ksuid"
)

type BadgerConfig struct {
	// StorageDir is the directory where badger will store its data
	StorageDir string
	// Log is used to log warnings and errors
	Log *slog.Logger
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

func (b *BadgerPartitionStore) Config() BadgerConfig {
	return b.conf
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

func (b *BadgerPartition) Produce(_ context.Context, batch types.ProduceBatch, now clock.Time) error {
	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(txn *badger.Txn) error {
		for _, r := range batch.Requests {
			for _, item := range r.Items {
				// Check for duplicate SourceID
				if item.SourceID != nil {
					sourceKey := []byte("source:" + string(item.SourceID))
					_, err := txn.Get(sourceKey)
					if err == nil {
						// Skip item - duplicate SourceID (idempotent)
						continue
					}
					if !errors.Is(err, badger.ErrKeyNotFound) {
						return errors.Errorf("checking SourceID: %w", err)
					}
				}

				b.uid = b.uid.Next()
				item.ID = []byte(b.uid.String())
				item.CreatedAt = now

				// If the EnqueueAt is less than 100 milliseconds from now, we want to enqueue
				// the item immediately. This avoids unnecessary work for something that will be moved into the
				// queue less than a few milliseconds from now. Programmers may want to be "complete", so they may
				// include an EnqueueAt time when producing items, which is set to now or near to now, due to clock
				// drift between systems. Because of this, Querator may end up doing more work and be less efficient
				// simply because of programmers who desire to completely fill out the pb.QueueProduceItem{} struct
				// with every possible field, regardless of whether the field is needed.

				// If EnqueueAt dates are in the past, enqueue the item instead of adding it as a scheduled item.
				if item.EnqueueAt.Before(now.Add(time.Millisecond * 100)) {
					item.EnqueueAt = clock.Time{}
				}

				// TODO: GetByPartition buffers from memory pool
				var buf bytes.Buffer
				if err := gob.NewEncoder(&buf).Encode(item); err != nil {
					return errors.Errorf("during gob.Encode(): %w", err)
				}

				if err := txn.Set(item.ID, buf.Bytes()); err != nil {
					return errors.Errorf("during Set(): %w", err)
				}

				// Add SourceID index if set
				if item.SourceID != nil {
					sourceKey := []byte("source:" + string(item.SourceID))
					if err := txn.Set(sourceKey, item.ID); err != nil {
						return errors.Errorf("setting SourceID index: %w", err)
					}
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

			key := iter.Item().Key()

			// Skip secondary index keys (source:xxx)
			if bytes.HasPrefix(key, []byte("source:")) {
				continue
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

			// Skip scheduled items
			if !item.EnqueueAt.IsZero() {
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

func (b *BadgerPartition) Complete(_ context.Context, batch types.CompleteBatch) error {
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
				batch.Requests[i].Err = reply.NewInvalidOption("invalid storage id; '%s': %s", id, err)
				continue nextBatch
			}

			// TODO: Test complete with id's that do not exist in the database
			kvItem, err := txn.Get(id)
			if err != nil {
				batch.Requests[i].Err = reply.NewInvalidOption("invalid storage id; '%s' does not exist", id)
				continue nextBatch
			}
			var v []byte
			v, err = kvItem.ValueCopy(v)
			if err != nil {
				batch.Requests[i].Err = reply.NewInvalidOption("invalid storage id; '%s' does not exist", id)
				continue nextBatch
			}

			item := new(types.Item) // TODO: memory pool
			if err = gob.NewDecoder(bytes.NewReader(v)).Decode(item); err != nil {
				return errors.Errorf("during Decode(): %w", err)
			}

			if !item.IsLeased {
				batch.Requests[i].Err = reply.NewConflict("item(s) cannot be completed; '%s' is not "+
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

func (b *BadgerPartition) Retry(_ context.Context, batch types.RetryBatch) error {
	var done bool

	db, err := b.getDB()
	if err != nil {
		return err
	}

	txn := db.NewTransaction(true)

	defer func() {
		if !done {
			txn.Discard()
		}
	}()

nextBatch:
	for i := range batch.Requests {
		for _, retryItem := range batch.Requests[i].Items {
			if err = b.validateID(retryItem.ID); err != nil {
				batch.Requests[i].Err = reply.NewInvalidOption("invalid storage id; '%s': %s", retryItem.ID, err)
				continue nextBatch
			}

			kvItem, err := txn.Get(retryItem.ID)
			if err != nil {
				batch.Requests[i].Err = reply.NewInvalidOption("invalid storage id; '%s' does not exist", retryItem.ID)
				continue nextBatch
			}
			var v []byte
			v, err = kvItem.ValueCopy(v)
			if err != nil {
				batch.Requests[i].Err = reply.NewInvalidOption("invalid storage id; '%s' does not exist", retryItem.ID)
				continue nextBatch
			}

			item := new(types.Item) // TODO: memory pool
			if err = gob.NewDecoder(bytes.NewReader(v)).Decode(item); err != nil {
				return errors.Errorf("during Decode(): %w", err)
			}

			if !item.IsLeased {
				batch.Requests[i].Err = reply.NewConflict("item(s) cannot be retried; '%s' is not "+
					"marked as leased", retryItem.ID)
				continue nextBatch
			}

			// Clear lease status (attempts incremented on next lease)
			
			item.IsLeased = false
			item.LeaseDeadline = clock.Time{}

			if retryItem.Dead {
				// TODO: Move to dead letter queue when implemented
				// For now, just delete the item
				if err = txn.Delete(retryItem.ID); err != nil {
					return errors.Errorf("during Delete(%s): %w", retryItem.ID, err)
				}
			} else {
				// For scheduled retry or immediate retry, update the item
				if !retryItem.RetryAt.IsZero() {
					// If RetryAt is in the past or less than 100ms from now, treat as immediate retry
					now := clock.Now().UTC()
					if retryItem.RetryAt.Before(now.Add(time.Millisecond * 100)) {
						// Immediate retry - EnqueueAt stays zero
						item.EnqueueAt = clock.Time{}
					} else {
						// Schedule for future retry
						item.EnqueueAt = retryItem.RetryAt
					}
				}
				// For immediate retry (empty RetryAt), item stays in queue with incremented attempts

				// Update the item in storage
				var buf bytes.Buffer
				if err := gob.NewEncoder(&buf).Encode(item); err != nil {
					return errors.Errorf("during gob.Encode(): %w", err)
				}
				if err := txn.Set(retryItem.ID, buf.Bytes()); err != nil {
					return errors.Errorf("during Set(%s): %w", retryItem.ID, err)
				}
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
			return reply.NewInvalidOption("invalid storage id; '%s': %s", opts.Pivot, err)
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
			key := iter.Item().Key()

			// Skip secondary index keys (source:xxx)
			if bytes.HasPrefix(key, []byte("source:")) {
				continue
			}

			var v []byte
			v, err := iter.Item().ValueCopy(v)
			if err != nil {
				return errors.Errorf("during GetByPartition value: %w", err)
			}

			item := new(types.Item) // TODO: memory pool
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(item); err != nil {
				return errors.Errorf("during Decode(): %w", err)
			}

			// Skip scheduled items in the regular list
			if !item.EnqueueAt.IsZero() {
				continue
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

func (b *BadgerPartition) ListScheduled(_ context.Context, items *[]*types.Item, opts types.ListOptions) error {
	db, err := b.getDB()
	if err != nil {
		return err
	}

	if opts.Pivot != nil {
		if err := b.validateID(opts.Pivot); err != nil {
			return reply.NewInvalidOption("invalid storage id; '%s': %s", opts.Pivot, err)
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
			key := iter.Item().Key()

			// Skip secondary index keys (source:xxx)
			if bytes.HasPrefix(key, []byte("source:")) {
				continue
			}

			var v []byte
			v, err := iter.Item().ValueCopy(v)
			if err != nil {
				return err
			}

			item := new(types.Item)
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(item); err != nil {
				return errors.Errorf("during Decode(): %w", err)
			}

			// Only return scheduled items (those with non-zero EnqueueAt)
			if item.EnqueueAt.IsZero() {
				continue
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

func (b *BadgerPartition) Add(_ context.Context, items []*types.Item, now clock.Time) error {
	db, err := b.getDB()
	if err != nil {
		return err
	}

	if len(items) == 0 {
		return reply.NewInvalidOption("items is invalid; cannot be empty")
	}

	return db.Update(func(txn *badger.Txn) error {

		for _, item := range items {
			// Check for duplicate SourceID
			if item.SourceID != nil {
				sourceKey := []byte("source:" + string(item.SourceID))
				_, err := txn.Get(sourceKey)
				if err == nil {
					// Skip item - duplicate SourceID (idempotent)
					continue
				}
				if !errors.Is(err, badger.ErrKeyNotFound) {
					return errors.Errorf("checking SourceID: %w", err)
				}
			}

			b.uid = b.uid.Next()
			item.ID = []byte(b.uid.String())
			item.CreatedAt = now

			// TODO: GetByPartition buffers from memory pool
			var buf bytes.Buffer
			if err := gob.NewEncoder(&buf).Encode(item); err != nil {
				return errors.Errorf("during gob.Encode(): %w", err)
			}

			if err := txn.Set(item.ID, buf.Bytes()); err != nil {
				return errors.Errorf("during Put(): %w", err)
			}

			// Add SourceID index if set
			if item.SourceID != nil {
				sourceKey := []byte("source:" + string(item.SourceID))
				if err := txn.Set(sourceKey, item.ID); err != nil {
					return errors.Errorf("setting SourceID index: %w", err)
				}
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
				return reply.NewInvalidOption("invalid storage id; '%s': %s", id, err)
			}

			// Get the item to check for SourceID
			kvItem, err := txn.Get(id)
			if err != nil {
				if errors.Is(err, badger.ErrKeyNotFound) {
					// Item doesn't exist, nothing to delete
					continue
				}
				return errors.Errorf("getting item for delete: %w", err)
			}

			var item types.Item
			err = kvItem.Value(func(val []byte) error {
				return gob.NewDecoder(bytes.NewReader(val)).Decode(&item)
			})
			if err != nil {
				return errors.Errorf("decoding item for delete: %w", err)
			}

			// Delete SourceID index if set
			if item.SourceID != nil {
				sourceKey := []byte("source:" + string(item.SourceID))
				if err := txn.Delete(sourceKey); err != nil {
					return errors.Errorf("deleting SourceID index: %w", err)
				}
			}

			if err := txn.Delete(id); err != nil {
				return errors.Errorf("during delete: %w", err)
			}
		}
		return nil
	})
}

func (b *BadgerPartition) Clear(_ context.Context, req types.ClearRequest) error {
	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(txn *badger.Txn) error {
		if req.Destructive && req.Queue {
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
				return err
			}

			item := new(types.Item) // TODO: memory pool
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(item); err != nil {
				return errors.Errorf("during Decode(): %w", err)
			}

			shouldDelete := false

			// Clear scheduled items
			if req.Scheduled && !item.EnqueueAt.IsZero() {
				shouldDelete = true
			}

			// Clear queue items (those with EnqueueAt zero or in past)
			if req.Queue && item.EnqueueAt.IsZero() {
				if req.Destructive || !item.IsLeased {
					shouldDelete = true
				}
			}

			if shouldDelete {
				if err := txn.Delete(k); err != nil {
					return errors.Errorf("during Delete(): %w", err)
				}
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

func (b *BadgerPartition) ScanForScheduled(_ context.Context, now clock.Time) iter.Seq2[types.Action, error] {
	return func(yield func(types.Action, error) bool) {
		db, err := b.getDB()
		if err != nil {
			yield(types.Action{}, err)
			return
		}

		err = db.View(func(txn *badger.Txn) error {
			iter := txn.NewIterator(badger.DefaultIteratorOptions)
			defer iter.Close()

			for iter.Rewind(); iter.Valid(); iter.Next() {
				key := iter.Item().Key()

				// Skip secondary index keys (source:xxx)
				if bytes.HasPrefix(key, []byte("source:")) {
					continue
				}

				var v []byte
				v, err := iter.Item().ValueCopy(v)
				if err != nil {
					return err
				}

				item := new(types.Item)
				if err := gob.NewDecoder(bytes.NewReader(v)).Decode(item); err != nil {
					return errors.Errorf("during Decode(): %w", err)
				}

				// Skip non-scheduled items
				if item.EnqueueAt.IsZero() {
					continue
				}

				// If the scheduled time has passed, yield an action to queue the item
				if now.After(item.EnqueueAt) {
					if !yield(types.Action{
						Action:       types.ActionQueueScheduledItem,
						PartitionNum: b.info.PartitionNum,
						Queue:        b.info.Queue.Name,
						Item:         *item,
					}, nil) {
						return nil
					}
				}
			}
			return nil
		})

		if err != nil {
			yield(types.Action{}, err)
		}
	}
}

func (b *BadgerPartition) ScanForActions(_ context.Context, now clock.Time) iter.Seq2[types.Action, error] {
	return func(yield func(types.Action, error) bool) {
		db, err := b.getDB()
		if err != nil {
			yield(types.Action{}, err)
			return
		}

		err = db.View(func(txn *badger.Txn) error {
			iter := txn.NewIterator(badger.DefaultIteratorOptions)
			defer iter.Close()

			for iter.Rewind(); iter.Valid(); iter.Next() {
				key := iter.Item().Key()

				// Skip secondary index keys (source:xxx)
				if bytes.HasPrefix(key, []byte("source:")) {
					continue
				}

				var v []byte
				v, err := iter.Item().ValueCopy(v)
				if err != nil {
					return err
				}

				item := new(types.Item)
				if err := gob.NewDecoder(bytes.NewReader(v)).Decode(item); err != nil {
					return errors.Errorf("during Decode(): %w", err)
				}

				// Skip scheduled items
				if !item.EnqueueAt.IsZero() {
					continue
				}

				// Is the leased item expired?
				if item.IsLeased {
					if now.After(item.LeaseDeadline) {
						if !yield(types.Action{
							Action:       types.ActionLeaseExpired,
							PartitionNum: b.info.PartitionNum,
							Queue:        b.info.Queue.Name,
							Item:         *item,
						}, nil) {
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
						}, nil) {
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
					}, nil) {
						return nil
					}
					continue
				}
			}
			return nil
		})

		if err != nil {
			yield(types.Action{}, err)
		}
	}
}

func (b *BadgerPartition) TakeAction(_ context.Context, batch types.LifeCycleBatch,
	state *types.PartitionState) error {

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
					if err := txn.Delete(a.Item.ID); err != nil {
						return err
					}

				case types.ActionQueueScheduledItem:
					// Find the scheduled item and move it to the regular queue
					item := new(types.Item)
					kvItem, err := txn.Get(a.Item.ID)
					if err != nil {
						if errors.Is(err, badger.ErrKeyNotFound) {
							b.conf.Log.Warn("unable to find scheduled item while processing action; ignoring action",
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

					if item.EnqueueAt.IsZero() {
						b.conf.Log.Warn("assertion failed; expected to be a scheduled item; action ignored",
							"id", string(a.Item.ID))
						continue
					}

					// Clear the scheduled time and assign a new ID
					item.EnqueueAt = clock.Time{}
					b.uid = b.uid.Next()
					newID := []byte(b.uid.String())
					item.ID = newID

					// Encode and store the updated item with new ID
					var buf bytes.Buffer
					if err := gob.NewEncoder(&buf).Encode(item); err != nil {
						return err
					}

					if err := txn.Set(newID, buf.Bytes()); err != nil {
						return err
					}

					// Delete the old scheduled item
					if err := txn.Delete(a.Item.ID); err != nil {
						return err
					}

					// Tell the partition it gained a new un-leased item
					state.UnLeased++

				default:
					b.conf.Log.Warn("assertion failed; undefined action", "action",
						fmt.Sprintf("0x%X", int(a.Action)))
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
		nextLease := theFuture
		nextExpire := theFuture

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

			// Skip scheduled items
			if !item.EnqueueAt.IsZero() {
				continue
			}

			if item.LeaseDeadline.Before(nextLease) {
				nextLease = item.LeaseDeadline
			}

			if item.ExpireDeadline.Before(nextExpire) {
				nextExpire = item.ExpireDeadline
			}
		}

		if nextLease != theFuture {
			info.NextLeaseExpiry = nextLease
		}

		if nextExpire != theFuture {
			info.NextExpireDeadline = nextExpire
		}
		return nil
	})
}

func (b *BadgerPartition) Stats(_ context.Context, stats *types.PartitionStats, now clock.Time) error {
	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.View(func(txn *badger.Txn) error {

		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		for iter.Rewind(); iter.Valid(); iter.Next() {
			key := iter.Item().Key()

			// Skip secondary index keys (source:xxx)
			if bytes.HasPrefix(key, []byte("source:")) {
				continue
			}

			var v []byte
			v, err := iter.Item().ValueCopy(v)
			if err != nil {
				return errors.Errorf("during GetByPartition value: %w", err)
			}

			item := new(types.Item) // TODO: memory pool
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(item); err != nil {
				return errors.Errorf("during Decode(): %w", err)
			}

			// Count scheduled and do not include them in other stats
			if !item.EnqueueAt.IsZero() {
				stats.Scheduled++
				continue
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
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.db == nil {
		return nil
	}
	err := b.db.Close()
	b.db = nil
	return err
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
	mu   sync.Mutex
	db   *badger.DB
	conf BadgerConfig
}

var _ Queues = &BadgerQueues{}

func (b *BadgerQueues) getDB() (*badger.DB, error) {
	b.mu.Lock()
	if b.db != nil {
		b.mu.Unlock()
		return b.db, nil
	}

	// We store info about the queues in a single db file. We prefix it with `~` to make it
	// impossible for someone to create a queue with the same name.
	dir := filepath.Join(b.conf.StorageDir, fmt.Sprintf("~queue-storage-%s", bucketName))

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
			return reply.NewInvalidOption("invalid queue; '%s' already exists", info.Name)
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
			return reply.NewInvalidOption("lease timeout is too long; %s cannot be greater than the "+
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
			if opts.Namespace != "" && info.Namespace != opts.Namespace {
				continue
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
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.db == nil {
		return nil
	}
	err := b.db.Close()
	b.db = nil
	return err
}

func (b *BadgerQueues) Config() BadgerConfig {
	return b.conf
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

// ---------------------------------------------
// Namespaces Implementation
// ---------------------------------------------

type BadgerNamespaces struct {
	mu   sync.Mutex
	conf BadgerConfig
	db   *badger.DB
}

var _ Namespaces = &BadgerNamespaces{}

func NewBadgerNamespaces(conf BadgerConfig) *BadgerNamespaces {
	set.Default(conf.Log, slog.Default())
	return &BadgerNamespaces{
		conf: conf,
	}
}

func (b *BadgerNamespaces) getDB() (*badger.DB, error) {
	b.mu.Lock()
	if b.db != nil {
		b.mu.Unlock()
		return b.db, nil
	}

	// We store info about namespaces in a single db file. We prefix it with `~` to make it
	// impossible for someone to create a namespace with the same name.
	dir := filepath.Join(b.conf.StorageDir, fmt.Sprintf("~namespace-storage-%s", bucketName))

	opts := badger.DefaultOptions(dir)
	opts.Logger = newBadgerLogger(b.conf.Log)
	db, err := badger.Open(opts)
	if err != nil {
		b.mu.Unlock()
		return nil, errors.Errorf("while opening namespace db '%s': %w", dir, err)
	}

	b.db = db
	b.mu.Unlock()
	return db, nil
}

func (b *BadgerNamespaces) Get(_ context.Context, name string, ns *types.Namespace) error {
	if strings.TrimSpace(name) == "" {
		return types.NewErrNamespaceNotExist(name)
	}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.View(func(txn *badger.Txn) error {
		kvItem, err := txn.Get([]byte(name))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return types.NewErrNamespaceNotExist(name)
			}
			return errors.Errorf("during Get(): %w", err)
		}

		var v []byte
		v, err = kvItem.ValueCopy(v)
		if err != nil {
			return errors.Errorf("during ValueCopy(): %w", err)
		}

		if err := gob.NewDecoder(bytes.NewReader(v)).Decode(ns); err != nil {
			return errors.Errorf("during Decode(): %w", err)
		}
		return nil
	})
}

func (b *BadgerNamespaces) Add(_ context.Context, ns types.Namespace) error {
	if strings.TrimSpace(ns.Name) == "" {
		return reply.NewInvalidOption("namespace name is invalid; cannot be empty")
	}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(txn *badger.Txn) error {
		// If the namespace already exists in the store
		_, err := txn.Get([]byte(ns.Name))
		if err == nil {
			return types.NewErrNamespaceAlreadyExists(ns.Name)
		}
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return errors.Errorf("during Get(): %w", err)
		}

		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(ns); err != nil {
			return errors.Errorf("during gob.Encode(): %w", err)
		}

		if err := txn.Set([]byte(ns.Name), buf.Bytes()); err != nil {
			return errors.Errorf("during Set(): %w", err)
		}
		return nil
	})
}

func (b *BadgerNamespaces) Update(_ context.Context, ns types.Namespace) error {
	if strings.TrimSpace(ns.Name) == "" {
		return reply.NewInvalidOption("namespace name is invalid; cannot be empty")
	}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(ns.Name))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return types.NewErrNamespaceNotExist(ns.Name)
			}
			return errors.Errorf("during Get(): %w", err)
		}

		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(ns); err != nil {
			return errors.Errorf("during gob.Encode(): %w", err)
		}

		if err := txn.Set([]byte(ns.Name), buf.Bytes()); err != nil {
			return errors.Errorf("during Set(): %w", err)
		}
		return nil
	})
}

func (b *BadgerNamespaces) List(_ context.Context, namespaces *[]types.Namespace, opts types.ListOptions) error {
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
				return errors.Errorf("during ValueCopy(): %w", err)
			}

			var ns types.Namespace
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&ns); err != nil {
				return errors.Errorf("during Decode(): %w", err)
			}
			*namespaces = append(*namespaces, ns)
			count++

			if count >= opts.Limit {
				return nil
			}
		}
		return nil
	})
}

func (b *BadgerNamespaces) Delete(_ context.Context, name string) error {
	if strings.TrimSpace(name) == "" {
		return types.NewErrNamespaceNotExist(name)
	}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(txn *badger.Txn) error {
		// Check if namespace exists first
		_, err := txn.Get([]byte(name))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return types.NewErrNamespaceNotExist(name)
			}
			return errors.Errorf("during Get(): %w", err)
		}

		if err := txn.Delete([]byte(name)); err != nil {
			return errors.Errorf("during Delete(%s): %w", name, err)
		}
		return nil
	})
}

func (b *BadgerNamespaces) Close(_ context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.db == nil {
		return nil
	}
	err := b.db.Close()
	b.db = nil
	return err
}

// ---------------------------------------------
// Users Implementation
// ---------------------------------------------

type BadgerUsers struct {
	mu   sync.Mutex
	conf BadgerConfig
	db   *badger.DB
}

var _ Users = &BadgerUsers{}

func NewBadgerUsers(conf BadgerConfig) *BadgerUsers {
	set.Default(conf.Log, slog.Default())
	return &BadgerUsers{
		conf: conf,
	}
}

func (b *BadgerUsers) getDB() (*badger.DB, error) {
	b.mu.Lock()
	if b.db != nil {
		b.mu.Unlock()
		return b.db, nil
	}

	dir := filepath.Join(b.conf.StorageDir, fmt.Sprintf("~user-storage-%s", bucketName))

	opts := badger.DefaultOptions(dir)
	opts.Logger = newBadgerLogger(b.conf.Log)
	db, err := badger.Open(opts)
	if err != nil {
		b.mu.Unlock()
		return nil, errors.Errorf("while opening user db '%s': %w", dir, err)
	}

	b.db = db
	b.mu.Unlock()
	return db, nil
}

func (b *BadgerUsers) Get(_ context.Context, id string, user *types.User) error {
	if strings.TrimSpace(id) == "" {
		return types.NewErrUserNotExist(id)
	}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.View(func(txn *badger.Txn) error {
		kvItem, err := txn.Get([]byte("user:" + id))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return types.NewErrUserNotExist(id)
			}
			return errors.Errorf("during Get(): %w", err)
		}

		var v []byte
		v, err = kvItem.ValueCopy(v)
		if err != nil {
			return errors.Errorf("during ValueCopy(): %w", err)
		}

		if err := gob.NewDecoder(bytes.NewReader(v)).Decode(user); err != nil {
			return errors.Errorf("during Decode(): %w", err)
		}
		return nil
	})
}

func (b *BadgerUsers) GetByUsername(_ context.Context, username string, user *types.User) error {
	if strings.TrimSpace(username) == "" {
		return types.NewErrUserNotExist(username)
	}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.View(func(txn *badger.Txn) error {
		// First lookup the ID by username
		kvItem, err := txn.Get([]byte("user-username:" + username))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return types.NewErrUserNotExist(username)
			}
			return errors.Errorf("during Get(): %w", err)
		}

		var idBytes []byte
		idBytes, err = kvItem.ValueCopy(idBytes)
		if err != nil {
			return errors.Errorf("during ValueCopy(): %w", err)
		}

		// Now get the user by ID
		kvItem, err = txn.Get([]byte("user:" + string(idBytes)))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return types.NewErrUserNotExist(username)
			}
			return errors.Errorf("during Get(): %w", err)
		}

		var v []byte
		v, err = kvItem.ValueCopy(v)
		if err != nil {
			return errors.Errorf("during ValueCopy(): %w", err)
		}

		if err := gob.NewDecoder(bytes.NewReader(v)).Decode(user); err != nil {
			return errors.Errorf("during Decode(): %w", err)
		}
		return nil
	})
}

func (b *BadgerUsers) Add(_ context.Context, user types.User) error {
	if strings.TrimSpace(user.ID) == "" {
		return reply.NewInvalidOption("user id is invalid; cannot be empty")
	}

	if strings.TrimSpace(user.Username) == "" {
		return reply.NewInvalidOption("username is invalid; cannot be empty")
	}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(txn *badger.Txn) error {
		// Check if user ID already exists
		_, err := txn.Get([]byte("user:" + user.ID))
		if err == nil {
			return types.NewErrUserAlreadyExists(user.ID)
		}
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return errors.Errorf("during Get(): %w", err)
		}

		// Check if username is taken
		_, err = txn.Get([]byte("user-username:" + user.Username))
		if err == nil {
			return types.NewErrUsernameAlreadyTaken(user.Username)
		}
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return errors.Errorf("during Get(): %w", err)
		}

		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(user); err != nil {
			return errors.Errorf("during gob.Encode(): %w", err)
		}

		if err := txn.Set([]byte("user:"+user.ID), buf.Bytes()); err != nil {
			return errors.Errorf("during Set(): %w", err)
		}

		// Set username index
		if err := txn.Set([]byte("user-username:"+user.Username), []byte(user.ID)); err != nil {
			return errors.Errorf("during Set(): %w", err)
		}
		return nil
	})
}

func (b *BadgerUsers) List(_ context.Context, users *[]types.User, opts types.ListOptions) error {
	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.View(func(txn *badger.Txn) error {
		var count int
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		prefix := []byte("user:")

		if opts.Pivot != nil {
			iter.Seek([]byte("user:" + string(opts.Pivot)))
		} else {
			iter.Seek(prefix)
		}

		for ; iter.ValidForPrefix(prefix); iter.Next() {
			var v []byte
			v, err := iter.Item().ValueCopy(v)
			if err != nil {
				return errors.Errorf("during ValueCopy(): %w", err)
			}

			var user types.User
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&user); err != nil {
				return errors.Errorf("during Decode(): %w", err)
			}
			*users = append(*users, user)
			count++

			if count >= opts.Limit {
				return nil
			}
		}
		return nil
	})
}

func (b *BadgerUsers) Delete(_ context.Context, id string) error {
	if strings.TrimSpace(id) == "" {
		return types.NewErrUserNotExist(id)
	}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(txn *badger.Txn) error {
		// Get the user first to get the username for index cleanup
		kvItem, err := txn.Get([]byte("user:" + id))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return types.NewErrUserNotExist(id)
			}
			return errors.Errorf("during Get(): %w", err)
		}

		var v []byte
		v, err = kvItem.ValueCopy(v)
		if err != nil {
			return errors.Errorf("during ValueCopy(): %w", err)
		}

		var user types.User
		if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&user); err != nil {
			return errors.Errorf("during Decode(): %w", err)
		}

		// Delete username index
		if err := txn.Delete([]byte("user-username:" + user.Username)); err != nil {
			return errors.Errorf("during Delete(): %w", err)
		}

		// Delete user
		if err := txn.Delete([]byte("user:" + id)); err != nil {
			return errors.Errorf("during Delete(): %w", err)
		}
		return nil
	})
}

func (b *BadgerUsers) Close(_ context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.db == nil {
		return nil
	}
	err := b.db.Close()
	b.db = nil
	return err
}

// ---------------------------------------------
// API Keys Implementation
// ---------------------------------------------

type BadgerAPIKeys struct {
	mu   sync.Mutex
	conf BadgerConfig
	db   *badger.DB
}

var _ APIKeys = &BadgerAPIKeys{}

func NewBadgerAPIKeys(conf BadgerConfig) *BadgerAPIKeys {
	set.Default(conf.Log, slog.Default())
	return &BadgerAPIKeys{
		conf: conf,
	}
}

func (b *BadgerAPIKeys) getDB() (*badger.DB, error) {
	b.mu.Lock()
	if b.db != nil {
		b.mu.Unlock()
		return b.db, nil
	}

	dir := filepath.Join(b.conf.StorageDir, fmt.Sprintf("~apikey-storage-%s", bucketName))

	opts := badger.DefaultOptions(dir)
	opts.Logger = newBadgerLogger(b.conf.Log)
	db, err := badger.Open(opts)
	if err != nil {
		b.mu.Unlock()
		return nil, errors.Errorf("while opening apikey db '%s': %w", dir, err)
	}

	b.db = db
	b.mu.Unlock()
	return db, nil
}

func (b *BadgerAPIKeys) Get(_ context.Context, id string, key *types.APIKey) error {
	if strings.TrimSpace(id) == "" {
		return types.ErrAPIKeyNotExist
	}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.View(func(txn *badger.Txn) error {
		kvItem, err := txn.Get([]byte("apikey:" + id))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return types.ErrAPIKeyNotExist
			}
			return errors.Errorf("during Get(): %w", err)
		}

		var v []byte
		v, err = kvItem.ValueCopy(v)
		if err != nil {
			return errors.Errorf("during ValueCopy(): %w", err)
		}

		if err := gob.NewDecoder(bytes.NewReader(v)).Decode(key); err != nil {
			return errors.Errorf("during Decode(): %w", err)
		}
		return nil
	})
}

func (b *BadgerAPIKeys) GetByHash(_ context.Context, hash string, key *types.APIKey) error {
	if strings.TrimSpace(hash) == "" {
		return types.ErrAPIKeyNotExist
	}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.View(func(txn *badger.Txn) error {
		// First lookup the ID by hash
		kvItem, err := txn.Get([]byte("apikey-hash:" + hash))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return types.ErrAPIKeyNotExist
			}
			return errors.Errorf("during Get(): %w", err)
		}

		var idBytes []byte
		idBytes, err = kvItem.ValueCopy(idBytes)
		if err != nil {
			return errors.Errorf("during ValueCopy(): %w", err)
		}

		// Now get the key by ID
		kvItem, err = txn.Get([]byte("apikey:" + string(idBytes)))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return types.ErrAPIKeyNotExist
			}
			return errors.Errorf("during Get(): %w", err)
		}

		var v []byte
		v, err = kvItem.ValueCopy(v)
		if err != nil {
			return errors.Errorf("during ValueCopy(): %w", err)
		}

		if err := gob.NewDecoder(bytes.NewReader(v)).Decode(key); err != nil {
			return errors.Errorf("during Decode(): %w", err)
		}
		return nil
	})
}

func (b *BadgerAPIKeys) Add(_ context.Context, key types.APIKey) error {
	if strings.TrimSpace(key.ID) == "" {
		return reply.NewInvalidOption("api key id is invalid; cannot be empty")
	}

	if strings.TrimSpace(key.KeyHash) == "" {
		return reply.NewInvalidOption("api key hash is invalid; cannot be empty")
	}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(txn *badger.Txn) error {
		// Check if key ID already exists
		_, err := txn.Get([]byte("apikey:" + key.ID))
		if err == nil {
			return reply.NewInvalidOption("api key already exists")
		}
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return errors.Errorf("during Get(): %w", err)
		}

		// Check if hash is already taken
		_, err = txn.Get([]byte("apikey-hash:" + key.KeyHash))
		if err == nil {
			return reply.NewInvalidOption("api key hash already exists")
		}
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return errors.Errorf("during Get(): %w", err)
		}

		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(key); err != nil {
			return errors.Errorf("during gob.Encode(): %w", err)
		}

		if err := txn.Set([]byte("apikey:"+key.ID), buf.Bytes()); err != nil {
			return errors.Errorf("during Set(): %w", err)
		}

		// Set hash index
		if err := txn.Set([]byte("apikey-hash:"+key.KeyHash), []byte(key.ID)); err != nil {
			return errors.Errorf("during Set(): %w", err)
		}

		// Set user index (append key ID to list)
		userKeyList := []byte("apikey-user:" + key.UserID)
		existingIDs, _ := b.getUserKeyIDs(txn, key.UserID)
		existingIDs = append(existingIDs, key.ID)

		var listBuf bytes.Buffer
		if err := gob.NewEncoder(&listBuf).Encode(existingIDs); err != nil {
			return errors.Errorf("during gob.Encode(): %w", err)
		}
		if err := txn.Set(userKeyList, listBuf.Bytes()); err != nil {
			return errors.Errorf("during Set(): %w", err)
		}

		return nil
	})
}

func (b *BadgerAPIKeys) getUserKeyIDs(txn *badger.Txn, userID string) ([]string, error) {
	kvItem, err := txn.Get([]byte("apikey-user:" + userID))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}

	var v []byte
	v, err = kvItem.ValueCopy(v)
	if err != nil {
		return nil, err
	}

	var ids []string
	if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&ids); err != nil {
		return nil, err
	}
	return ids, nil
}

func (b *BadgerAPIKeys) List(_ context.Context, keys *[]types.APIKey, opts types.ListOptions) error {
	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.View(func(txn *badger.Txn) error {
		var count int
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		prefix := []byte("apikey:")

		if opts.Pivot != nil {
			iter.Seek([]byte("apikey:" + string(opts.Pivot)))
		} else {
			iter.Seek(prefix)
		}

		for ; iter.ValidForPrefix(prefix); iter.Next() {
			var v []byte
			v, err := iter.Item().ValueCopy(v)
			if err != nil {
				return errors.Errorf("during ValueCopy(): %w", err)
			}

			var apiKey types.APIKey
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&apiKey); err != nil {
				return errors.Errorf("during Decode(): %w", err)
			}
			*keys = append(*keys, apiKey)
			count++

			if count >= opts.Limit {
				return nil
			}
		}
		return nil
	})
}

func (b *BadgerAPIKeys) ListByUser(_ context.Context, userID string, keys *[]types.APIKey, opts types.ListOptions) error {
	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.View(func(txn *badger.Txn) error {
		ids, err := b.getUserKeyIDs(txn, userID)
		if err != nil {
			return err
		}
		if ids == nil {
			return nil
		}

		var count int
		var pastPivot bool

		if opts.Pivot == nil {
			pastPivot = true
		}

		for _, id := range ids {
			if count >= opts.Limit {
				return nil
			}

			if !pastPivot {
				if id == string(opts.Pivot) {
					pastPivot = true
				} else {
					continue
				}
			}

			kvItem, err := txn.Get([]byte("apikey:" + id))
			if err != nil {
				continue
			}

			var v []byte
			v, err = kvItem.ValueCopy(v)
			if err != nil {
				return errors.Errorf("during ValueCopy(): %w", err)
			}

			var apiKey types.APIKey
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&apiKey); err != nil {
				return errors.Errorf("during Decode(): %w", err)
			}
			*keys = append(*keys, apiKey)
			count++
		}
		return nil
	})
}

func (b *BadgerAPIKeys) Delete(_ context.Context, id string) error {
	if strings.TrimSpace(id) == "" {
		return nil
	}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(txn *badger.Txn) error {
		// Get the key first for cleanup
		kvItem, err := txn.Get([]byte("apikey:" + id))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			return errors.Errorf("during Get(): %w", err)
		}

		var v []byte
		v, err = kvItem.ValueCopy(v)
		if err != nil {
			return errors.Errorf("during ValueCopy(): %w", err)
		}

		var key types.APIKey
		if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&key); err != nil {
			return errors.Errorf("during Decode(): %w", err)
		}

		// Delete hash index
		if err := txn.Delete([]byte("apikey-hash:" + key.KeyHash)); err != nil {
			return errors.Errorf("during Delete(): %w", err)
		}

		// Update user index
		ids, _ := b.getUserKeyIDs(txn, key.UserID)
		newIDs := make([]string, 0, len(ids))
		for _, existingID := range ids {
			if existingID != id {
				newIDs = append(newIDs, existingID)
			}
		}
		if len(newIDs) > 0 {
			var listBuf bytes.Buffer
			if err := gob.NewEncoder(&listBuf).Encode(newIDs); err != nil {
				return errors.Errorf("during gob.Encode(): %w", err)
			}
			if err := txn.Set([]byte("apikey-user:"+key.UserID), listBuf.Bytes()); err != nil {
				return errors.Errorf("during Set(): %w", err)
			}
		} else {
			_ = txn.Delete([]byte("apikey-user:" + key.UserID))
		}

		// Delete key
		if err := txn.Delete([]byte("apikey:" + id)); err != nil {
			return errors.Errorf("during Delete(): %w", err)
		}
		return nil
	})
}

func (b *BadgerAPIKeys) DeleteByUser(_ context.Context, userID string) error {
	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(txn *badger.Txn) error {
		ids, err := b.getUserKeyIDs(txn, userID)
		if err != nil {
			return errors.Errorf("during getUserKeyIDs(): %w", err)
		}
		if ids == nil {
			return nil
		}

		for _, id := range ids {
			// Get the key for hash cleanup
			kvItem, err := txn.Get([]byte("apikey:" + id))
			if err != nil {
				if errors.Is(err, badger.ErrKeyNotFound) {
					continue
				}
				return errors.Errorf("during Get(): %w", err)
			}

			var v []byte
			v, err = kvItem.ValueCopy(v)
			if err != nil {
				return errors.Errorf("during ValueCopy(): %w", err)
			}

			var key types.APIKey
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&key); err != nil {
				return errors.Errorf("during Decode(): %w", err)
			}

			// Delete hash index
			if err := txn.Delete([]byte("apikey-hash:" + key.KeyHash)); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return errors.Errorf("during Delete(): %w", err)
			}
			// Delete key
			if err := txn.Delete([]byte("apikey:" + id)); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return errors.Errorf("during Delete(): %w", err)
			}
		}

		// Delete user index
		if err := txn.Delete([]byte("apikey-user:" + userID)); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return errors.Errorf("during Delete(): %w", err)
		}
		return nil
	})
}

func (b *BadgerAPIKeys) UpdateLastUsed(_ context.Context, id string, lastUsed clock.Time) error {
	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(txn *badger.Txn) error {
		kvItem, err := txn.Get([]byte("apikey:" + id))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return types.ErrAPIKeyNotExist
			}
			return errors.Errorf("during Get(): %w", err)
		}

		var v []byte
		v, err = kvItem.ValueCopy(v)
		if err != nil {
			return errors.Errorf("during ValueCopy(): %w", err)
		}

		var key types.APIKey
		if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&key); err != nil {
			return errors.Errorf("during Decode(): %w", err)
		}

		key.LastUsedAt = &lastUsed

		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(key); err != nil {
			return errors.Errorf("during gob.Encode(): %w", err)
		}

		if err := txn.Set([]byte("apikey:"+id), buf.Bytes()); err != nil {
			return errors.Errorf("during Set(): %w", err)
		}
		return nil
	})
}

func (b *BadgerAPIKeys) Close(_ context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.db == nil {
		return nil
	}
	err := b.db.Close()
	b.db = nil
	return err
}

// ---------------------------------------------
// Roles Implementation
// ---------------------------------------------

type BadgerRoles struct {
	mu   sync.Mutex
	conf BadgerConfig
	db   *badger.DB
}

var _ Roles = &BadgerRoles{}

func NewBadgerRoles(conf BadgerConfig) *BadgerRoles {
	set.Default(conf.Log, slog.Default())
	return &BadgerRoles{
		conf: conf,
	}
}

func (b *BadgerRoles) getDB() (*badger.DB, error) {
	b.mu.Lock()
	if b.db != nil {
		b.mu.Unlock()
		return b.db, nil
	}

	dir := filepath.Join(b.conf.StorageDir, fmt.Sprintf("~role-storage-%s", bucketName))

	opts := badger.DefaultOptions(dir)
	opts.Logger = newBadgerLogger(b.conf.Log)
	db, err := badger.Open(opts)
	if err != nil {
		b.mu.Unlock()
		return nil, errors.Errorf("while opening role db '%s': %w", dir, err)
	}

	b.db = db
	b.mu.Unlock()
	return db, nil
}

func (b *BadgerRoles) Get(_ context.Context, namespace, name string, role *types.Role) error {
	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.View(func(txn *badger.Txn) error {
		// First lookup the ID by namespace:name
		indexKey := []byte("role-ns-name:" + namespace + ":" + name)
		kvItem, err := txn.Get(indexKey)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return types.NewErrRoleNotExist(namespace + ":" + name)
			}
			return errors.Errorf("during Get(): %w", err)
		}

		var idBytes []byte
		idBytes, err = kvItem.ValueCopy(idBytes)
		if err != nil {
			return errors.Errorf("during ValueCopy(): %w", err)
		}

		// Now get the role by ID
		kvItem, err = txn.Get([]byte("role:" + string(idBytes)))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return types.NewErrRoleNotExist(namespace + ":" + name)
			}
			return errors.Errorf("during Get(): %w", err)
		}

		var v []byte
		v, err = kvItem.ValueCopy(v)
		if err != nil {
			return errors.Errorf("during ValueCopy(): %w", err)
		}

		if err := gob.NewDecoder(bytes.NewReader(v)).Decode(role); err != nil {
			return errors.Errorf("during Decode(): %w", err)
		}
		return nil
	})
}

func (b *BadgerRoles) GetByID(_ context.Context, id string, role *types.Role) error {
	if strings.TrimSpace(id) == "" {
		return types.NewErrRoleNotExist(id)
	}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.View(func(txn *badger.Txn) error {
		kvItem, err := txn.Get([]byte("role:" + id))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return types.NewErrRoleNotExist(id)
			}
			return errors.Errorf("during Get(): %w", err)
		}

		var v []byte
		v, err = kvItem.ValueCopy(v)
		if err != nil {
			return errors.Errorf("during ValueCopy(): %w", err)
		}

		if err := gob.NewDecoder(bytes.NewReader(v)).Decode(role); err != nil {
			return errors.Errorf("during Decode(): %w", err)
		}
		return nil
	})
}

func (b *BadgerRoles) Add(_ context.Context, role types.Role) error {
	if strings.TrimSpace(role.ID) == "" {
		return reply.NewInvalidOption("role id is invalid; cannot be empty")
	}

	if strings.TrimSpace(role.Name) == "" {
		return reply.NewInvalidOption("role name is invalid; cannot be empty")
	}

	if strings.TrimSpace(role.Namespace) == "" {
		return reply.NewInvalidOption("role namespace is invalid; cannot be empty")
	}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(txn *badger.Txn) error {
		// Check if role already exists by namespace:name
		indexKey := []byte("role-ns-name:" + role.Namespace + ":" + role.Name)
		_, err := txn.Get(indexKey)
		if err == nil {
			return types.NewErrRoleAlreadyExists(role.Namespace, role.Name)
		}

		// Encode role
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(role); err != nil {
			return errors.Errorf("during gob.Encode(): %w", err)
		}

		// Store role
		if err := txn.Set([]byte("role:"+role.ID), buf.Bytes()); err != nil {
			return errors.Errorf("during Set(): %w", err)
		}

		// Set namespace:name index
		if err := txn.Set(indexKey, []byte(role.ID)); err != nil {
			return errors.Errorf("during Set(): %w", err)
		}

		return nil
	})
}

func (b *BadgerRoles) Update(_ context.Context, role types.Role) error {
	if strings.TrimSpace(role.ID) == "" {
		return types.NewErrRoleNotExist(role.ID)
	}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(txn *badger.Txn) error {
		// Get existing role
		kvItem, err := txn.Get([]byte("role:" + role.ID))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return types.NewErrRoleNotExist(role.ID)
			}
			return errors.Errorf("during Get(): %w", err)
		}

		var v []byte
		v, err = kvItem.ValueCopy(v)
		if err != nil {
			return errors.Errorf("during ValueCopy(): %w", err)
		}

		var existing types.Role
		if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&existing); err != nil {
			return errors.Errorf("during Decode(): %w", err)
		}

		// Update permissions only
		existing.Permissions = role.Permissions

		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(existing); err != nil {
			return errors.Errorf("during gob.Encode(): %w", err)
		}

		if err := txn.Set([]byte("role:"+role.ID), buf.Bytes()); err != nil {
			return errors.Errorf("during Set(): %w", err)
		}
		return nil
	})
}

func (b *BadgerRoles) List(_ context.Context, namespace string, roles *[]types.Role, opts types.ListOptions) error {
	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.View(func(txn *badger.Txn) error {
		var count int
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		prefix := []byte("role:")

		if opts.Pivot != nil {
			iter.Seek([]byte("role:" + string(opts.Pivot)))
		} else {
			iter.Seek(prefix)
		}

		for ; iter.ValidForPrefix(prefix); iter.Next() {
			var v []byte
			v, err := iter.Item().ValueCopy(v)
			if err != nil {
				return errors.Errorf("during ValueCopy(): %w", err)
			}

			var role types.Role
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&role); err != nil {
				return errors.Errorf("during Decode(): %w", err)
			}

			// Filter by namespace if specified
			if namespace != "" && role.Namespace != namespace {
				continue
			}

			*roles = append(*roles, role)
			count++

			if count >= opts.Limit {
				return nil
			}
		}
		return nil
	})
}

func (b *BadgerRoles) Delete(_ context.Context, id string) error {
	if strings.TrimSpace(id) == "" {
		return types.NewErrRoleNotExist(id)
	}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(txn *badger.Txn) error {
		// Get role first for cleanup
		kvItem, err := txn.Get([]byte("role:" + id))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return types.NewErrRoleNotExist(id)
			}
			return errors.Errorf("during Get(): %w", err)
		}

		var v []byte
		v, err = kvItem.ValueCopy(v)
		if err != nil {
			return errors.Errorf("during ValueCopy(): %w", err)
		}

		var role types.Role
		if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&role); err != nil {
			return errors.Errorf("during Decode(): %w", err)
		}

		// Delete namespace:name index
		indexKey := []byte("role-ns-name:" + role.Namespace + ":" + role.Name)
		if err := txn.Delete(indexKey); err != nil {
			return errors.Errorf("during Delete(): %w", err)
		}

		// Delete role
		if err := txn.Delete([]byte("role:" + id)); err != nil {
			return errors.Errorf("during Delete(): %w", err)
		}
		return nil
	})
}

func (b *BadgerRoles) Close(_ context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.db == nil {
		return nil
	}
	err := b.db.Close()
	b.db = nil
	return err
}

// ---------------------------------------------
// RoleBindings Implementation
// ---------------------------------------------

type BadgerRoleBindings struct {
	mu   sync.Mutex
	conf BadgerConfig
	db   *badger.DB
}

var _ RoleBindings = &BadgerRoleBindings{}

func NewBadgerRoleBindings(conf BadgerConfig) *BadgerRoleBindings {
	set.Default(conf.Log, slog.Default())
	return &BadgerRoleBindings{
		conf: conf,
	}
}

func (b *BadgerRoleBindings) getDB() (*badger.DB, error) {
	b.mu.Lock()
	if b.db != nil {
		b.mu.Unlock()
		return b.db, nil
	}

	dir := filepath.Join(b.conf.StorageDir, fmt.Sprintf("~rolebinding-storage-%s", bucketName))

	opts := badger.DefaultOptions(dir)
	opts.Logger = newBadgerLogger(b.conf.Log)
	db, err := badger.Open(opts)
	if err != nil {
		b.mu.Unlock()
		return nil, errors.Errorf("while opening rolebinding db '%s': %w", dir, err)
	}

	b.db = db
	b.mu.Unlock()
	return db, nil
}

func (b *BadgerRoleBindings) Get(_ context.Context, id string, binding *types.RoleBinding) error {
	if strings.TrimSpace(id) == "" {
		return types.NewErrRoleBindingNotExist(id)
	}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.View(func(txn *badger.Txn) error {
		kvItem, err := txn.Get([]byte("rolebinding:" + id))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return types.NewErrRoleBindingNotExist(id)
			}
			return errors.Errorf("during Get(): %w", err)
		}

		var v []byte
		v, err = kvItem.ValueCopy(v)
		if err != nil {
			return errors.Errorf("during ValueCopy(): %w", err)
		}

		if err := gob.NewDecoder(bytes.NewReader(v)).Decode(binding); err != nil {
			return errors.Errorf("during Decode(): %w", err)
		}
		return nil
	})
}

func (b *BadgerRoleBindings) Add(_ context.Context, binding types.RoleBinding) error {
	if strings.TrimSpace(binding.ID) == "" {
		return reply.NewInvalidOption("role binding id is invalid; cannot be empty")
	}

	if strings.TrimSpace(binding.UserID) == "" {
		return reply.NewInvalidOption("role binding user_id is invalid; cannot be empty")
	}

	if strings.TrimSpace(binding.RoleID) == "" {
		return reply.NewInvalidOption("role binding role_id is invalid; cannot be empty")
	}

	if strings.TrimSpace(binding.Namespace) == "" {
		return reply.NewInvalidOption("role binding namespace is invalid; cannot be empty")
	}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(txn *badger.Txn) error {
		// Check for duplicate (same user, namespace, role combination)
		uniqueKey := []byte("rolebinding-unique:" + binding.UserID + ":" + binding.Namespace + ":" + binding.RoleID)
		_, err := txn.Get(uniqueKey)
		if err == nil {
			return types.NewErrRoleBindingAlreadyExists(binding.UserID, binding.RoleID, binding.Namespace)
		}

		// Encode binding
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(binding); err != nil {
			return errors.Errorf("during gob.Encode(): %w", err)
		}

		// Store binding
		if err := txn.Set([]byte("rolebinding:"+binding.ID), buf.Bytes()); err != nil {
			return errors.Errorf("during Set(): %w", err)
		}

		// Set uniqueness index
		if err := txn.Set(uniqueKey, []byte(binding.ID)); err != nil {
			return errors.Errorf("during Set(): %w", err)
		}

		// Update user index
		userIDs, _ := b.getUserBindingIDs(txn, binding.UserID)
		userIDs = append(userIDs, binding.ID)
		var userBuf bytes.Buffer
		if err := gob.NewEncoder(&userBuf).Encode(userIDs); err != nil {
			return errors.Errorf("during gob.Encode(): %w", err)
		}
		if err := txn.Set([]byte("rolebinding-user:"+binding.UserID), userBuf.Bytes()); err != nil {
			return errors.Errorf("during Set(): %w", err)
		}

		// Update role index
		roleIDs, _ := b.getRoleBindingIDs(txn, binding.RoleID)
		roleIDs = append(roleIDs, binding.ID)
		var roleBuf bytes.Buffer
		if err := gob.NewEncoder(&roleBuf).Encode(roleIDs); err != nil {
			return errors.Errorf("during gob.Encode(): %w", err)
		}
		if err := txn.Set([]byte("rolebinding-role:"+binding.RoleID), roleBuf.Bytes()); err != nil {
			return errors.Errorf("during Set(): %w", err)
		}

		return nil
	})
}

func (b *BadgerRoleBindings) getUserBindingIDs(txn *badger.Txn, userID string) ([]string, error) {
	kvItem, err := txn.Get([]byte("rolebinding-user:" + userID))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}

	var v []byte
	v, err = kvItem.ValueCopy(v)
	if err != nil {
		return nil, err
	}

	var ids []string
	if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&ids); err != nil {
		return nil, err
	}
	return ids, nil
}

func (b *BadgerRoleBindings) getRoleBindingIDs(txn *badger.Txn, roleID string) ([]string, error) {
	kvItem, err := txn.Get([]byte("rolebinding-role:" + roleID))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}

	var v []byte
	v, err = kvItem.ValueCopy(v)
	if err != nil {
		return nil, err
	}

	var ids []string
	if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&ids); err != nil {
		return nil, err
	}
	return ids, nil
}

func (b *BadgerRoleBindings) List(_ context.Context, namespace string, bindings *[]types.RoleBinding, opts types.ListOptions) error {
	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.View(func(txn *badger.Txn) error {
		var count int
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		prefix := []byte("rolebinding:")

		if opts.Pivot != nil {
			iter.Seek([]byte("rolebinding:" + string(opts.Pivot)))
		} else {
			iter.Seek(prefix)
		}

		for ; iter.ValidForPrefix(prefix); iter.Next() {
			var v []byte
			v, err := iter.Item().ValueCopy(v)
			if err != nil {
				return errors.Errorf("during ValueCopy(): %w", err)
			}

			var binding types.RoleBinding
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&binding); err != nil {
				return errors.Errorf("during Decode(): %w", err)
			}

			// Filter by namespace if specified
			if namespace != "" && binding.Namespace != namespace {
				continue
			}

			*bindings = append(*bindings, binding)
			count++

			if count >= opts.Limit {
				return nil
			}
		}
		return nil
	})
}

func (b *BadgerRoleBindings) ListByUser(_ context.Context, userID string, bindings *[]types.RoleBinding) error {
	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.View(func(txn *badger.Txn) error {
		ids, _ := b.getUserBindingIDs(txn, userID)
		if ids == nil {
			return nil
		}

		for _, id := range ids {
			kvItem, err := txn.Get([]byte("rolebinding:" + id))
			if err != nil {
				continue
			}

			var v []byte
			v, err = kvItem.ValueCopy(v)
			if err != nil {
				continue
			}

			var binding types.RoleBinding
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&binding); err != nil {
				continue
			}
			*bindings = append(*bindings, binding)
		}
		return nil
	})
}

func (b *BadgerRoleBindings) ListByRole(_ context.Context, roleID string, bindings *[]types.RoleBinding) error {
	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.View(func(txn *badger.Txn) error {
		ids, _ := b.getRoleBindingIDs(txn, roleID)
		if ids == nil {
			return nil
		}

		for _, id := range ids {
			kvItem, err := txn.Get([]byte("rolebinding:" + id))
			if err != nil {
				continue
			}

			var v []byte
			v, err = kvItem.ValueCopy(v)
			if err != nil {
				continue
			}

			var binding types.RoleBinding
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&binding); err != nil {
				continue
			}
			*bindings = append(*bindings, binding)
		}
		return nil
	})
}

func (b *BadgerRoleBindings) DeleteByUserAndRole(_ context.Context, namespace, userID, roleID string) error {
	db, err := b.getDB()
	if err != nil {
		return err
	}

	notExist := types.NewErrRoleBindingNotExist(namespace + ":" + userID + ":" + roleID)

	return db.Update(func(txn *badger.Txn) error {
		uniqueKey := []byte("rolebinding-unique:" + userID + ":" + namespace + ":" + roleID)
		kvItem, err := txn.Get(uniqueKey)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return notExist
			}
			return errors.Errorf("during Get(): %w", err)
		}

		var v []byte
		v, err = kvItem.ValueCopy(v)
		if err != nil {
			return errors.Errorf("during ValueCopy(): %w", err)
		}
		id := string(v)

		_ = txn.Delete(uniqueKey)

		userIDs, _ := b.getUserBindingIDs(txn, userID)
		newUserIDs := make([]string, 0, len(userIDs))
		for _, existingID := range userIDs {
			if existingID != id {
				newUserIDs = append(newUserIDs, existingID)
			}
		}
		if len(newUserIDs) > 0 {
			var userBuf bytes.Buffer
			if err := gob.NewEncoder(&userBuf).Encode(newUserIDs); err != nil {
				return errors.Errorf("during gob.Encode(): %w", err)
			}
			if err := txn.Set([]byte("rolebinding-user:"+userID), userBuf.Bytes()); err != nil {
				return errors.Errorf("during Set(): %w", err)
			}
		} else {
			_ = txn.Delete([]byte("rolebinding-user:" + userID))
		}

		roleIDs, _ := b.getRoleBindingIDs(txn, roleID)
		newRoleIDs := make([]string, 0, len(roleIDs))
		for _, existingID := range roleIDs {
			if existingID != id {
				newRoleIDs = append(newRoleIDs, existingID)
			}
		}
		if len(newRoleIDs) > 0 {
			var roleBuf bytes.Buffer
			if err := gob.NewEncoder(&roleBuf).Encode(newRoleIDs); err != nil {
				return errors.Errorf("during gob.Encode(): %w", err)
			}
			if err := txn.Set([]byte("rolebinding-role:"+roleID), roleBuf.Bytes()); err != nil {
				return errors.Errorf("during Set(): %w", err)
			}
		} else {
			_ = txn.Delete([]byte("rolebinding-role:" + roleID))
		}

		if err := txn.Delete([]byte("rolebinding:" + id)); err != nil {
			return errors.Errorf("during Delete(): %w", err)
		}
		return nil
	})
}

func (b *BadgerRoleBindings) Delete(_ context.Context, id string) error {
	if strings.TrimSpace(id) == "" {
		return types.NewErrRoleBindingNotExist(id)
	}

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(txn *badger.Txn) error {
		// Get binding first for cleanup
		kvItem, err := txn.Get([]byte("rolebinding:" + id))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return types.NewErrRoleBindingNotExist(id)
			}
			return errors.Errorf("during Get(): %w", err)
		}

		var v []byte
		v, err = kvItem.ValueCopy(v)
		if err != nil {
			return errors.Errorf("during ValueCopy(): %w", err)
		}

		var binding types.RoleBinding
		if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&binding); err != nil {
			return errors.Errorf("during Decode(): %w", err)
		}

		// Delete uniqueness index
		uniqueKey := []byte("rolebinding-unique:" + binding.UserID + ":" + binding.Namespace + ":" + binding.RoleID)
		_ = txn.Delete(uniqueKey)

		// Update user index
		userIDs, _ := b.getUserBindingIDs(txn, binding.UserID)
		newUserIDs := make([]string, 0, len(userIDs))
		for _, existingID := range userIDs {
			if existingID != id {
				newUserIDs = append(newUserIDs, existingID)
			}
		}
		if len(newUserIDs) > 0 {
			var userBuf bytes.Buffer
			if err := gob.NewEncoder(&userBuf).Encode(newUserIDs); err != nil {
				return errors.Errorf("during gob.Encode(): %w", err)
			}
			if err := txn.Set([]byte("rolebinding-user:"+binding.UserID), userBuf.Bytes()); err != nil {
				return errors.Errorf("during Set(): %w", err)
			}
		} else {
			_ = txn.Delete([]byte("rolebinding-user:" + binding.UserID))
		}

		// Update role index
		roleIDs, _ := b.getRoleBindingIDs(txn, binding.RoleID)
		newRoleIDs := make([]string, 0, len(roleIDs))
		for _, existingID := range roleIDs {
			if existingID != id {
				newRoleIDs = append(newRoleIDs, existingID)
			}
		}
		if len(newRoleIDs) > 0 {
			var roleBuf bytes.Buffer
			if err := gob.NewEncoder(&roleBuf).Encode(newRoleIDs); err != nil {
				return errors.Errorf("during gob.Encode(): %w", err)
			}
			if err := txn.Set([]byte("rolebinding-role:"+binding.RoleID), roleBuf.Bytes()); err != nil {
				return errors.Errorf("during Set(): %w", err)
			}
		} else {
			_ = txn.Delete([]byte("rolebinding-role:" + binding.RoleID))
		}

		// Delete binding
		if err := txn.Delete([]byte("rolebinding:" + id)); err != nil {
			return errors.Errorf("during Delete(): %w", err)
		}
		return nil
	})
}

func (b *BadgerRoleBindings) DeleteByUser(_ context.Context, userID string) error {
	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(txn *badger.Txn) error {
		ids, err := b.getUserBindingIDs(txn, userID)
		if err != nil {
			return errors.Errorf("during getUserBindingIDs(): %w", err)
		}
		if ids == nil {
			return nil
		}

		for _, id := range ids {
			// Get binding for cleanup
			kvItem, err := txn.Get([]byte("rolebinding:" + id))
			if err != nil {
				if errors.Is(err, badger.ErrKeyNotFound) {
					continue
				}
				return errors.Errorf("during Get(): %w", err)
			}

			var v []byte
			v, err = kvItem.ValueCopy(v)
			if err != nil {
				return errors.Errorf("during ValueCopy(): %w", err)
			}

			var binding types.RoleBinding
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&binding); err != nil {
				return errors.Errorf("during Decode(): %w", err)
			}

			// Delete uniqueness index
			uniqueKey := []byte("rolebinding-unique:" + binding.UserID + ":" + binding.Namespace + ":" + binding.RoleID)
			if err := txn.Delete(uniqueKey); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return errors.Errorf("during Delete(): %w", err)
			}

			// Update role index
			roleIDs, err := b.getRoleBindingIDs(txn, binding.RoleID)
			if err != nil {
				return errors.Errorf("during getRoleBindingIDs(): %w", err)
			}
			newRoleIDs := make([]string, 0, len(roleIDs))
			for _, existingID := range roleIDs {
				if existingID != id {
					newRoleIDs = append(newRoleIDs, existingID)
				}
			}
			if len(newRoleIDs) > 0 {
				var roleBuf bytes.Buffer
				if err := gob.NewEncoder(&roleBuf).Encode(newRoleIDs); err != nil {
					return errors.Errorf("during Encode(): %w", err)
				}
				if err := txn.Set([]byte("rolebinding-role:"+binding.RoleID), roleBuf.Bytes()); err != nil {
					return errors.Errorf("during Set(): %w", err)
				}
			} else {
				if err := txn.Delete([]byte("rolebinding-role:" + binding.RoleID)); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
					return errors.Errorf("during Delete(): %w", err)
				}
			}

			// Delete binding
			if err := txn.Delete([]byte("rolebinding:" + id)); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return errors.Errorf("during Delete(): %w", err)
			}
		}

		// Delete user index
		if err := txn.Delete([]byte("rolebinding-user:" + userID)); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return errors.Errorf("during Delete(): %w", err)
		}
		return nil
	})
}

func (b *BadgerRoleBindings) Close(_ context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.db == nil {
		return nil
	}
	err := b.db.Close()
	b.db = nil
	return err
}
