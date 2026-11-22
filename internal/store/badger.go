package store

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
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
	"github.com/kapetan-io/tiger/assert"
	"github.com/segmentio/ksuid"
)

var sourcePrefix = []byte("source:")

const (
	clearPageSize  = 1_000
	clearPageBytes = 1 << 20
)

type BadgerConfig struct {
	StorageDir   string
	Log          *slog.Logger
	MemTableSize int64
}

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

type BadgerPartition struct {
	info       types.PartitionInfo
	conf       BadgerConfig
	mu         sync.RWMutex
	uid        ksuid.KSUID
	db         *badger.DB
	leasePivot []byte
}

func (b *BadgerPartition) Produce(_ context.Context, batch types.ProduceBatch, now clock.Time) error {
	db, err := b.getDB()
	if err != nil {
		return err
	}

	splittable := batch.HandOffsOnly()
	txn := db.NewTransaction(true)
	defer func() { txn.Discard() }()
	var pending []producedWrites

	for _, r := range batch.Requests {
		r.Stored = 0
		for _, item := range r.Items {
			if item.SourceID != nil {
				_, err := txn.Get([]byte("source:" + string(item.SourceID)))
				if err == nil {
					pending = append(pending, producedWrites{request: r})
					continue
				}
				if !errors.Is(err, badger.ErrKeyNotFound) {
					return errors.Errorf("checking SourceID: %w", err)
				}
			}

			b.uid = b.uid.Next()
			item.ID = []byte(b.uid.String())
			item.CreatedAt = now

			if item.EnqueueAt.Before(now.Add(time.Millisecond * 100)) {
				item.EnqueueAt = clock.Time{}
			}

			var buf bytes.Buffer
			if err := gob.NewEncoder(&buf).Encode(item); err != nil {
				return errors.Errorf("during gob.Encode(): %w", err)
			}

			produced := producedWrites{request: r, writes: []badgerWrite{{key: item.ID, value: buf.Bytes()}}}
			if item.SourceID != nil {
				produced.writes = append(produced.writes, badgerWrite{key: []byte("source:" + string(item.SourceID)), value: item.ID})
			}

			err := applyWrites(txn, produced.writes)
			if errors.Is(err, badger.ErrTxnTooBig) && splittable {
				txn.Discard()
				if err := commitProducedInNewTransaction(db, pending); err != nil {
					return errors.Errorf("during Commit(): %w", err)
				}
				pending = pending[:0]
				txn = db.NewTransaction(true)
				err = applyWrites(txn, produced.writes)
			}
			if err != nil {
				return errors.Errorf("during Set(): %w", err)
			}
			pending = append(pending, produced)

			if item.EnqueueAt.IsZero() {
				b.lowerLeasePivot(item.ID)
			}
		}
	}
	return txn.Commit()
}

type producedWrites struct {
	request *types.ProduceRequest
	writes  []badgerWrite
}

func commitProducedInNewTransaction(db *badger.DB, pending []producedWrites) error {
	txn := db.NewTransaction(true)
	defer txn.Discard()
	for _, produced := range pending {
		if err := applyWrites(txn, produced.writes); err != nil {
			return err
		}
	}
	if err := txn.Commit(); err != nil {
		return err
	}
	for _, produced := range pending {
		produced.request.Stored++
	}
	return nil
}

func (b *BadgerPartition) Lease(ctx context.Context, batch types.LeaseBatch, opts LeaseOptions) error {
	db, err := b.getDB()
	if err != nil {
		return err
	}

	var pivot []byte
	err = db.Update(func(txn *badger.Txn) error {
		var err error
		pivot, err = b.leaseItems(ctx, txn, batch, opts)
		return err
	})
	if err != nil {
		for _, r := range batch.Requests {
			if r != nil {
				r.Items = r.Items[:0]
			}
		}
		return err
	}
	b.leasePivot = pivot
	return nil
}

func (b *BadgerPartition) leaseItems(ctx context.Context, txn *badger.Txn, batch types.LeaseBatch,
	opts LeaseOptions) (pivot []byte, err error) {

	batchIter := batch.Iterator()
	var count int
	iter := txn.NewIterator(badger.IteratorOptions{
		PrefetchValues: false,
		PrefetchSize:   100,
		Reverse:        false,
		AllVersions:    false,
	})

	defer iter.Close()

	for iter.Seek(b.leasePivot); iter.Valid() && ctx.Err() == nil; iter.Next() {
		if count >= batch.TotalRequested {
			break
		}

		key := iter.Item().Key()
		if bytes.HasPrefix(key, sourcePrefix) {
			break
		}

		var v []byte
		v, err := iter.Item().ValueCopy(v)
		if err != nil {
			return nil, errors.Errorf("while getting value: %w", err)
		}

		item := new(types.Item)
		if err := gob.NewDecoder(bytes.NewReader(v)).Decode(item); err != nil {
			return nil, errors.Errorf("during Decode(): %w", err)
		}

		if item.IsLeased {
			continue
		}

		if !item.EnqueueAt.IsZero() {
			continue
		}

		item.LeaseDeadline = opts.LeaseDeadline
		item.IsLeased = true
		item.Attempts++

		if !batchIter.Next(item) {
			break
		}
		count++

		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(item); err != nil {
			return nil, errors.Errorf("during gob.Encode(): %w", err)
		}

		if err := txn.Set(item.ID, buf.Bytes()); err != nil {
			if errors.Is(err, badger.ErrTxnTooBig) {
				batch.Unassign([]*types.Item{item})
				break
			}
			return nil, errors.Errorf("during Put(): %w", err)
		}
	}

	if iter.Valid() && !bytes.HasPrefix(iter.Item().Key(), sourcePrefix) {
		return iter.Item().KeyCopy(nil), nil
	}
	return sourcePrefix, nil
}

func (b *BadgerPartition) lowerLeasePivot(id []byte) {
	if bytes.Compare(id, b.leasePivot) < 0 {
		b.leasePivot = bytes.Clone(id)
	}
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
			txn.Discard()
		}
	}()

	for _, req := range batch.Requests {
		if err = b.completeItems(txn, req); err != nil {
			batch.MarkRolledBack()
			return err
		}
	}

	err = txn.Commit()
	if err != nil {
		batch.MarkRolledBack()
		return errors.Errorf("during Commit(): %w", err)
	}

	done = true
	return nil
}

func (b *BadgerPartition) completeItems(txn *badger.Txn, req *types.CompleteRequest) error {
	for _, id := range req.Ids {
		if err := b.validateID(id); err != nil {
			req.Err = reply.NewInvalidOption("invalid storage id; '%s': %s", id, err)
			return nil
		}

		kvItem, err := txn.Get(id)
		if err != nil {
			req.Err = reply.NewInvalidOption("invalid storage id; '%s' does not exist", id)
			return nil
		}
		var v []byte
		v, err = kvItem.ValueCopy(v)
		if err != nil {
			req.Err = reply.NewInvalidOption("invalid storage id; '%s' does not exist", id)
			return nil
		}

		item := new(types.Item)
		if err = gob.NewDecoder(bytes.NewReader(v)).Decode(item); err != nil {
			return errors.Errorf("during Decode(): %w", err)
		}

		if !item.IsLeased {
			req.Err = reply.NewConflict("item(s) cannot be completed; '%s' is not "+
				"marked as leased", id)
			return nil
		}

		if item.SourceID != nil {
			sourceKey := []byte("source:" + string(item.SourceID))
			if err = txn.Delete(sourceKey); err != nil {
				return errors.Errorf("during Delete(source:%s): %w", item.SourceID, err)
			}
		}

		if err = txn.Delete(id); err != nil {
			return errors.Errorf("during Delete(%s): %w", id, err)
		}
		req.Completed++
	}
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

	for _, req := range batch.Requests {
		if err = b.retryItems(txn, req); err != nil {
			batch.MarkRolledBack()
			return err
		}
	}

	err = txn.Commit()
	if err != nil {
		batch.MarkRolledBack()
		return errors.Errorf("during Commit(): %w", err)
	}

	done = true
	return nil
}

func (b *BadgerPartition) retryItems(txn *badger.Txn, req *types.RetryRequest) error {
	for _, retryItem := range req.Items {
		if err := b.validateID(retryItem.ID); err != nil {
			req.Err = reply.NewInvalidOption("invalid storage id; '%s': %s", retryItem.ID, err)
			return nil
		}

		kvItem, err := txn.Get(retryItem.ID)
		if err != nil {
			req.Err = reply.NewInvalidOption("invalid storage id; '%s' does not exist", retryItem.ID)
			return nil
		}
		var v []byte
		v, err = kvItem.ValueCopy(v)
		if err != nil {
			req.Err = reply.NewInvalidOption("invalid storage id; '%s' does not exist", retryItem.ID)
			return nil
		}

		item := new(types.Item)
		if err = gob.NewDecoder(bytes.NewReader(v)).Decode(item); err != nil {
			return errors.Errorf("during Decode(): %w", err)
		}

		if !item.IsLeased {
			req.Err = reply.NewConflict("item(s) cannot be retried; '%s' is not "+
				"marked as leased", retryItem.ID)
			return nil
		}

		switch {
		case retryItem.Dead:
			if item.SourceID != nil {
				sourceKey := []byte("source:" + string(item.SourceID))
				if err = txn.Delete(sourceKey); err != nil {
					return errors.Errorf("during Delete(source:%s): %w", item.SourceID, err)
				}
			}
			if err = txn.Delete(retryItem.ID); err != nil {
				return errors.Errorf("during Delete(%s): %w", retryItem.ID, err)
			}
		case !retryItem.RetryAt.IsZero():
			now := clock.Now().UTC()
			if retryItem.RetryAt.Before(now.Add(time.Millisecond * 100)) {
				item.IsLeased = false
				item.LeaseDeadline = clock.Time{}
				item.EnqueueAt = clock.Time{}
				b.uid = b.uid.Next()
				item.ID = []byte(b.uid.String())
				var buf bytes.Buffer
				if err := gob.NewEncoder(&buf).Encode(item); err != nil {
					return errors.Errorf("during gob.Encode(): %w", err)
				}
				if err := txn.Set(item.ID, buf.Bytes()); err != nil {
					return errors.Errorf("during Set(%s): %w", item.ID, err)
				}
				b.lowerLeasePivot(item.ID)
				if err := txn.Delete(retryItem.ID); err != nil {
					return errors.Errorf("during Delete(%s): %w", retryItem.ID, err)
				}
				req.UnLeased++
			} else {
				item.IsLeased = false
				item.LeaseDeadline = clock.Time{}
				item.EnqueueAt = retryItem.RetryAt
				var buf bytes.Buffer
				if err := gob.NewEncoder(&buf).Encode(item); err != nil {
					return errors.Errorf("during gob.Encode(): %w", err)
				}
				if err := txn.Set(retryItem.ID, buf.Bytes()); err != nil {
					return errors.Errorf("during Set(%s): %w", retryItem.ID, err)
				}
			}
		default:
			item.IsLeased = false
			item.LeaseDeadline = clock.Time{}
			b.uid = b.uid.Next()
			item.ID = []byte(b.uid.String())
			var buf bytes.Buffer
			if err := gob.NewEncoder(&buf).Encode(item); err != nil {
				return errors.Errorf("during gob.Encode(): %w", err)
			}
			if err := txn.Set(item.ID, buf.Bytes()); err != nil {
				return errors.Errorf("during Set(%s): %w", item.ID, err)
			}
			b.lowerLeasePivot(item.ID)
			if err := txn.Delete(retryItem.ID); err != nil {
				return errors.Errorf("during Delete(%s): %w", retryItem.ID, err)
			}
			req.UnLeased++
		}
		req.Retried++
	}
	return nil
}

func (b *BadgerPartition) List(ctx context.Context, items *[]*types.Item, opts types.ListOptions) error {
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

		for ; iter.Valid() && ctx.Err() == nil; iter.Next() {
			key := iter.Item().Key()

			if bytes.HasPrefix(key, []byte("source:")) {
				continue
			}

			var v []byte
			v, err := iter.Item().ValueCopy(v)
			if err != nil {
				return errors.Errorf("during GetByPartition value: %w", err)
			}

			item := new(types.Item)
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(item); err != nil {
				return errors.Errorf("during Decode(): %w", err)
			}

			if !item.EnqueueAt.IsZero() {
				continue
			}

			*items = append(*items, item)
			count++

			if count >= opts.Limit {
				return nil
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		return nil
	})
}

func (b *BadgerPartition) ListScheduled(ctx context.Context, items *[]*types.Item, opts types.ListOptions) error {
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

		for ; iter.Valid() && ctx.Err() == nil; iter.Next() {
			key := iter.Item().Key()

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

			if item.EnqueueAt.IsZero() {
				continue
			}

			*items = append(*items, item)
			count++

			if count >= opts.Limit {
				return nil
			}
		}
		if err := ctx.Err(); err != nil {
			return err
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
			if item.SourceID != nil {
				sourceKey := []byte("source:" + string(item.SourceID))
				_, err := txn.Get(sourceKey)
				if err == nil {
					continue
				}
				if !errors.Is(err, badger.ErrKeyNotFound) {
					return errors.Errorf("checking SourceID: %w", err)
				}
			}

			b.uid = b.uid.Next()
			item.ID = []byte(b.uid.String())
			item.CreatedAt = now

			var buf bytes.Buffer
			if err := gob.NewEncoder(&buf).Encode(item); err != nil {
				return errors.Errorf("during gob.Encode(): %w", err)
			}

			if err := txn.Set(item.ID, buf.Bytes()); err != nil {
				return errors.Errorf("during Put(): %w", err)
			}
			if !item.IsLeased && item.EnqueueAt.IsZero() {
				b.lowerLeasePivot(item.ID)
			}

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

			kvItem, err := txn.Get(id)
			if err != nil {
				if errors.Is(err, badger.ErrKeyNotFound) {
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

func (b *BadgerPartition) Clear(ctx context.Context, req types.ClearRequest) error {
	db, err := b.getDB()
	if err != nil {
		return err
	}

	if req.Destructive && req.Queue && req.Scheduled {
		if err := db.DropAll(); err != nil {
			return errors.Errorf("during destructive DropAll(): %w", err)
		}
		return nil
	}

	var pivot []byte
	for ctx.Err() == nil {
		if err := db.Update(func(txn *badger.Txn) error {
			var err error
			pivot, err = clearPage(ctx, txn, pivot, req)
			return err
		}); err != nil {
			return err
		}
		if pivot == nil {
			return nil
		}
	}
	return ctx.Err()
}

func clearPage(ctx context.Context, txn *badger.Txn, pivot []byte, req types.ClearRequest) ([]byte, error) {
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()

	var touched, written int
	for iter.Seek(pivot); iter.Valid() && ctx.Err() == nil; iter.Next() {
		if touched >= clearPageSize {
			return iter.Item().KeyCopy(nil), nil
		}

		k := iter.Item().KeyCopy(nil)
		if bytes.HasPrefix(k, sourcePrefix) {
			return nil, nil
		}
		touched++

		v, err := iter.Item().ValueCopy(nil)
		if err != nil {
			return nil, err
		}

		item := new(types.Item)
		if err := gob.NewDecoder(bytes.NewReader(v)).Decode(item); err != nil {
			return nil, errors.Errorf("during Decode(): %w", err)
		}

		if !clearSelects(req, item) {
			continue
		}

		var sourceKey []byte
		if item.SourceID != nil {
			sourceKey = []byte("source:" + string(item.SourceID))
		}
		if written > 0 && written+len(k)+len(sourceKey) > clearPageBytes {
			return k, nil
		}
		written += len(k) + len(sourceKey)

		if sourceKey != nil {
			touched++
			if err := txn.Delete(sourceKey); err != nil {
				return nil, errors.Errorf("deleting SourceID index: %w", err)
			}
		}

		if err := txn.Delete(k); err != nil {
			return nil, errors.Errorf("during Delete(): %w", err)
		}
	}
	if iter.Valid() {
		return iter.Item().KeyCopy(nil), nil
	}
	return nil, nil
}

func clearSelects(req types.ClearRequest, item *types.Item) bool {
	if req.Scheduled && !item.EnqueueAt.IsZero() {
		return true
	}
	return req.Queue && item.EnqueueAt.IsZero() && (req.Destructive || !item.IsLeased)
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

func (b *BadgerPartition) ScanForScheduled(ctx context.Context, scan *types.LifeCycleScan, now clock.Time) error {
	return b.readLifeCyclePage(ctx, scan, func(item *types.Item) {
		if item.EnqueueAt.IsZero() {
			return
		}
		if item.EnqueueAt.After(now) {
			scan.SeeEnqueueAt(item.EnqueueAt)
			return
		}
		scan.Add(types.Action{
			Action:       types.ActionQueueScheduledItem,
			PartitionNum: b.info.PartitionNum,
			Queue:        b.info.Queue.Name,
			Item:         *item,
		})
	})
}

func (b *BadgerPartition) ScanForActions(ctx context.Context, scan *types.LifeCycleScan, now clock.Time) error {
	return b.readLifeCyclePage(ctx, scan, func(item *types.Item) {
		if !item.EnqueueAt.IsZero() {
			return
		}
		if item.IsLeased {
			if !item.LeaseDeadline.After(now) {
				scan.Add(types.Action{
					Action:       types.ActionLeaseExpired,
					PartitionNum: b.info.PartitionNum,
					Queue:        b.info.Queue.Name,
					Item:         *item,
				})
				return
			}
			if b.info.Queue.MaxAttempts != 0 && item.Attempts >= b.info.Queue.MaxAttempts {
				scan.Add(types.Action{
					Action:       types.ActionItemMaxAttempts,
					PartitionNum: b.info.PartitionNum,
					Queue:        b.info.Queue.Name,
					Item:         *item,
				})
				return
			}
			scan.SeeLeaseDeadline(item.LeaseDeadline)
		}
		if !item.ExpireDeadline.After(now) {
			scan.Add(types.Action{
				Action:       types.ActionItemExpired,
				PartitionNum: b.info.PartitionNum,
				Queue:        b.info.Queue.Name,
				Item:         *item,
			})
			return
		}
		scan.SeeExpireDeadline(item.ExpireDeadline)
	})
}

func (b *BadgerPartition) readLifeCyclePage(ctx context.Context, scan *types.LifeCycleScan, visit func(*types.Item)) error {
	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.View(func(txn *badger.Txn) error {
		tail := scan.Last
		if !scan.MidPass() {
			tail = lastItemID(txn)
		}
		if tail == nil {
			scan.EndPass()
			return nil
		}

		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		var pivot []byte
		var read int
		for iter.Seek(scan.Pivot); iter.Valid() && ctx.Err() == nil; iter.Next() {
			key := iter.Item().Key()
			if bytes.Equal(key, scan.Pivot) {
				continue
			}
			if bytes.Compare(key, tail) > 0 {
				break
			}
			if read == LifeCyclePageSize {
				scan.Pivot, scan.Last = pivot, tail
				return nil
			}
			read++

			v, err := iter.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			item := new(types.Item)
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(item); err != nil {
				return errors.Errorf("during Decode(): %w", err)
			}
			visit(item)
			pivot = iter.Item().KeyCopy(nil)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		scan.EndPass()
		return nil
	})
}

func lastItemID(txn *badger.Txn) []byte {
	iter := txn.NewIterator(badger.IteratorOptions{Reverse: true})
	defer iter.Close()

	iter.Seek(sourcePrefix)
	if !iter.Valid() || bytes.HasPrefix(iter.Item().Key(), sourcePrefix) {
		return nil
	}
	return iter.Item().KeyCopy(nil)
}

type badgerWrite struct {
	key    []byte
	value  []byte
	delete bool
}

type actionWriteSet struct {
	writes   []badgerWrite
	unLeased int
	expired  int
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

	txn := db.NewTransaction(true)
	defer func() { txn.Discard() }()
	var pending []actionWriteSet

	for _, req := range batch.Requests {
		for _, a := range req.Actions {
			action, err := b.actionWrites(txn, a)
			if err != nil {
				return err
			}

			err = applyWrites(txn, action.writes)
			if errors.Is(err, badger.ErrTxnTooBig) {
				txn.Discard()
				if err := commitInNewTransaction(db, pending, state); err != nil {
					return err
				}
				pending = pending[:0]
				txn = db.NewTransaction(true)
				err = applyWrites(txn, action.writes)
			}
			if err != nil {
				return err
			}
			pending = append(pending, action)
		}
	}

	return commitActions(txn, pending, state)
}

func commitInNewTransaction(db *badger.DB, pending []actionWriteSet, state *types.PartitionState) error {
	txn := db.NewTransaction(true)
	defer txn.Discard()
	for _, action := range pending {
		if err := applyWrites(txn, action.writes); err != nil {
			return err
		}
	}
	return commitActions(txn, pending, state)
}

func commitActions(txn *badger.Txn, pending []actionWriteSet, state *types.PartitionState) error {
	if err := txn.Commit(); err != nil {
		return err
	}
	for _, action := range pending {
		state.UnLeased += action.unLeased
		state.LeasesExpired(action.expired)
	}
	return nil
}

func applyWrites(txn *badger.Txn, writes []badgerWrite) error {
	for _, w := range writes {
		if w.delete {
			if err := txn.Delete(w.key); err != nil {
				return err
			}
			continue
		}
		if err := txn.Set(w.key, w.value); err != nil {
			return err
		}
	}
	return nil
}

func (b *BadgerPartition) actionWrites(txn *badger.Txn, a types.Action) (actionWriteSet, error) {
	switch a.Action {
	case types.ActionLeaseExpired:
		item, err := getItem(txn, a.Item.ID)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				b.conf.Log.Warn("unable to find item while processing action; ignoring action",
					"id", a.Item.ID, "action", types.ActionToString(a.Action))
				return actionWriteSet{}, nil
			}
			return actionWriteSet{}, err
		}

		item.LeaseDeadline = clock.Time{}
		item.IsLeased = false
		b.uid = b.uid.Next()
		item.ID = []byte(b.uid.String())

		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(item); err != nil {
			return actionWriteSet{}, err
		}
		b.lowerLeasePivot(item.ID)
		return actionWriteSet{expired: 1, writes: []badgerWrite{
			{key: item.ID, value: buf.Bytes()},
			{key: a.Item.ID, delete: true},
		}}, nil

	case types.ActionDeleteItem:
		var writes []badgerWrite
		if a.Item.SourceID != nil {
			writes = append(writes, badgerWrite{key: []byte("source:" + string(a.Item.SourceID)), delete: true})
		}
		return actionWriteSet{writes: append(writes, badgerWrite{key: a.Item.ID, delete: true})}, nil

	case types.ActionQueueScheduledItem:
		item, err := getItem(txn, a.Item.ID)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				b.conf.Log.Warn("unable to find scheduled item while processing action; ignoring action",
					"id", a.Item.ID, "action", types.ActionToString(a.Action))
				return actionWriteSet{}, nil
			}
			return actionWriteSet{}, err
		}

		if item.EnqueueAt.IsZero() {
			b.conf.Log.Warn("assertion failed; expected to be a scheduled item; action ignored",
				"id", string(a.Item.ID))
			return actionWriteSet{}, nil
		}

		item.EnqueueAt = clock.Time{}
		b.uid = b.uid.Next()
		item.ID = []byte(b.uid.String())

		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(item); err != nil {
			return actionWriteSet{}, err
		}
		b.lowerLeasePivot(item.ID)
		return actionWriteSet{unLeased: 1, writes: []badgerWrite{
			{key: item.ID, value: buf.Bytes()},
			{key: a.Item.ID, delete: true},
		}}, nil

	default:
		assert.Unreachable(fmt.Sprintf("undefined action kind '0x%X'", int(a.Action)))
	}
	return actionWriteSet{}, nil
}

func getItem(txn *badger.Txn, id []byte) (*types.Item, error) {
	kvItem, err := txn.Get(id)
	if err != nil {
		return nil, err
	}
	item := new(types.Item)
	err = kvItem.Value(func(val []byte) error {
		return gob.NewDecoder(bytes.NewReader(val)).Decode(item)
	})
	return item, err
}

func (b *BadgerPartition) Stats(ctx context.Context, stats *types.PartitionStats, now clock.Time) error {
	db, err := b.getDB()
	if err != nil {
		return err
	}

	untilCancel, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	stopWatching := context.AfterFunc(ctx, func() {
		if errors.Is(ctx.Err(), context.Canceled) {
			cancel()
		}
	})
	defer stopWatching()

	var pivot []byte
	err = db.View(func(txn *badger.Txn) error {
		pivot, err = countPartition(untilCancel, txn, stats, now)
		return err
	})
	if err == nil && errors.Is(ctx.Err(), context.Canceled) {
		err = ctx.Err()
	}
	if err != nil {
		b.leasePivot = nil
		return err
	}
	b.leasePivot = pivot
	return nil
}

func countPartition(ctx context.Context, txn *badger.Txn, stats *types.PartitionStats, now clock.Time) ([]byte, error) {
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()

	pivot := sourcePrefix
	for iter.Rewind(); iter.Valid() && ctx.Err() == nil; iter.Next() {
		key := iter.Item().Key()

		if bytes.HasPrefix(key, []byte("source:")) {
			continue
		}

		var v []byte
		v, err := iter.Item().ValueCopy(v)
		if err != nil {
			return nil, errors.Errorf("during GetByPartition value: %w", err)
		}

		item := new(types.Item)
		if err := gob.NewDecoder(bytes.NewReader(v)).Decode(item); err != nil {
			return nil, errors.Errorf("during Decode(): %w", err)
		}

		if !item.EnqueueAt.IsZero() {
			stats.Scheduled++
			continue
		}

		stats.Total++
		stats.AverageAge += now.Sub(item.CreatedAt)
		if item.IsLeased {
			stats.AverageLeasedAge += item.LeaseDeadline.Sub(now)
			stats.NumLeased++
		} else if bytes.Equal(pivot, sourcePrefix) {
			pivot = iter.Item().KeyCopy(nil)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if stats.Total != 0 {
		stats.AverageAge = clock.Duration(int64(stats.AverageAge) / int64(stats.Total))
	}
	if stats.NumLeased != 0 {
		stats.AverageLeasedAge = clock.Duration(int64(stats.AverageLeasedAge) / int64(stats.NumLeased))
	}
	return pivot, nil
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
	if b.conf.MemTableSize != 0 {
		opts.MemTableSize = b.conf.MemTableSize
		opts.ValueThreshold = min(opts.ValueThreshold, badgerTransactionLimit(opts.MemTableSize))
	}
	db, err := badger.Open(opts)
	if err != nil {
		b.mu.Unlock()
		return nil, errors.Errorf("while opening db '%s': %w", dir, err)
	}
	b.db = db
	b.mu.Unlock()
	return db, nil
}

func badgerTransactionLimit(memTableSize int64) int64 {
	return 15 * memTableSize / 100
}

type BadgerQueues struct {
	QueuesValidation
	mu   sync.Mutex
	db   *badger.DB
	conf BadgerConfig
}

func NewBadgerQueues(conf BadgerConfig) Queues {
	set.Default(conf.Log, slog.Default())
	return &BadgerQueues{
		conf: conf,
	}
}

var _ Queues = &BadgerQueues{}

func (b *BadgerQueues) getDB() (*badger.DB, error) {
	b.mu.Lock()
	if b.db != nil {
		b.mu.Unlock()
		return b.db, nil
	}

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
		queue.Namespace = systemIfEmpty(queue.Namespace)
		return nil
	})
}

func (b *BadgerQueues) Add(_ context.Context, info types.QueueInfo) error {
	if err := b.validateAdd(info); err != nil {
		return err
	}
	info.Namespace = systemIfEmpty(info.Namespace)

	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.Update(func(txn *badger.Txn) error {

		_, err := txn.Get([]byte(info.Name))
		if err == nil {
			return reply.NewInvalidOptionKind(types.KindQueueAlreadyExists, "invalid queue; '%s' already exists", info.Name)
		}

		var buf bytes.Buffer
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

		if err := b.validateTimeoutChange(timeoutChange{Stored: found, Proposed: info}); err != nil {
			return err
		}

		found.Update(info)

		if err := b.validateUpdate(found); err != nil {
			return err
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

func (b *BadgerQueues) List(ctx context.Context, queues *[]types.QueueInfo, opts types.ListOptions) error {
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

		for ; iter.Valid() && ctx.Err() == nil; iter.Next() {
			v, err := iter.Item().ValueCopy(v)
			if err != nil {
				return errors.Errorf("during GetByPartition value(): %w", err)
			}

			var info types.QueueInfo
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&info); err != nil {
				return errors.Errorf("during Decode(): %w", err)
			}
			info.Namespace = systemIfEmpty(info.Namespace)
			if opts.Namespace != "" && info.Namespace != opts.Namespace {
				continue
			}
			if count >= opts.Limit {
				return nil
			}
			*queues = append(*queues, info)
			count++
		}
		if err := ctx.Err(); err != nil {
			return err
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

func (b *BadgerNamespaces) List(ctx context.Context, namespaces *[]types.Namespace, opts types.ListOptions) error {
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

		for ; iter.Valid() && ctx.Err() == nil; iter.Next() {
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
		if err := ctx.Err(); err != nil {
			return err
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
		_, err := txn.Get([]byte("user:" + user.ID))
		if err == nil {
			return types.NewErrUserAlreadyExists(user.ID)
		}
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return errors.Errorf("during Get(): %w", err)
		}

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

		if err := txn.Set([]byte("user-username:"+user.Username), []byte(user.ID)); err != nil {
			return errors.Errorf("during Set(): %w", err)
		}
		return nil
	})
}

func (b *BadgerUsers) List(ctx context.Context, users *[]types.User, opts types.ListOptions) error {
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

		for ; iter.ValidForPrefix(prefix) && ctx.Err() == nil; iter.Next() {
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
		if err := ctx.Err(); err != nil {
			return err
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

		if err := txn.Delete([]byte("user-username:" + user.Username)); err != nil {
			return errors.Errorf("during Delete(): %w", err)
		}

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
		_, err := txn.Get([]byte("apikey:" + key.ID))
		if err == nil {
			return reply.NewInvalidOption("api key already exists")
		}
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return errors.Errorf("during Get(): %w", err)
		}

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

		if err := txn.Set([]byte("apikey-hash:"+key.KeyHash), []byte(key.ID)); err != nil {
			return errors.Errorf("during Set(): %w", err)
		}

		userKeyList := []byte("apikey-user:" + key.UserID)
		existingIDs, err := b.getUserKeyIDs(txn, key.UserID)
		if err != nil {
			return errors.Errorf("during getUserKeyIDs(): %w", err)
		}
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

func (b *BadgerAPIKeys) List(ctx context.Context, keys *[]types.APIKey, opts types.ListOptions) error {
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

		for ; iter.ValidForPrefix(prefix) && ctx.Err() == nil; iter.Next() {
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
		if err := ctx.Err(); err != nil {
			return err
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

		if err := txn.Delete([]byte("apikey-hash:" + key.KeyHash)); err != nil {
			return errors.Errorf("during Delete(): %w", err)
		}

		ids, err := b.getUserKeyIDs(txn, key.UserID)
		if err != nil {
			return errors.Errorf("during getUserKeyIDs(): %w", err)
		}
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
			if err := txn.Delete([]byte("apikey-user:" + key.UserID)); err != nil {
				return errors.Errorf("during Delete(): %w", err)
			}
		}

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

			if err := txn.Delete([]byte("apikey-hash:" + key.KeyHash)); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return errors.Errorf("during Delete(): %w", err)
			}
			if err := txn.Delete([]byte("apikey:" + id)); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return errors.Errorf("during Delete(): %w", err)
			}
		}

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

func (b *BadgerRoles) Get(_ context.Context, roleName RoleName, role *types.Role) error {
	namespace, name := roleName.Namespace, roleName.Name
	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.View(func(txn *badger.Txn) error {
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
		_, err := txn.Get([]byte("role:" + role.ID))
		if err == nil {
			return types.NewErrRoleAlreadyExists(role)
		}
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return errors.Errorf("during Get(): %w", err)
		}

		indexKey := []byte("role-ns-name:" + role.Namespace + ":" + role.Name)
		_, err = txn.Get(indexKey)
		if err == nil {
			return types.NewErrRoleAlreadyExists(role)
		}
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return errors.Errorf("during uniqueness check: %w", err)
		}

		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(role); err != nil {
			return errors.Errorf("during gob.Encode(): %w", err)
		}

		if err := txn.Set([]byte("role:"+role.ID), buf.Bytes()); err != nil {
			return errors.Errorf("during Set(): %w", err)
		}

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

func (b *BadgerRoles) List(ctx context.Context, namespace string, roles *[]types.Role, opts types.ListOptions) error {
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

		for ; iter.ValidForPrefix(prefix) && ctx.Err() == nil; iter.Next() {
			var v []byte
			v, err := iter.Item().ValueCopy(v)
			if err != nil {
				return errors.Errorf("during ValueCopy(): %w", err)
			}

			var role types.Role
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&role); err != nil {
				return errors.Errorf("during Decode(): %w", err)
			}

			if namespace != "" && role.Namespace != namespace {
				continue
			}
			if count >= opts.Limit {
				return nil
			}
			*roles = append(*roles, role)
			count++
		}
		if err := ctx.Err(); err != nil {
			return err
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

		indexKey := []byte("role-ns-name:" + role.Namespace + ":" + role.Name)
		if err := txn.Delete(indexKey); err != nil {
			return errors.Errorf("during Delete(): %w", err)
		}

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
		_, err := txn.Get([]byte("rolebinding:" + binding.ID))
		if err == nil {
			return types.NewErrRoleBindingAlreadyExists(binding)
		}
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return errors.Errorf("during Get(): %w", err)
		}

		uniqueKey := []byte("rolebinding-unique:" + binding.UserID + ":" + binding.Namespace + ":" + binding.RoleID)
		_, err = txn.Get(uniqueKey)
		if err == nil {
			return types.NewErrRoleBindingAlreadyExists(binding)
		}
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return errors.Errorf("during uniqueness check: %w", err)
		}

		userIDs, err := b.getUserBindingIDs(txn, binding.UserID)
		if err != nil {
			return errors.Errorf("during getUserBindingIDs(): %w", err)
		}
		var held []types.RoleBinding
		b.listInNamespace(txn, bindingsInNamespace{IDs: userIDs, Namespace: binding.Namespace}, &held, types.MaxRoleBindingsPerUserInNamespace)
		if len(held) >= types.MaxRoleBindingsPerUserInNamespace {
			return types.NewErrRoleBindingLimitReached(binding)
		}

		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(binding); err != nil {
			return errors.Errorf("during gob.Encode(): %w", err)
		}

		if err := txn.Set([]byte("rolebinding:"+binding.ID), buf.Bytes()); err != nil {
			return errors.Errorf("during Set(): %w", err)
		}

		if err := txn.Set(uniqueKey, []byte(binding.ID)); err != nil {
			return errors.Errorf("during Set(): %w", err)
		}

		userIDs = append(userIDs, binding.ID)
		var userBuf bytes.Buffer
		if err := gob.NewEncoder(&userBuf).Encode(userIDs); err != nil {
			return errors.Errorf("during gob.Encode(): %w", err)
		}
		if err := txn.Set([]byte("rolebinding-user:"+binding.UserID), userBuf.Bytes()); err != nil {
			return errors.Errorf("during Set(): %w", err)
		}

		roleIDs, err := b.getRoleBindingIDs(txn, binding.RoleID)
		if err != nil {
			return errors.Errorf("during getRoleBindingIDs(): %w", err)
		}
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

func (b *BadgerRoleBindings) List(ctx context.Context, namespace string, bindings *[]types.RoleBinding, opts types.ListOptions) error {
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

		for ; iter.ValidForPrefix(prefix) && ctx.Err() == nil; iter.Next() {
			var v []byte
			v, err := iter.Item().ValueCopy(v)
			if err != nil {
				return errors.Errorf("during ValueCopy(): %w", err)
			}

			var binding types.RoleBinding
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&binding); err != nil {
				return errors.Errorf("during Decode(): %w", err)
			}

			if namespace != "" && binding.Namespace != namespace {
				continue
			}
			if count >= opts.Limit {
				return nil
			}
			*bindings = append(*bindings, binding)
			count++
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		return nil
	})
}

func (b *BadgerRoleBindings) ListByUser(_ context.Context, user UserInNamespace, bindings *[]types.RoleBinding, limit int) error {
	userID, namespace := user.UserID, user.Namespace
	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.View(func(txn *badger.Txn) error {
		ids, err := b.getUserBindingIDs(txn, userID)
		if err != nil {
			return errors.Errorf("during getUserBindingIDs(): %w", err)
		}
		if ids == nil {
			return nil
		}

		b.listInNamespace(txn, bindingsInNamespace{IDs: ids, Namespace: namespace}, bindings, limit)
		return nil
	})
}

type bindingsInNamespace struct {
	Namespace string
	IDs       []string
}

func (b *BadgerRoleBindings) listInNamespace(txn *badger.Txn, from bindingsInNamespace, bindings *[]types.RoleBinding, limit int) {
	for _, id := range from.IDs {
		if len(*bindings) >= limit {
			return
		}
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
		if binding.Namespace != from.Namespace {
			continue
		}
		*bindings = append(*bindings, binding)
	}
}

func (b *BadgerRoleBindings) ListByRole(_ context.Context, roleID string, bindings *[]types.RoleBinding, limit int) error {
	db, err := b.getDB()
	if err != nil {
		return err
	}

	return db.View(func(txn *badger.Txn) error {
		ids, err := b.getRoleBindingIDs(txn, roleID)
		if err != nil {
			return errors.Errorf("during getRoleBindingIDs(): %w", err)
		}
		if ids == nil {
			return nil
		}

		for _, id := range ids {
			if limit > 0 && len(*bindings) >= limit {
				break
			}
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

func (b *BadgerRoleBindings) DeleteByUserAndRole(_ context.Context, binding RoleForUser) error {
	namespace, userID, roleID := binding.Namespace, binding.UserID, binding.RoleID
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

		if err := txn.Delete(uniqueKey); err != nil {
			return errors.Errorf("during Delete(): %w", err)
		}

		userIDs, err := b.getUserBindingIDs(txn, userID)
		if err != nil {
			return errors.Errorf("during getUserBindingIDs(): %w", err)
		}
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
			if err := txn.Delete([]byte("rolebinding-user:" + userID)); err != nil {
				return errors.Errorf("during Delete(): %w", err)
			}
		}

		roleIDs, err := b.getRoleBindingIDs(txn, roleID)
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
				return errors.Errorf("during gob.Encode(): %w", err)
			}
			if err := txn.Set([]byte("rolebinding-role:"+roleID), roleBuf.Bytes()); err != nil {
				return errors.Errorf("during Set(): %w", err)
			}
		} else {
			if err := txn.Delete([]byte("rolebinding-role:" + roleID)); err != nil {
				return errors.Errorf("during Delete(): %w", err)
			}
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

		uniqueKey := []byte("rolebinding-unique:" + binding.UserID + ":" + binding.Namespace + ":" + binding.RoleID)
		if err := txn.Delete(uniqueKey); err != nil {
			return errors.Errorf("during Delete(): %w", err)
		}

		userIDs, err := b.getUserBindingIDs(txn, binding.UserID)
		if err != nil {
			return errors.Errorf("during getUserBindingIDs(): %w", err)
		}
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
			if err := txn.Delete([]byte("rolebinding-user:" + binding.UserID)); err != nil {
				return errors.Errorf("during Delete(): %w", err)
			}
		}

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
				return errors.Errorf("during gob.Encode(): %w", err)
			}
			if err := txn.Set([]byte("rolebinding-role:"+binding.RoleID), roleBuf.Bytes()); err != nil {
				return errors.Errorf("during Set(): %w", err)
			}
		} else {
			if err := txn.Delete([]byte("rolebinding-role:" + binding.RoleID)); err != nil {
				return errors.Errorf("during Delete(): %w", err)
			}
		}

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

			uniqueKey := []byte("rolebinding-unique:" + binding.UserID + ":" + binding.Namespace + ":" + binding.RoleID)
			if err := txn.Delete(uniqueKey); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return errors.Errorf("during Delete(): %w", err)
			}

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

			if err := txn.Delete([]byte("rolebinding:" + id)); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return errors.Errorf("during Delete(): %w", err)
			}
		}

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
