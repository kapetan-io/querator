package store

import (
	"context"

	"github.com/dgraph-io/badger/v4"
)

func probeA(ctx context.Context, db *badger.DB) error {
	return db.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()
		n := 0
		for iter.Rewind(); iter.Valid() && ctx.Err() == nil; iter.Next() {
			n++
		}
		return ctx.Err()
	})
}

func probeD(ctx context.Context, iter *badger.Iterator) {
	for iter.Rewind(); iter.Valid() && ctx.Err() == nil; iter.Next() {
		if iter.Item() == nil {
			continue
		}
	}
}
