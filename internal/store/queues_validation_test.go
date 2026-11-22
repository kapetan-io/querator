package store_test

import (
	"bytes"
	"context"
	"encoding/gob"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The service parsers refuse a negative timeout before it reaches the store, so only a direct
// call can show that the store itself holds the rule.
func TestQueuesRefuseNegativeTimeoutOnUpdate(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []struct {
		Name string
		New  func(t *testing.T) store.Queues
	}{
		{"InMemory", func(t *testing.T) store.Queues { return store.NewMemoryQueues(slog.Default()) }},
		{"BadgerDB", func(t *testing.T) store.Queues {
			return store.NewBadgerQueues(store.BadgerConfig{StorageDir: t.TempDir(), Log: slog.Default()})
		}},
	} {
		t.Run(backend.Name, func(t *testing.T) {
			for _, test := range []struct {
				Name    string
				Update  types.QueueInfo
				Message string
			}{
				{"NegativeLease", types.QueueInfo{LeaseTimeout: -time.Minute}, "lease timeout is invalid; cannot be negative"},
				{"NegativeExpire", types.QueueInfo{ExpireTimeout: -time.Minute}, "expire timeout is invalid; cannot be negative"},
				{"BothNegative", types.QueueInfo{LeaseTimeout: -2 * time.Minute, ExpireTimeout: -time.Minute}, "lease timeout is invalid; cannot be negative"},
			} {
				t.Run(test.Name, func(t *testing.T) {
					queues := backend.New(t)
					require.NoError(t, queues.Add(ctx, types.QueueInfo{
						Name:                "queue",
						LeaseTimeout:        time.Minute,
						ExpireTimeout:       10 * time.Minute,
						RequestedPartitions: 1,
					}))

					update := test.Update
					update.Name = "queue"
					require.ErrorContains(t, queues.Update(ctx, update), test.Message)

					var stored types.QueueInfo
					require.NoError(t, queues.Get(ctx, "queue", &stored))
					assert.Equal(t, time.Minute, stored.LeaseTimeout)
					assert.Equal(t, 10*time.Minute, stored.ExpireTimeout)
				})
			}
		})
	}
}

// A queue stored before negative timeouts were refused must stay updatable. The queues manager
// hands the store the merged record, so the stored negative arrives as if it were new. Only
// BadgerDB keeps records across versions, so it is the one backend here that can hold such a record.
func TestQueuesUpdateOnStoredNegativeLease(t *testing.T) {
	ctx := context.Background()

	seed := func(t *testing.T) store.Queues {
		dir := t.TempDir()
		db, err := badger.Open(badger.DefaultOptions(filepath.Join(dir, "~queue-storage-partition")).WithLogger(nil))
		require.NoError(t, err)
		var buf bytes.Buffer
		require.NoError(t, gob.NewEncoder(&buf).Encode(types.QueueInfo{
			Name:                "queue",
			LeaseTimeout:        -5 * time.Minute,
			ExpireTimeout:       10 * time.Minute,
			RequestedPartitions: 1,
		}))
		require.NoError(t, db.Update(func(txn *badger.Txn) error { return txn.Set([]byte("queue"), buf.Bytes()) }))
		require.NoError(t, db.Close())
		return store.NewBadgerQueues(store.BadgerConfig{StorageDir: dir, Log: slog.Default()})
	}
	update := func(t *testing.T, queues store.Queues, change types.QueueUpdate) error {
		var stored types.QueueInfo
		require.NoError(t, queues.Get(ctx, "queue", &stored))
		return queues.Update(ctx, change.ApplyTo(stored))
	}

	t.Run("ReferenceOnly", func(t *testing.T) {
		queues := seed(t)
		require.NoError(t, update(t, queues, types.QueueUpdate{Name: "queue", Reference: new("owner")}))

		var stored types.QueueInfo
		require.NoError(t, queues.Get(ctx, "queue", &stored))
		assert.Equal(t, "owner", stored.Reference)
		assert.Equal(t, -5*time.Minute, stored.LeaseTimeout)
	})

	t.Run("PositiveLeaseRepairs", func(t *testing.T) {
		queues := seed(t)
		require.NoError(t, update(t, queues, types.QueueUpdate{Name: "queue", LeaseTimeout: 2 * time.Minute}))

		var stored types.QueueInfo
		require.NoError(t, queues.Get(ctx, "queue", &stored))
		assert.Equal(t, 2*time.Minute, stored.LeaseTimeout)
	})

	t.Run("DifferentNegativeRefused", func(t *testing.T) {
		queues := seed(t)
		require.ErrorContains(t, update(t, queues, types.QueueUpdate{Name: "queue", LeaseTimeout: -6 * time.Minute}),
			"lease timeout is invalid; cannot be negative")

		var stored types.QueueInfo
		require.NoError(t, queues.Get(ctx, "queue", &stored))
		assert.Equal(t, -5*time.Minute, stored.LeaseTimeout)
	})
}
