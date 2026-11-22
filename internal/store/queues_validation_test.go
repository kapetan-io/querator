package store_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

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
