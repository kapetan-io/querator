package internal_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kapetan-io/querator/internal"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport/auth"
	"github.com/stretchr/testify/require"
)

// TestAuthCacheCloseRace exercises both goroutine-lifecycle race conditions in AuthCache.
//
// Issue 1 — wg.Add/wg.Wait race:
// Previously, wg.Add(1) happened AFTER the select check, so Close() could run
// close(stopCleanup), see counter=0, and return from wg.Wait() before a racing
// goroutine called wg.Add(1). The fix moves wg.Add(1) before the select, with a
// matching wg.Done() in the stopCleanup case.
//
// Issue 2 — cleanupLoop not tracked in WaitGroup:
// Previously, go cleanupLoop() was started without incrementing the WaitGroup, so
// Close() could return from wg.Wait() before cleanupLoop had actually exited,
// causing goroutine leaks detected by -race and goleak.
//
// Run with: go test -race ./internal/... -run TestAuthCacheCloseRace
func TestAuthCacheCloseRace(t *testing.T) {
	const iterations = 200

	for range iterations {
		apiKeys := store.NewMemoryAPIKeys(nil)
		users := store.NewMemoryUsers(nil)

		user := types.User{
			ID:       "user-1",
			Username: "test-user",
		}
		require.NoError(t, users.Add(context.Background(), user))

		generated, err := auth.GenerateAPIKey("live")
		require.NoError(t, err)

		require.NoError(t, apiKeys.Add(context.Background(), types.APIKey{
			ID:      "key-1",
			UserID:  user.ID,
			KeyHash: generated.KeyHash,
		}))

		cache := internal.NewAuthCache(internal.AuthCacheConfig{
			APIKeys:         apiKeys,
			Users:           users,
			TTL:             time.Minute,
			CleanupInterval: time.Hour,
		})

		// Authenticate from multiple goroutines concurrently.
		// Each cache-miss triggers an UpdateLastUsed goroutine internally (the wg.Add race).
		// We use a barrier so goroutines all start at once, maximizing race probability.
		const numGoroutines = 20
		var ready, done sync.WaitGroup
		ready.Add(numGoroutines)
		done.Add(numGoroutines)
		for range numGoroutines {
			go func() {
				ready.Done()
				ready.Wait() // wait for all goroutines to be ready before proceeding
				defer done.Done()
				_, _ = cache.Authenticate(context.Background(), generated.Key)
			}()
		}

		// Wait for all Authenticate calls to complete before calling Close.
		// This is the correct production usage: Close is called after all
		// Authenticate calls have finished. The race we're testing is between
		// the internal UpdateLastUsed goroutines (spawned inside Authenticate)
		// and Close().
		done.Wait()

		// Close must wait for all in-flight UpdateLastUsed goroutines (Issue 1)
		// and for cleanupLoop to exit (Issue 2).
		cache.Close()

		// Double-close must not panic (protected by sync.Once).
		cache.Close()
	}
}

// TestAuthCacheCleanupLoopTracked verifies that Close() waits for cleanupLoop to
// finish before returning. This directly tests Issue 2: the cleanupLoop goroutine
// must be tracked in the WaitGroup so that Close() does not return prematurely.
func TestAuthCacheCleanupLoopTracked(t *testing.T) {
	// Run many iterations to catch the race reliably with -race.
	const iterations = 500

	for range iterations {
		apiKeys := store.NewMemoryAPIKeys(nil)
		users := store.NewMemoryUsers(nil)

		cache := internal.NewAuthCache(internal.AuthCacheConfig{
			APIKeys:         apiKeys,
			Users:           users,
			TTL:             time.Minute,
			CleanupInterval: time.Hour,
		})

		// Close must not return before cleanupLoop exits.
		// Before the fix, cleanupLoop was not in the WaitGroup, so the goroutine
		// could continue running (and accessing shared state) after Close returned.
		cache.Close()
	}
}
