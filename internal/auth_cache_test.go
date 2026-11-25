package internal_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kapetan-io/querator/internal"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport/auth"
	"github.com/kapetan-io/tackle/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// slowAPIKeys wraps store.APIKeys adding a configurable delay to GetByHash and
// counting calls to UpdateLastUsed. Used to demonstrate the cache-miss fan-out race.
type slowAPIKeys struct {
	store.APIKeys
	getByHashDelay time.Duration
	getByHashErr   error
	updateLastUsed atomic.Int64
	getByHashCalls atomic.Int64
}

func (s *slowAPIKeys) GetByHash(ctx context.Context, hash string, key *types.APIKey) error {
	s.getByHashCalls.Add(1)
	if s.getByHashErr != nil {
		return s.getByHashErr
	}
	time.Sleep(s.getByHashDelay)
	return s.APIKeys.GetByHash(ctx, hash, key)
}

func (s *slowAPIKeys) UpdateLastUsed(ctx context.Context, id string, t clock.Time) error {
	s.updateLastUsed.Add(1)
	return s.APIKeys.UpdateLastUsed(ctx, id, t)
}

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
		for range numGoroutines {
			done.Go(func() {
				ready.Done()
				ready.Wait() // wait for all goroutines to be ready before proceeding
				_, err := cache.Authenticate(context.Background(), generated.Key)
				assert.NoError(t, err)
			})
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

// TestAuthCacheMissUpdateLastUsedFanOut demonstrates that concurrent goroutines racing
// through a cache miss each independently call UpdateLastUsed, resulting in N calls
// instead of 1. The slow GetByHash ensures all goroutines pile up at the miss.
func TestAuthCacheMissUpdateLastUsedFanOut(t *testing.T) {
	const numGoroutines = 20

	memKeys := store.NewMemoryAPIKeys(nil)
	users := store.NewMemoryUsers(nil)

	user := types.User{ID: "user-1", Username: "test-user"}
	require.NoError(t, users.Add(context.Background(), user))

	generated, err := auth.GenerateAPIKey("live")
	require.NoError(t, err)

	require.NoError(t, memKeys.Add(context.Background(), types.APIKey{
		ID:      "key-1",
		UserID:  user.ID,
		KeyHash: generated.KeyHash,
	}))

	slow := &slowAPIKeys{APIKeys: memKeys, getByHashDelay: 20 * time.Millisecond}
	cache := internal.NewAuthCache(internal.AuthCacheConfig{
		APIKeys:         slow,
		Users:           users,
		TTL:             time.Minute,
		CleanupInterval: time.Hour,
	})

	var ready, done sync.WaitGroup
	ready.Add(numGoroutines)
	for range numGoroutines {
		done.Go(func() {
			ready.Done()
			ready.Wait()
			_, err := cache.Authenticate(context.Background(), generated.Key)
			assert.NoError(t, err)
		})
	}
	done.Wait()
	cache.Close()

	// Singleflight collapses all concurrent misses into one storage fetch,
	// so UpdateLastUsed must be called exactly once regardless of goroutine count.
	assert.Equal(t, int64(1), slow.updateLastUsed.Load())
}

func TestSameInvalidAPICallToDB(t *testing.T) {
	memKeys := store.NewMemoryAPIKeys(nil)
	users := store.NewMemoryUsers(nil)

	generated, err := auth.GenerateAPIKey("live")
	require.NoError(t, err)

	invalidKeys := &slowAPIKeys{APIKeys: memKeys}
	cache := internal.NewAuthCache(internal.AuthCacheConfig{
		APIKeys:         invalidKeys,
		Users:           users,
		TTL:             time.Minute,
		CleanupInterval: time.Hour,
	})
	defer cache.Close()

	for range 2 {
		_, err := cache.Authenticate(context.Background(), generated.Key)
		require.ErrorIs(t, err, types.ErrAPIKeyNotExist)

	}

	assert.Equal(t, int64(1), invalidKeys.getByHashCalls.Load())
}

func TestAuthCacheDoesNotCacheStorageErrors(t *testing.T) {
	memKeys := store.NewMemoryAPIKeys(nil)
	users := store.NewMemoryUsers(nil)

	generated, err := auth.GenerateAPIKey("live")
	require.NoError(t, err)

	// Simulate a temporary storage failure to prove it isn't cached.
	slow := &slowAPIKeys{APIKeys: memKeys, getByHashErr: context.DeadlineExceeded}
	cache := internal.NewAuthCache(internal.AuthCacheConfig{
		APIKeys:         slow,
		Users:           users,
		CleanupInterval: time.Hour,
	})
	defer cache.Close()

	for range 2 {
		_, err := cache.Authenticate(context.Background(), generated.Key)
		require.ErrorIs(t, err, types.ErrAPIKeyInvalid)
	}

	assert.Equal(t, int64(2), slow.getByHashCalls.Load())
}

func TestAuthCacheLimitsMissingKeyEntries(t *testing.T) {
	memKeys := store.NewMemoryAPIKeys(nil)
	users := store.NewMemoryUsers(nil)

	first, err := auth.GenerateAPIKey("live")
	require.NoError(t, err)
	second, err := auth.GenerateAPIKey("live")
	require.NoError(t, err)

	slow := &slowAPIKeys{APIKeys: memKeys}
	cache := internal.NewAuthCache(internal.AuthCacheConfig{
		APIKeys:            slow,
		Users:              users,
		MaxNegativeEntries: 1,
	})
	defer cache.Close()

	for _, key := range []string{first.Key, first.Key, second.Key, second.Key} {
		_, err := cache.Authenticate(context.Background(), key)
		require.ErrorIs(t, err, types.ErrAPIKeyNotExist)
	}

	assert.Equal(t, int64(3), slow.getByHashCalls.Load())
}

func TestAuthCacheExpiresMissingKeyEntries(t *testing.T) {
	memKeys := store.NewMemoryAPIKeys(nil)
	users := store.NewMemoryUsers(nil)

	generated, err := auth.GenerateAPIKey("live")
	require.NoError(t, err)

	slow := &slowAPIKeys{APIKeys: memKeys}
	cache := internal.NewAuthCache(internal.AuthCacheConfig{
		APIKeys:            slow,
		Users:              users,
		NegativeTTL:        time.Second,
		MaxNegativeEntries: 1,
		CleanupInterval:    time.Hour,
	})
	defer cache.Close()

	for range 2 {
		_, err := cache.Authenticate(context.Background(), generated.Key)
		require.ErrorIs(t, err, types.ErrAPIKeyNotExist)
	}
	time.Sleep(1100 * time.Millisecond)
	for range 2 {
		_, err := cache.Authenticate(context.Background(), generated.Key)
		require.ErrorIs(t, err, types.ErrAPIKeyNotExist)
	}

	assert.Equal(t, int64(2), slow.getByHashCalls.Load())
}
