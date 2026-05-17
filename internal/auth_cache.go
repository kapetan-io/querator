package internal

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport/auth"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/set"
	"golang.org/x/sync/singleflight"
)

const (
	// DefaultCacheTTL is how long cached entries remain valid
	DefaultCacheTTL = 5 * time.Minute
	// DefaultCleanupInterval is how often to clean up expired entries
	DefaultCleanupInterval = time.Minute
)

// AuthCacheConfig configures the auth cache
type AuthCacheConfig struct {
	CleanupInterval time.Duration
	APIKeys         store.APIKeys
	Users           store.Users
	Log             *slog.Logger
	TTL             time.Duration
}

// AuthCache provides caching for API key lookups to reduce storage load
type AuthCache struct {
	cleanupInterval time.Duration
	byKeyHash       map[string]authCacheEntry
	byUserID        map[string]map[string]struct{}
	lastUsedCh      chan lastUsedUpdate
	users           store.Users
	apiKeys         store.APIKeys
	log             *slog.Logger
	stopCleanup     chan struct{}
	sf              singleflight.Group
	mu              sync.RWMutex
	wg              sync.WaitGroup
	once            sync.Once
	ttl             time.Duration
}

type lastUsedUpdate struct {
	keyID string
	at    time.Time
}

type authCacheEntry struct {
	keyExpiresAt *time.Time
	expiresAt    time.Time
	principal    auth.Principal
}

// NewAuthCache creates a new auth cache
func NewAuthCache(conf AuthCacheConfig) *AuthCache {
	set.Default(&conf.Log, slog.Default())
	set.Default(&conf.TTL, DefaultCacheTTL)
	set.Default(&conf.CleanupInterval, DefaultCleanupInterval)

	c := &AuthCache{
		cleanupInterval: conf.CleanupInterval,
		byKeyHash:       make(map[string]authCacheEntry),
		byUserID:        make(map[string]map[string]struct{}),
		lastUsedCh:      make(chan lastUsedUpdate, 64),
		stopCleanup:     make(chan struct{}),
		apiKeys:         conf.APIKeys,
		users:           conf.Users,
		log:             conf.Log,
		ttl:             conf.TTL,
	}

	c.wg.Add(2)
	go c.cleanupLoop()
	go c.lastUsedLoop()

	return c
}

// Authenticate validates an API key and returns the principal
func (c *AuthCache) Authenticate(ctx context.Context, key string) (auth.Principal, error) {
	if err := auth.ValidateAPIKeyFormat(key); err != nil {
		return auth.Principal{}, types.ErrAPIKeyInvalid
	}

	keyHash := auth.HashAPIKey(key)

	c.mu.RLock()
	entry, ok := c.byKeyHash[keyHash]
	c.mu.RUnlock()

	if ok && clock.Now().UTC().Before(entry.expiresAt) {
		if entry.keyExpiresAt != nil && clock.Now().UTC().After(*entry.keyExpiresAt) {
			c.mu.Lock()
			if current, exists := c.byKeyHash[keyHash]; exists && current.keyExpiresAt != nil &&
				clock.Now().UTC().After(*current.keyExpiresAt) {
				delete(c.byKeyHash, keyHash)
			}
			c.mu.Unlock()
			return auth.Principal{}, types.ErrAPIKeyExpired
		}
		return entry.principal, nil
	}

	v, err, _ := c.sf.Do(keyHash, func() (any, error) {
		// Use a detached context with a bounded timeout so that a single caller's
		// context cancellation does not propagate to all callers sharing this flight.
		sfCtx, sfCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer sfCancel()

		var apiKey types.APIKey
		if err := c.apiKeys.GetByHash(sfCtx, keyHash, &apiKey); err != nil {
			return nil, err
		}

		if apiKey.ExpiresAt != nil && clock.Now().UTC().After(*apiKey.ExpiresAt) {
			return nil, types.ErrAPIKeyExpired
		}

		var user types.User
		if err := c.users.Get(sfCtx, apiKey.UserID, &user); err != nil {
			c.log.Warn("storage error during api key user lookup; returning 401 to caller",
				"user_id", apiKey.UserID, "error", err)
			return nil, types.ErrAPIKeyInvalid
		}

		principal := auth.Principal{
			NamespaceScope: apiKey.NamespaceScope,
			UserID:         user.ID,
			Username:       user.Username,
		}

		c.mu.Lock()
		c.byKeyHash[keyHash] = authCacheEntry{
			keyExpiresAt: apiKey.ExpiresAt,
			expiresAt:    clock.Now().UTC().Add(c.ttl),
			principal:    principal,
		}
		if c.byUserID[user.ID] == nil {
			c.byUserID[user.ID] = make(map[string]struct{})
		}
		c.byUserID[user.ID][keyHash] = struct{}{}
		c.mu.Unlock()

		select {
		case c.lastUsedCh <- lastUsedUpdate{keyID: apiKey.ID, at: clock.Now().UTC()}:
		default:
		}

		return principal, nil
	})
	if err != nil {
		return auth.Principal{}, err
	}
	return v.(auth.Principal), nil
}

// Invalidate removes a cached entry by key hash
func (c *AuthCache) Invalidate(keyHash string) {
	c.mu.Lock()
	if entry, ok := c.byKeyHash[keyHash]; ok {
		delete(c.byKeyHash, keyHash)
		if hashes, ok := c.byUserID[entry.principal.UserID]; ok {
			delete(hashes, keyHash)
			if len(hashes) == 0 {
				delete(c.byUserID, entry.principal.UserID)
			}
		}
	}
	c.mu.Unlock()
}

// InvalidateUser removes all cached entries for a user
func (c *AuthCache) InvalidateUser(userID string) {
	c.mu.Lock()
	if hashes, ok := c.byUserID[userID]; ok {
		for hash := range hashes {
			delete(c.byKeyHash, hash)
		}
		delete(c.byUserID, userID)
	}
	c.mu.Unlock()
}

// Close stops the cleanup goroutine and waits for pending operations
func (c *AuthCache) Close() {
	c.once.Do(func() {
		close(c.stopCleanup)
		c.wg.Wait()
	})
}

func (c *AuthCache) cleanupLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.cleanup()
		case <-c.stopCleanup:
			return
		}
	}
}

func (c *AuthCache) cleanup() {
	now := clock.Now().UTC()
	c.mu.Lock()
	for hash, entry := range c.byKeyHash {
		if now.After(entry.expiresAt) {
			delete(c.byKeyHash, hash)
			if hashes, ok := c.byUserID[entry.principal.UserID]; ok {
				delete(hashes, hash)
				if len(hashes) == 0 {
					delete(c.byUserID, entry.principal.UserID)
				}
			}
		}
	}
	c.mu.Unlock()
}

func (c *AuthCache) lastUsedLoop() {
	defer c.wg.Done()
	for {
		select {
		case update := <-c.lastUsedCh:
			_ = c.apiKeys.UpdateLastUsed(context.Background(), update.keyID, update.at)
		case <-c.stopCleanup:
			// Drain remaining updates before exiting
			for {
				select {
				case update := <-c.lastUsedCh:
					_ = c.apiKeys.UpdateLastUsed(context.Background(), update.keyID, update.at)
				default:
					return
				}
			}
		}
	}
}
