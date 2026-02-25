package internal

import (
	"context"
	"sync"
	"time"

	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport/auth"
	"github.com/kapetan-io/tackle/clock"
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
	TTL             time.Duration
}

// AuthCache provides caching for API key lookups to reduce storage load
type AuthCache struct {
	cleanupInterval time.Duration
	byKeyHash       map[string]authCacheEntry
	users           store.Users
	apiKeys         store.APIKeys
	stopCleanup     chan struct{}
	mu              sync.RWMutex
	wg              sync.WaitGroup
	once            sync.Once
	ttl             time.Duration
}

type authCacheEntry struct {
	expiresAt time.Time
	principal auth.Principal
}

// NewAuthCache creates a new auth cache
func NewAuthCache(conf AuthCacheConfig) *AuthCache {
	if conf.TTL == 0 {
		conf.TTL = DefaultCacheTTL
	}
	if conf.CleanupInterval == 0 {
		conf.CleanupInterval = DefaultCleanupInterval
	}

	c := &AuthCache{
		cleanupInterval: conf.CleanupInterval,
		byKeyHash:       make(map[string]authCacheEntry),
		stopCleanup:     make(chan struct{}),
		apiKeys:         conf.APIKeys,
		users:           conf.Users,
		ttl:             conf.TTL,
	}

	go c.cleanupLoop()

	return c
}

// Authenticate validates an API key and returns the principal
func (c *AuthCache) Authenticate(ctx context.Context, key string) (auth.Principal, error) {
	if err := auth.ValidateAPIKeyFormat(key); err != nil {
		return auth.Principal{}, types.ErrAPIKeyInvalid
	}

	keyHash := auth.HashAPIKey(key)

	// Check cache first
	c.mu.RLock()
	entry, ok := c.byKeyHash[keyHash]
	c.mu.RUnlock()

	if ok && clock.Now().UTC().Before(entry.expiresAt) {
		return entry.principal, nil
	}

	// Cache miss or expired - lookup from storage
	var apiKey types.APIKey
	if err := c.apiKeys.GetByHash(ctx, keyHash, &apiKey); err != nil {
		return auth.Principal{}, err
	}

	// Check if key is expired
	if apiKey.ExpiresAt != nil && clock.Now().UTC().After(*apiKey.ExpiresAt) {
		return auth.Principal{}, types.ErrAPIKeyExpired
	}

	// Fetch the user
	var user types.User
	if err := c.users.Get(ctx, apiKey.UserID, &user); err != nil {
		return auth.Principal{}, types.ErrAPIKeyInvalid
	}

	principal := auth.Principal{
		NamespaceScope: apiKey.NamespaceScope,
		UserID:         user.ID,
		Username:       user.Username,
	}

	// Update cache
	c.mu.Lock()
	c.byKeyHash[keyHash] = authCacheEntry{
		expiresAt: clock.Now().UTC().Add(c.ttl),
		principal: principal,
	}
	c.mu.Unlock()

	// Update last used time asynchronously
	select {
	case <-c.stopCleanup:
	default:
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			_ = c.apiKeys.UpdateLastUsed(context.Background(), apiKey.ID, clock.Now().UTC())
		}()
	}

	return principal, nil
}

// Invalidate removes a cached entry by key hash
func (c *AuthCache) Invalidate(keyHash string) {
	c.mu.Lock()
	delete(c.byKeyHash, keyHash)
	c.mu.Unlock()
}

// InvalidateUser removes all cached entries for a user
func (c *AuthCache) InvalidateUser(userID string) {
	c.mu.Lock()
	for hash, entry := range c.byKeyHash {
		if entry.principal.UserID == userID {
			delete(c.byKeyHash, hash)
		}
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
		}
	}
	c.mu.Unlock()
}
