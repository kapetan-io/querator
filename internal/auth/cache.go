package auth

import (
	"context"
	"sync"
	"time"

	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/tackle/clock"
)

const (
	// DefaultCacheTTL is how long cached entries remain valid
	DefaultCacheTTL = 5 * time.Minute
	// DefaultCleanupInterval is how often to clean up expired entries
	DefaultCleanupInterval = time.Minute
)

// CacheConfig configures the auth cache
type CacheConfig struct {
	CleanupInterval time.Duration
	APIKeys         store.APIKeys
	Users           store.Users
	TTL             time.Duration
}

// Cache provides caching for API key lookups to reduce storage load
type Cache struct {
	cleanupInterval time.Duration
	byKeyHash       map[string]cacheEntry
	users           store.Users
	apiKeys         store.APIKeys
	stopCleanup     chan struct{}
	mu              sync.RWMutex
	wg              sync.WaitGroup
	once            sync.Once
	ttl             time.Duration
}

type cacheEntry struct {
	expiresAt time.Time
	principal types.Principal
}

// NewCache creates a new auth cache
func NewCache(conf CacheConfig) *Cache {
	if conf.TTL == 0 {
		conf.TTL = DefaultCacheTTL
	}
	if conf.CleanupInterval == 0 {
		conf.CleanupInterval = DefaultCleanupInterval
	}

	c := &Cache{
		cleanupInterval: conf.CleanupInterval,
		byKeyHash:       make(map[string]cacheEntry),
		stopCleanup:     make(chan struct{}),
		apiKeys:         conf.APIKeys,
		users:           conf.Users,
		ttl:             conf.TTL,
	}

	go c.cleanupLoop()

	return c
}

// Authenticate validates an API key and returns the principal
func (c *Cache) Authenticate(ctx context.Context, key string) (types.Principal, error) {
	if err := ValidateAPIKeyFormat(key); err != nil {
		return types.Principal{}, types.ErrAPIKeyInvalid
	}

	keyHash := HashAPIKey(key)

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
		return types.Principal{}, err
	}

	// Check if key is expired
	if apiKey.ExpiresAt != nil && clock.Now().UTC().After(*apiKey.ExpiresAt) {
		return types.Principal{}, types.ErrAPIKeyExpired
	}

	// Fetch the user
	var user types.User
	if err := c.users.Get(ctx, apiKey.UserID, &user); err != nil {
		return types.Principal{}, types.ErrAPIKeyInvalid
	}

	principal := types.Principal{
		NamespaceScope: apiKey.NamespaceScope,
		IsAnonymous:    false,
		User:           user,
	}

	// Update cache
	c.mu.Lock()
	c.byKeyHash[keyHash] = cacheEntry{
		expiresAt: clock.Now().UTC().Add(c.ttl),
		principal: principal,
	}
	c.mu.Unlock()

	// Update last used time asynchronously
	select {
	case <-c.stopCleanup:
		// Cache is closed, don't spawn new goroutines
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
func (c *Cache) Invalidate(keyHash string) {
	c.mu.Lock()
	delete(c.byKeyHash, keyHash)
	c.mu.Unlock()
}

// InvalidateUser removes all cached entries for a user
func (c *Cache) InvalidateUser(userID string) {
	c.mu.Lock()
	for hash, entry := range c.byKeyHash {
		if entry.principal.User.ID == userID {
			delete(c.byKeyHash, hash)
		}
	}
	c.mu.Unlock()
}

// Close stops the cleanup goroutine and waits for pending operations
func (c *Cache) Close() {
	c.once.Do(func() {
		close(c.stopCleanup)
		c.wg.Wait()
	})
}

func (c *Cache) cleanupLoop() {
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

func (c *Cache) cleanup() {
	now := clock.Now().UTC()
	c.mu.Lock()
	for hash, entry := range c.byKeyHash {
		if now.After(entry.expiresAt) {
			delete(c.byKeyHash, hash)
		}
	}
	c.mu.Unlock()
}
