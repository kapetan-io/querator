package internal

import (
	"context"
	"errors"
	"log/slog"
	"maps"
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
	DefaultCacheTTL                = 5 * time.Minute
	DefaultNegativeCacheTTL        = 30 * time.Second
	DefaultCleanupInterval         = time.Minute
	DefaultMaxNegativeCacheEntries = 1024

	lastUsedBufferSize = 64
)

type AuthCacheConfig struct {
	CleanupInterval    time.Duration
	APIKeys            store.APIKeys
	Users              store.Users
	Log                *slog.Logger
	TTL                time.Duration
	NegativeTTL        time.Duration
	MaxNegativeEntries int
}

type AuthCache struct {
	cleanupInterval    time.Duration
	byKeyHash          map[string]authCacheEntry
	byUserID           map[string]map[string]struct{}
	lastUsedCh         lastUsedUpdates
	users              store.Users
	apiKeys            store.APIKeys
	log                *slog.Logger
	stop               stopSignal
	sf                 singleflight.Group
	mu                 sync.RWMutex
	wg                 sync.WaitGroup
	once               sync.Once
	ttl                time.Duration
	negativeTTL        time.Duration
	maxNegativeEntries int
	negativeEntries    int
}

type lastUsedUpdate struct {
	keyID string
	at    time.Time
}

type authCacheEntry struct {
	keyExpiresAt *time.Time
	expiresAt    time.Time
	principal    auth.Principal
	notFound     bool
}

func NewAuthCache(conf AuthCacheConfig) *AuthCache {
	set.Default(&conf.Log, slog.Default())
	set.Default(&conf.TTL, DefaultCacheTTL)
	set.Default(&conf.CleanupInterval, DefaultCleanupInterval)
	set.Default(&conf.NegativeTTL, DefaultNegativeCacheTTL)
	set.Default(&conf.MaxNegativeEntries, DefaultMaxNegativeCacheEntries)

	c := &AuthCache{
		cleanupInterval:    conf.CleanupInterval,
		byKeyHash:          make(map[string]authCacheEntry),
		byUserID:           make(map[string]map[string]struct{}),
		lastUsedCh:         make(lastUsedUpdates, lastUsedBufferSize),
		stop:               make(stopSignal),
		apiKeys:            conf.APIKeys,
		users:              conf.Users,
		log:                conf.Log,
		ttl:                conf.TTL,
		negativeTTL:        conf.NegativeTTL,
		maxNegativeEntries: conf.MaxNegativeEntries,
	}

	c.wg.Go(c.cleanupLoop)
	c.wg.Go(c.lastUsedLoop)

	return c
}

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
		if entry.notFound {
			return auth.Principal{}, types.ErrAPIKeyNotExist
		}
		return entry.principal, nil
	}

	v, err, _ := c.sf.Do(keyHash, func() (any, error) {
		sfCtx, sfCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer sfCancel()

		var apiKey types.APIKey
		if err := c.apiKeys.GetByHash(sfCtx, keyHash, &apiKey); err != nil {
			if !errors.Is(err, types.ErrAPIKeyNotExist) {
				c.log.Warn("storage error during api key lookup; returning 401 to caller",
					"error", err)
				return nil, types.ErrAPIKeyInvalid
			}
			c.Invalidate(keyHash)
			c.mu.Lock()
			if c.negativeEntries < c.maxNegativeEntries {
				c.byKeyHash[keyHash] = authCacheEntry{
					expiresAt: clock.Now().UTC().Add(c.negativeTTL),
					notFound:  true,
				}
				c.negativeEntries++
			}
			c.mu.Unlock()
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

		c.Invalidate(keyHash)
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

func (c *AuthCache) Invalidate(keyHash string) {
	c.mu.Lock()
	if entry, ok := c.byKeyHash[keyHash]; ok {
		delete(c.byKeyHash, keyHash)
		if entry.notFound {
			c.negativeEntries--
		}
		if hashes, ok := c.byUserID[entry.principal.UserID]; ok {
			delete(hashes, keyHash)
			if len(hashes) == 0 {
				delete(c.byUserID, entry.principal.UserID)
			}
		}
	}
	c.mu.Unlock()
}

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

func (c *AuthCache) Close() {
	c.once.Do(func() { close(c.stop) })
	c.wg.Wait()
}

func (c *AuthCache) cleanupLoop() {
	ticker := time.NewTicker(c.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.cleanup()
		case <-c.stop:
			return
		}
	}
}

func (c *AuthCache) cleanup() {
	now := clock.Now().UTC()
	c.mu.Lock()
	maps.DeleteFunc(c.byKeyHash, func(hash string, entry authCacheEntry) bool {
		if !now.After(entry.expiresAt) {
			return false
		}
		if entry.notFound {
			c.negativeEntries--
		}
		if hashes, ok := c.byUserID[entry.principal.UserID]; ok {
			delete(hashes, hash)
			if len(hashes) == 0 {
				delete(c.byUserID, entry.principal.UserID)
			}
		}
		return true
	})
	c.mu.Unlock()
}

func (c *AuthCache) lastUsedLoop() {
	for {
		select {
		case update := <-c.lastUsedCh:
			c.recordLastUsed(update)
		case <-c.stop:
			for {
				select {
				case update := <-c.lastUsedCh:
					c.recordLastUsed(update)
				default:
					return
				}
			}
		}
	}
}

func (c *AuthCache) recordLastUsed(update lastUsedUpdate) {
	if err := c.apiKeys.UpdateLastUsed(context.Background(), update.keyID, update.at); err != nil {
		c.log.Warn("failed to record api key last used time", "key_id", update.keyID, "error", err)
	}
}
