# Known Issues

## Auth Cache Invalidation in Multi-Node Deployments

**Severity**: Medium  
**Component**: `internal/auth_cache.go`, `service/service.go`

When an API key is deleted, `APIKeysDelete` calls `auth.InvalidateKey(keyHash)` which removes
the entry from the local in-process auth cache. In a single-node deployment this is sufficient.

In a multi-node deployment, each node maintains its own independent `AuthCache` instance. Key
invalidation is not propagated across nodes. A deleted API key will remain valid on other nodes
until its cache TTL expires (default: 5 minutes).

**Impact**: After deleting an API key, the key holder can continue making authenticated requests
to other cluster nodes for up to one cache TTL period.

**Workaround**: None currently. Operators requiring immediate revocation in a multi-node setup
should restart all nodes, which clears their caches.

**Future mitigation**: A pub/sub invalidation channel (e.g. via the storage backend or a message
bus) could propagate invalidations cluster-wide. This will be considered when multi-node
deployments become a primary use case.

## API Key Prefix Design is Incomplete

**Severity**: Low  
**Component**: `transport/auth/apikey.go`, `internal/types/namespace.go`, `service/service.go`

The current API key format is `<envTag>_<randomPrefix>_<secret>` where:
- `envTag` is passed by the caller at key creation time (defaults to `"qtr"`)
- `randomPrefix` is the first 8 characters of the secret (redundant and leaks entropy)
- `secret` is 64 hex characters (32 random bytes)

There are two problems with this design:

**1. The random middle segment is purposeless.** It is derived from the secret (`secret[:8]`),
so it reveals part of the key to anyone who can read the database. It provides no identification
value beyond what the env tag already provides, and it makes the key format unnecessarily complex.

**2. There is no operator-level default prefix.** The `envTag` is entirely caller-controlled per
request. Operators running multiple Querator instances (e.g. prod, staging) have no way to ensure
keys generated in a given namespace always carry a meaningful prefix without relying on every API
caller to pass the correct value.

**Desired behavior**: The prefix should resolve via a cascade:
1. `req.EnvTag` if provided by the caller
2. `namespace.KeyPrefix` if configured on the namespace the key belongs to
3. `"qtr"` as the global default

**Proposed changes**:
- Add `KeyPrefix string` to the `Namespace` type and its proto definition
- Expose `KeyPrefix` on namespace create/update endpoints
- Update `APIKeysCreate` in `service/service.go` to resolve the prefix via the cascade above
- Drop the random middle segment; new format becomes `<prefix>_<64-char-secret>`
- Update `ValidateAPIKeyFormat` and `GenerateAPIKey` in `transport/auth/apikey.go` accordingly
- Existing keys with the old three-segment format must remain valid during any transition period

**Impact of current state**: Keys always use `"qtr"` as the prefix unless the caller remembers
to pass `EnvTag`. Operators have no namespace-level control over key prefixes.
