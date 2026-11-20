# 29. Authenticating requests

Date: 2026-11-19

## Status

Accepted

## Context

Every produce and lease request checks its API key before it does any work
([25. Multi-tenancy and auth](0025-multi-tenancy-and-auth.md)):

```
POST /v1/queue.produce
Authorization: Bearer qtr-live-abc123...
```

1. **Who is this key?** Querator looks the key up in its cache. On a miss it reads the key and then
   its user from storage, and records that the key was just used.
2. **May this user do this here?** Querator reads the user's role bindings in the namespace, such as
   "alice is a Producer in `orders`", and the roles they name.

Both steps run in front of every request, so a slow or failing step slows or fails the whole API.
Each decision below keeps one of them from doing that.

## Decision

**A permission check reads a bounded number of a user's role bindings in one namespace.** A user with
10,000 bindings cannot make every request they send read all 10,000 from storage.

**Creating a role binding past that bound is refused.** A user can hold at most 100 role bindings in one
namespace, the same number a permission check reads, so every binding the API accepts is one a check
reads. The store counts the user's bindings in the namespace inside the same write that adds the new
one, so two creates at once cannot both land the 101st. The Open Door binding goes through the same
store write. A start that finds the anonymous user holding 100 other bindings in `_system`, and no
Open Door binding, logs a warning and starts without granting it. Failing the start instead would
leave the operator no way to free a slot, since bindings are removed through the API of a running
server. Skipping the grant withholds Admin, but the anonymous user keeps the permissions its other
`_system` bindings grant, and a permission check in any namespace falls back to `_system` bindings.

**Requests that arrive together with the same key share one storage lookup.** Fifty requests with the
same uncached key cause one lookup, which reads the key and its user once, and all fifty wait on it.
That lookup runs on its own timer, not the first caller's, so when the first caller hangs up the other
forty-nine still get an answer.

**Recording when a key was last used never slows a request.** The write goes into a buffer, and when
the buffer is full it is thrown away.

## Consequences

| Decision | What it protects | What it costs |
|---|---|---|
| Read a bounded number of bindings | No request scans storage without limit | A binding past the bound is never read, so a permission granted only by it is denied. A user can still pass the bound if a binding is unreadable when a create counts, or if the bindings were written by a Querator that did not refuse creates past it |
| Refuse a binding past the bound | An admin cannot grant a permission a check will ignore | A user who needs more than 100 grants in one namespace must be bound to broader roles. Counting can read every binding the user holds in every namespace on each create |
| One shared lookup per key | Storage is not hit fifty times at once | A caller waits up to the lookup's own 10-second limit, whatever its deadline, and one storage error answers every waiting caller with an invalid-key error |
| Drop last-used writes when busy | A request never waits on bookkeeping | A key's last-used time can be behind |

A key's last-used time is behind by up to the cache time, because a request answered from the cache
records nothing, and by more when a write is thrown away.
