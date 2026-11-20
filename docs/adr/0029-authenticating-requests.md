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

1. **Who is this key?** Querator looks the key up, in its cache first and then in storage.
2. **May this user do this here?** Querator reads the user's role bindings in the namespace, such as
   "alice is a Producer in `orders`", and the roles they name.
3. **Record that the key was just used.**

These steps run in front of every request, so a slow or failing step slows or fails the whole API.
Each decision below keeps one of them from doing that.

## Decision

**A permission check reads a bounded number of a user's role bindings in one namespace.** A user with
10,000 bindings cannot make every request they send read all 10,000 from storage.

**Requests that arrive together with the same key share one storage lookup.** Fifty requests with the
same uncached key cause one read, and all fifty wait on it. That read runs on its own timer, not the
first caller's, so when the first caller hangs up the other forty-nine still get an answer.

**Recording when a key was last used never slows a request.** The write goes into a buffer, and when
the buffer is full it is thrown away.

## Consequences

| Decision | What it protects | What it costs |
|---|---|---|
| Read a bounded number of bindings | No request scans storage without limit | A binding past the bound is never read, so a permission granted only by it is denied. ENG-853 refuses a binding past the bound when it is created. |
| One shared lookup per key | Storage is not hit fifty times at once | Nothing |
| Drop last-used writes when busy | A request never waits on bookkeeping | A key's last-used time can be behind |

A key's last-used time is behind by up to the cache time, because a request answered from the cache
records nothing, and by more when a write is thrown away.
