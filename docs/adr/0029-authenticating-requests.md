# 29. Authenticating requests

Date: 2026-11-19

## Status

Accepted

## Context

Every authenticated request looks up its API key, from a cache or from storage, and checks the caller's
permission against role bindings in storage ([25. Multi-tenancy and
auth](0025-multi-tenancy-and-auth.md)). That work sits in front of every produce and lease, so its cost
and its failures are paid by the whole API.

## Decision

A permission check reads a bounded number of role bindings for each namespace it checks, so a user with
many bindings cannot make one request scan storage without limit.

Concurrent requests that carry the same API key share one storage lookup. The shared lookup runs on its
own context, not on the first caller's, so one caller giving up does not fail every caller waiting on it.

Recording when a key was last used must never slow authentication. An update that finds the buffer of
pending last-used writes full is dropped.

## Consequences

A user's role bindings beyond the bound in one namespace are never examined, so a permission held only
through them is denied.

A key's last-used time can lag its most recent use by up to the cache time, and further when an update is
dropped.
