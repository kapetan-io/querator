# 14. Ordered Storage

Date: 2024-07-26

## Status

Accepted

## Context

Querator is a FIFO queue, as such any data store must obey some basic FIFO rules.

## Decision

Items that are created in the data store are expected to be listed or retrieved in the same order in which they 
are created. In other words it obeys FIFO rules when listing data from the data store. All the functional tests 
depend upon this strict order. Storage implementations that do not obey these rules will not pass the functional 
tests. This restriction also applies when listing queues via `store.QueuesStore`. The functional tests will assume 
this ordering on all future data storage implementations.

## Consequences

Any data store that cannot guarantee insert ordering cannot be used with Querator. Fortunately almost any data 
store which uses a btree for the primary index should be compatible with Querator.
