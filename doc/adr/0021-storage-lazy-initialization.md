# 21. Storage lazy initialization

Date: 2024-09-23

## Status

Accepted

## Context

Querator must operate effectively in the face of storage failures within a dynamic cloud environment.

## Decision

Querator should operate with limited capacity in the event of underlying storage failure. Since Querator may 
interact with multiple storage systems simultaneously, it should never fail to start, become unresponsive, 
or panic due to storage failures. To achieve this, all storage interactions should be performed using lazy
initialization, and any calls to the storage system should always assume the underlying storage is not
available and handle errors gracefully.

#### Queue Info Storage
If Querator is unable to connect to the queue info storage (where queue information is stored) upon startup,
it will log an error and continue to run. Each API call that requires queue info will attempt to establish
contact with the underlying storage in a lazy initialization manner. To avoid overwhelming the queue info
storage system, Querator will employ the inflight package to control thundering herd issues associated with
a degraded or failed storage system. 

#### Partition Storage
Partitions assigned to a logical queue will also use lazy initialization to ensure that partition assignments
are always successful. However, once the logical queue uses the partition, it may discover that the underlying
partition has failed or that all assigned partitions have failed. The logical queue may decide to take a failed
or degraded partition out of consideration until it recovers. The logical queue should attempt to continue
operations using any partitions that have not failed until the failed partition recover, or the partition
is re-assigned.

## Consequences

If Querator starts in an environment where all underlying storage systems have failed, clients in the
environment may enter an exponential retry loop. This could result in additional traffic and pressure on
the underlying storage as it attempts to recover. Rate limiting and inflight mechanisms may help reduce
this pressure.

Cloud deployment systems rely on health check endpoints to ensure the deployed service is healthy before
continuing a deployment. It may be useful to include a health check endpoint that validates the availability
of each underlying storage system or a subset of them before allowing the deployment to continue. The Querator
health check should include options for selecting which conditions are acceptable to the deployment system:

- requireStorage=ALL - All configured storage systems should be healthy else the health check returns non 200
- requireStorage=Quorum,Quorum=3 - A Quorum of configured storage systems should be healthy else the health
  check returns non 200
- requireStorage=One - Atleast one configured storage system should be healthy
- requireStorage=QueueInfo - Only the queue info storage system should be healthy
