# 19. partition item distribution

Date: 2024-09-11

## Status

Accepted

## Context

The introduction of partitions in ADR [0016-queue-partitions](0016-queue-partitions.md) complicates the distribution of items.

We evaluated two methods for distributing items during a produce operation:

1. **Round-Robin Distribution**: This method involves distributing produce request items in a round-robin 
manner, ensuring that each item is assigned to a partition independently of the produce request. This approach 
aims to achieve an absolutely even distribution of items across all partitions.
2. **Opportunistic Distribution**: This method involves distributing produce request items directly to partitions
without dissociating them from the produce request and assigns produce requests to partitions based on the number
of items currently in the partition.

### 1. Round-Robin Distribution

With round-robin distribution, Querator collects items from the batches of all concurrent produce requests and
then distributes these items evenly across all partitions.

To illustrate this, consider the following scenario: there are three concurrent produce requests queued for
processing by Querator.

- **Produce Request 001**: Produces 50 items
- **Produce Request 002**: Produces 30 items
- **Produce Request 003**: Produces 100 items

In total, there are 180 items queued to be produced. Assuming there are four partitions with no items currently
in the partitions, a round-robin distribution would result in the following allocation:
- **Partition 0**: 45 items
- **Partition 1**: 45 items
- **Partition 2**: 45 items
- **Partition 3**: 45 items

This approach ensures an even distribution of items across the partitions. At first glance, it appears to
be the most efficient method for achieving even distribution. However, when considering failure modes, 
round-robin distribution complicates both the server and client implementations.

#### Partition Failure

Consider the scenario where one of the partitions' underlying storage fails during a write operation, or 
the storage system becomes saturated, causing responses to take longer than the produce request timeout 
provided by the client.

Let's first discuss the partition failure scenario. With round-robin distribution, the item distribution 
for a single produce request might look like this:

- Produce Request produces 30 items
  - 8 items are produced to partition 0
  - 7 items are produced to partition 1
  - 7 items are produced to partition 2
  - 7 items are produced to partition 3

Now, consider the scenario where partition 1 fails, resulting in 7 items that could not be written. We 
have two options to resolve this partial write scenario:

##### Option 1
Temporarily remove partition 1 from consideration and re-run the distribution algorithm for the 7 items,
distributing them to partitions 0, 2, and 3. This option is appealing because it allows the client to remain
unaware of the underlying partition failure and continue normal operation.

##### Option 2
Inform the client which 7 items failed to be produced, so that it can take appropriate action. This option
shifts the complexity of handling partition failure to the client, which would likely need to collect a
list of all the failed items in a response object and retry. Since one of the stated goals of Querator is 
to reduce client complexity by pushing complexity to the service, this option is not desirable.

Since Option 1, is the more appealing option. Let's examine several possible failure modes that 
should be considered when implementing Option 1.

#### Hard Failure of a Partition
Consider the scenario where, upon re-running the distribution algorithm for the 7 unwritten items, the 
remaining partition storage systems also fail or connectivity is disrupted. Because Querator made two
attempts to write items to partitions -— successfully writing 22 of the 30 items in the first attempt and
failing in the second due to the remaining partitions failing -— we find ourselves in a partial write 
scenario with no way to resolve it. The only solution is to inform the client that a partial write occurred,
notifying them of which items failed. This solution results in the same outcome as Option 2, pushing
the complexity of handling failed items back to the client.

#### Degradation of a Partition
Now, instead of a hard failure, consider a saturation or degradation of performance for partition 1. In
this scenario, Querator has written 8 items to partition 0 and is attempting to write 7 items to partition 1.
Due to the degradation of partition 1, the entire produce operation waits for partition 1 to finish writing.
If the write to partition 1 takes longer than the `request_timeout` provided by the client, Querator must
respect the client timeout and cancel the write to partition 1. Again, we have a partial write scenario with
three options:

1. **Rollback all writes**: Roll back all the writes to other partitions. However, it's possible that the
other partitions have failed while waiting for partition 1, making rollback impossible.
2. **Adjust write timeout**: Ensure the write timeout to partition 1 is short enough to allow the write to
fail and complete the re-run distribution to other partitions. However, since we cannot control the timeouts
clients provide and adjusting the minimum timeout requirements based on the number of partitions is not
practical (as clients likely hard-code their timeouts), this approach is not feasible. As the number of 
partitions increases, the likelihood of exceeding the client timeout also increases.
3. **Inform the client**: Inform the client which items failed to be produced. This again pushes the complexity
back to the client.

The following go code illustrates how round-robin may be implemented.

```go
partitions := l.conf.Partitions
var items []*types.Item
var unWritten []*types.Item

for _, req := range state.Producers.Requests {
    // Cancel any produce requests that have timed out
    if l.conf.Clock.Now().UTC().After(req.RequestDeadline) {
        req.Err = ErrRequestTimeout
        state.Producers.Remove(req)
        close(req.ReadyCh)
        continue
    }
    // Assign a DeadTimeout to each item
    for _, item := range req.Items {
        item.DeadDeadline = l.conf.Clock.Now().UTC().Add(l.conf.DeadTimeout)
    }
    items = append(items, req.Items...)
}

for {
    // distributeItems() writes the items to a slice of active partitions and returns:
    //  unWritten  - A slice of items which failed write
    //  partitions - A slice of partitions which did not fail
    unWritten, partitions = l.distributeItems(ctx, partitions, items)
    // unWritten is empty if all items had successful writes to the partitions
    if len(unWritten) == 0 {
        break
    }
    // If none of the partitions had successful writes
    if len(partitions) == 0 || len(unWritten) == len(items) {
        l.conf.Logger.Warn("writes to all partitions failed",
            "queue", l.conf.Name,
            "partitions", spew.Sdump(l.conf.PartitionInfo))
        // We get here if there was an error with the data store, and we should continue to
        // retry until the client's request_timeout is reached
        break
    }
    // re-run the distribution for the remaining items
    items = unWritten

    // What happens if the second attempt fails, now we are in a situation where some of the items
    // are produced, but we don't know which items belong to which producer request!
}
```

### Opportunistic Distribution
With opportunistic distribution, Querator writes all items from a request to a single partition. To achieve a
semi-even distribution, Querator inspects the queued item counts of each partition and opportunistically
chooses the partition with the fewest items to write the request items to.

To illustrate this, consider the following scenario: there are three concurrent produce requests queued for
processing by Querator.

- **Produce Request 001**: Produces 50 items
- **Produce Request 002**: Produces 30 items
- **Produce Request 003**: Produces 100 items

In total, there are 180 items to be produced. Assuming there are four partitions with no items currently in them, the
opportunistic distribution would result in the following allocation:
- **Partition 0**: 50 items
- **Partition 1**: 30 items
- **Partition 2**: 100 items
- **Partition 3**: 0 items

Now, consider three more concurrent produce requests are queued for processing by Querator:
- **Produce Request 001**: Produces 20 items
- **Produce Request 002**: Produces 100 items
- **Produce Request 003**: Produces 100 items

In total, there are 220 additional items to be produced. Opportunistic distribution ensures that the partition with
the least number of items gets priority. Therefore, the distribution should look like this:
- **Partition 3**: 20 items (total: 20)
- **Partition 3**: 100 items (total: 120)
- **Partition 1**: 100 items (total: 130)
- **Partition 0** and **Partition 2** receive no new items in this round.

The resulting partition counts are:
- **Partition 0**: 50 items
- **Partition 1**: 130 items
- **Partition 2**: 100 items
- **Partition 3**: 120 items

Upon the next produce request, **Partition 0** will receive the produce request items because it now holds the
least number of items in the queue, and the process repeats.

#### Partition Failure
Consider the scenario above where one of the partitions' underlying storage fails during a write operation, or
the storage system becomes saturated, causing responses to take longer than the produce request timeout provided
by the client.

Let's first discuss the partition failure scenario. Consider the case where partition 1 has failed, and the 30
items from **Request 002** failed to write to partition 1. 

We have two options to handle the failed partition write:

##### Option 1
Temporarily remove partition 1 from consideration and re-run the distribution algorithm for **Request 002**,
resulting in producing the items on another available partition, such as partition 2, provided that it has not
also failed. If upon re-run, we encounter further partitions that have failed, we continue to re-run the 
distribution algorithm until we find an available partition, or all partitions are either identified as failed, 
or we reach the `request_timeout` deadline. If we are unable to find any available partitions, the request will
continue to wait, and Querator will periodically attempt to write the produce request to a partition until a 
partition is available or the `request_timeout` deadline is reached. A major benefit of Option 1 is that it
avoids the possibility of a partial write. Provided the underlying storage backend is using transactions,
the write to a partition either succeeds are fails in its entirety, which can be retried by Querator
or the client.

Another benefit of Option 1 is that the complexity of the retry is handled by the service, not the client.
A possible downside to Option 1 is that Querator will continue to attempt to find an available partition, 
even if connectivity to Querator from the client has been lost and Querator is not notified by the underlying
TCP session that the client has disconnected. This is possible due to the nature of how TCP operates.

##### Option 2
Immediately return an error to the client, asking the client to retry the entire produce request. The 
advantage of this solution is that the client might be assigned to a different Querator instance that does
not have failed partitions when it retries the produce request. The retry logic for the client is likely
identical to Option 1, as eventually the request will time out and need to be retried.

The downside to Option 2 is that the entire batch of items will need to be re-transmitted for each partition
failure, which can create significant network traffic during a partition failure. If the partition has not 
actually failed but is degraded due to network congestion or saturation, Option 2 could exacerbate the network
saturation. Additionally, Option 2 relies heavily upon the client for reasonable retry and exponential backoff
during what could be an extended partition failure.

#### Degradation of a Partition
Now, instead of a hard failure, consider a degradation of performance for partition 1. In this scenario,
Querator relies on both the `request_timeout` and `LogicalConfig.WriteTimeout` to timeout requests to the backend
storage and client requests. In both timeout scenarios, the logic for the client remains the same, retry the
entire request as there is no partial write which needs to be handled.

Additionally, it might be possible for Querator to detect congestion or degradation of a partition or storage
backend and modify the opportunistic distribution algorithm to account for this degradation by avoiding that 
partition until performance is restored. This way, Querator can maintain availability and minimize any lag 
created by waiting for slow-responding partitions, which might impact other partitions handled by the same
logical queue.


## Decision
Querator will implement opportunistic distribution when producing items to partitions.

Opportunistic distribution offers a more efficient solution compared to round-robin distribution, as it requires
less information about the items and their producers to be tracked by Querator. Additionally, the produced items
in a request can be directly provided to the storage backend to be batched in the most efficient manner for the
backend.

Since the entire produce request batch does not need to be divided for distribution, there is no risk of a
partial write for a single produce request. As a result, the client implementation can remain simple, requiring
only the implementation of retry and backoff mechanisms for proper handling of failed or degraded partitions.


## Consequences
When a client requests an operation via an endpoint, it is Querator's responsibility to fulfill that operation
request until the `request_timeout` is reached. In the case of producing a batch of items, this means Querator
should continue to attempt to find an appropriate partition until the client's `request_timeout` is reached.

However, it is entirely possible that connectivity to the client is lost before Querator can inform the client
that a produce request eventually found an available partition. If connectivity is lost, duplicate item
production will likely result when the client inevitably retries the produce request after timing out.
