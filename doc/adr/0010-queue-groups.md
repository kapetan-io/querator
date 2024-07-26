# 10. Queue Groups

Date: 2024-07-04

## Status

Proposal

## Context

A queue is a single synchronization R/W point for producers and consumers. As such, eventually throughput will suffer
as the bandwidth of items increases. In order to scale, Querator must provide some method of multiplexing production
and reservation of items across queues.

## Decision

We propose the creation of Queue Groups which allow users to combine multiple queues by creating a "Queue Group". 
Then a client can produce/reserve via a "Queue Group", and the "Queue Group" round robins the requests to each queue
in the group. The API for interacting with Queue Groups should be almost identical to the API for interacting with a
single queue. 

## Consequences

The introduction of Queue Groups destroys the ordering we get from a FIFO of queued items. However, for 
some applications ordering is not important to the consumer, but Almost Exactly Once Delivery is still desired and
increased throughput is needed. For such consumers a Queue Group or something similar is needed.
