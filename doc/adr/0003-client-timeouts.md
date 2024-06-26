# 3. Client Timeouts

Date: 2024-06-21

## Status

Accepted

## Context

How client timeouts are handled can effect the reliability of items produced and consumed. We recognize that we have
no control over how HTTP clients will behave, but we are making a declaration here of how we expect clients to behave
and the consequences of that behavior.

## Decision

Our decision is that HTTP clients should NOT time out without providing the server with the time at which they
will time out. The rational is that during times of saturation, degradation or catastrophic failure the server should 
always do the right thing to avoid losing an in flight request. In addition, the client should prefer a longer timeout
duration to allow time for the server to handle any in-flight requests during saturation events. If the client decides
to stop waiting for a request without telling the server, it is possible that the request will be fulfilled by the
server at the moment the client decided to time out the request. If instead the client relies upon the server to handle
request timeouts then the server can safely cancel in flight requests once the go routine handling the queue has been
given CPU time and realizes the clients RequestTimeout has elapsed. In this way, the server can avoid reserving, 
producing or completing items for which there is no client. The server can only do this the server has been told by 
the client how long the client will wait for a response from the server.

Consider the example of a server that is experiencing a high saturation event such it is handling many individual 
queues very slowly.
1. Client issues a request to reserve 10 items from the queue with a 5s request timeout.
2. 1 second after the client makes its request, the server kernel notifies the service of the incomming request,
   and the golang HTTP library spawns a go routine to handle the HTTP request. That request then queues the 
   reservation request with `Queue` and waits for a response.
3. 5 seconds later (because the CPU is busy doing something else) the scheduler provides CPU time to the 
   `processQueues()` routine, and an item waiting in the queue is provided to the waiting client channel.
4. The HTTP go routine handling the HTTP request, receives notification from the kernel that the client has closed 
   the remote connection, and thus cancels the context and never receives the item marked for reservation in the queue.

Since the server is under heavy saturation, timing of all operations are slowed, and occur in an order which neither 
the client nor the server can predict. If both the client and server know the client has been waiting longer than 
RequestTimeout the server can do the right thing and avoid reserving an item from the queue, since it knows the 
client has likely already timed out and cancelled the HTTP request.

Clients SHOULD time out AFTER the stated timeout period in order to allow the server time to cancel or complete
the requested operation. IE: If the client wants a 10s timeout, then the client should set RequestTimeout to '10s' 
and then set an internal timeout of '11s' at which time it will stop waiting on the server for a response and cancel
the HTTP request.

In general, our recommendation is that clients SHOULD maintain a longer than expected time out window. Doing so will 
allow the server to continue to operate during degraded or high saturation events.

## Consequences

Clients and networks will not always operate in a manner we expect. Especially in the world of mobile devices and
mobile networks where devices can and will switch networks un-expectedly. In these situations, increasing the timeout
can cause normally short operations to last for quite a long time if connectivity is lost. Indeed, due to the nature of 
TCP, it's possible the server will have no idea the client has gone away and despite sending the reservation to the
TCP stack, the client will not receive the reservation provided. This is one of the reasons EOD is impossible, and we 
instead strive for AEOD (Almost Exactly Once Delivery).

If the timeout of the client is too short, it increases the likelihood of duplicate processing, and if the timeout
is too long, it increases the likelihood of stalled processing during connectivity loss.
