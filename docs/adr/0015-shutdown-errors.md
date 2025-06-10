# 15. shutdown errors

Date: 2024-07-31

## Status

Proposal

## Context

HTTP servers cannot be closed until all in flight handlers have completed. As the Server.Shutdown()
documentation states.

```
 // Shutdown gracefully shuts down the server without interrupting any
 // active connections. Shutdown works by first closing all open
 // listeners, then closing all idle connections, and then waiting
 // indefinitely for connections to return to idle and then shut down.
```

## Decision

Because the server controls [client timeouts](0009-client-timeouts.md) we must shut down the service before
shut down of the http server. During this shutdown period, clients could receive a `querator.MsgServiceInShutdown`
or `querator.MsgQueueInShutdown` with an HTTP code 453 (Request Failed) from the server. Clients should handle this
message gracefully and in a manner they see fit for their uninterrupted operation. This error should not be considered
a catastrophic error, indeed the client should continue to attempt to re-establish contact with another instance of
Querator or refresh its network topology in an attempt to re-establish contact. The server will not respond with a 454 
(Retry Request) as the service might not be available at this address or port and continued attempts to retry could
be in vain.

## Consequences

The complexity of the client increases to handle a transient situation which occurs when the service is in shutdown.
This also impacts operation of Querator behind a proxy, if an single instance of Querator behind a proxy is in 
shutdown, then the availability of the service is not compromised, then the proxy should likely not return a 453 to 
the client as there are other Querator instances available. However, if Querator is not operated behind a proxy and
instead the client is aware of the Querator topology, then it is upon the client to refresh that topology and attempt
to re-establish contact with a running Querator instance in the cluster. This is especially true if Querator is to 
eventually support streaming, and some sort of Queue Groups where clients wish to connect to the instance which is
the coordinator for queues it's been assigned.

## Items Up for Discussion
- Should we return 453 (Request Failed) with the shutdown message or 454 (Retry Request)
- Should we introduce a new HTTP code for this sort of thing which Proxies could intercept and retry on behalf of
  the client?