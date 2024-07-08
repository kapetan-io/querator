# 11. transport structs

Date: 2024-07-08

## Status

Accepted

## Context

We need a consistent way of passing data through all the abstraction layers without introducing new data structs
at every layer of the abstraction. The goal is to reduce the impact of a single request on the garbage collector
by avoiding the need for each abstraction to introduce a new struct tailored to it's layer of abstraction. 

## Decision

`transport.XXXXRequest` structs will be used by all layers of abstraction to efficiently hold data which each 
abstraction will act upon. The `XXXXXRequest` style structs will hold external and internal state about the 
request and it's options. It is also to be used for passing `Response` data back out to the callers. As a result,
the `Request` struct will be passed around as a pointer. 

We should NOT use protobuf structs for passing around data through abstraction layers. This is because it is
impractical to store internal state in a generated protobuf struct, and adding public fields to the protobuf
struct would just confuse users and encourage them to attempt to use those internal fields. Doing this also allows
us to follow the "always abstract your input" rule which gives us freedom to refactor the code without needing
to worry about impacting the public interface and make breaking changes which would necessitate a major version bump.

In some cases, introducing a new struct for an abstraction is unavoidable, but such a decision should not be taken
lightly and the impact of adding more GC overhead be considered carefully.

## Consequences

The implementation of the abstraction layers will be tied to a struct which is a part of the public interface. This
does not mean that implementations cannot add additional fields to the struct which are private to the
implementation. In the following example, `Private` holds a private struct with variables which are not 
part of the public interface and can change depending on the implementation of `Service.Produce()`.

```go
package transport
type ProduceRequest struct {
    // Public Fields HERE
    RequestTimeout time.Duration
    // etc....

    // Private Internal Struct
    Private internal.ServiceProduceState
}
// -------------------
package internal

type ServiceProduceState struct {
    // Place variables which are not a part
    // of the public interface HERE
}
```