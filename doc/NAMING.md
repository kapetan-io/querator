# Naming Guide
Nothing too strict, just some general "labels for what this thing is, and why we call it that"

### Time Related Naming
* A *deadline* is a specific point in time by which an operation must be completed. A deadline is of type `time.Time`
* A *timeout* is a maximum amount of time allowed for an operation to complete. A timeout is of type `time.Duration`
* An *at* is a timestamp when something happened. Timestamps are of type `time.Time` and should always be in UTC.
  Examples are `CreatedAt`, `UpdatedAt`.

If you have a type of `time.Duration` it should be named `<thing>Timeout` for example `LeaseTimeout`.
However, if your type is `time.Time` then it should be named `LeaseDeadline`. This is consistent with many
golang libraries like `context.Context`.  For example, `context.WithTimeout(context.Context, time.Duration)`.

### Items vs Messages
* An *item* typically refers to a single entry or element in a collection or queue. An item can be any type
  of data or object that is stored and processed in the queue.
* A *message* is the basic unit of data that is transmitted between applications or services.

While both "message" and "item" can refer to a single unit of data in a queue, the term "message" often carries
a more specific meaning related to communication and messaging systems, whereas "item" is more generic and can be
used in various queue-based contexts. Since Querator can be used for many different applications beyond "message"
delivery. We prefer to use the term "item" or more specifically "Queue Item" when discussing "items" in the queue
and avoid using the term "message" in code and our API.

One could make the argument that Querator delivers a "message" to a consumer regardless of how it is used. You could
also make the argument that anything in the body of an HTTP request or websocket payload is a "message". But I don't
apply broad terms to such things.

### Options, Config & Arguments
**Options** - Can be used in place of a long list of OPTIONAL method arguments. In the case where there is a common
set or where there are quite a few "Optional" arguments that must be passed to a function or method, you may pass 
around method `Options` by using a Named `Option` struct. In the example below, "name" is required, but all the other
args are "Optional"

```go
type MethodOptions struct {
	Key string
	More int
}

func Method(ctx context.Context, name string, opts MethodOptions) {
    set.Default(&opts.Key, "transdimensional gun")
    set.Default(&opts.More, 42)

	switch name {
	case "rick":
		return Rick(opts)
	case "morty":
		return Morty(opts)
	}
}
```

**Arguments** - A parameters or fields passed to a function or method. Arguments indicate that the "args" are required
or are made up of fields or parameters that are required. If they are all optional, you should instead call them
`Options`. Leaving required fields in the method signature. If there are too many args such that a function or method
needs a second line then consider wrapping all of those arguments into a struct which can be passed into the method.

```go
type MethodArgs struct {
	Name string
	Thing1 int
	Thing2 float64
	Thing3 bool
}

func Method(ctx context.Context, args MethodArgs) {
	switch args.Name {
	case "rick":
		return Rick(args)
	case "morty":
		return Morty(args)
	}
}
```

**Config** - Is a struct which holds configuration used to instantiate a struct. A config is typically static
and is made up of required and optional configuration for the instance. Typically passed in via the `New()` method to
create a new instance of a struct.

```go
type ThingConfig struct {
	Thing1 int
	Thing2 float64
	Thing3 bool
}

type ThingManager struct {
	conf ThingConfig
}

func NewThingManager(conf ThingConfig) *ThingManager {
	return &ThingManager{conf: conf}
}
```


