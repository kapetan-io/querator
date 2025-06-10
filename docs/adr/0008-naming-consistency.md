# 2. Naming Consistency

Date: 2024-06-21

## Status

Accepted

## Context

Naming consistency in a software project is crucial because it enhances readability, maintainability, and collaboration.
Consistent naming conventions ensure that all team members understand the codebase, reducing confusion and errors.
It also facilitates code reviews, debugging, and updates, as well as makes it easier for new team members to onboard.
Additionally, consistent naming helps in creating a unified and professional codebase, which is essential for
large-scale projects.

## Decision

#### Time Related Naming
* A *deadline* is a specific point in time by which an operation must be completed. A deadline is of type `time.Time`
* A *timeout* is a maximum amount of time allowed for an operation to complete. A timeout if of type `time.Duration`
* An *at* is a timestamp when something happened. Timestamps are of type `time.Time` and should always be in UTC. 
  Examples are `CreatedAt`, `UpdatedAt`.

If you have a type of `time.Duration` it should be named `<thing>Timeout` for example `LeaseTimeout`.
However, if your type is `time.Time` then it should be named `LeaseDeadline`. This is consistent with many
golang libraries like `context.Context`.  For example, `context.WithTimeout(context.Context, time.Duration)`.

#### Items vs Messages
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
apply broad terms to such things. I may yet live to regret this, but, Querator manages "items" in a queue, not 
"messages". I have spoken. `<insert mandalorian meme here>`

## Consequences

Emotional Turmoil, aka a big storm of feelings inside that can be hard to handle.
