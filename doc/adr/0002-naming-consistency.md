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

If you have a type of `time.Duration` it should be named `<thing>Timeout` for example `ReservationTimeout`.
However, if your type is `time.Time` then it should be named `ReservationDeadline`. This is consistent with many
golang libraries like `context.Context`.  For example, `context.WithTimeout(context.Context, time.Duration)`.

## Consequences

Emotional Turmoil, aka a big storm of feelings inside that can be hard to handle.
