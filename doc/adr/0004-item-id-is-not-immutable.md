# 5. item_id is not immutable

Date: 2024-06-26

## Status

Accepted

## Context

The desire is to simplify client implementations as much as possible. A goal of the project is to make 
adoption of Querator easy for those interested.

## Decision

The decision was made to simplify the API by allowing querator to generate a new `id` everytime it is 
scheduled, deferred, or produced. This allows the `id` to reflect where in storage the item exists by
encoding that information into the `id`. This allows the API to know where the item exists in storage by purely 
looking at the `id` without the need to consult a lookup table or have the consumer remember this information 
beyond remembering the `id` and the name of the queue when it wants to defer, or complete an item.

The client MUST NOT make any assumptions about the format of the `id`, as it could change depending on the storage
implementation used. In addition, assumptions about the format `id` could break in the future.

Note: The `/queue.defer` and `/queue.complete` operations still require the user to provide a queue as a way 
to avoid abusing the API and encouraging simple client design. See [ADR 7 - Encourage one queue per client](0007-encourage-simple-clients)

## Consequences

Querator has no immutable id which allows a user to universally identify an item that was produced.
