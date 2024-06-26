# 5. item_id is not immutable

Date: 2024-06-26

## Status

Accepted

## Context

The desire is to simplify client implementations as much as possible. An unstated goal of the project is to make 
adoption of Querator easy for those interested.

## Decision

The decision was made to simplify the API by allowing querator to generate a new `item_id` everytime it is 
scheduled, deferred, or produced. This allows the `item_id` to reflect where in storage the item exists by
encoding that information into the `item_id`. This allows API to know where the item exists by purely looking
at the `item_id` without the need to consult a lookup table or have the consumer remember this information 
beyond remembering the `item_id` when it wants to defer, or complete an item. 

## Consequences

Querator has no immutable id which allows a user to universally identify an item that was produced.
