# 4. Audit Trail Vs Client ID

Date: 2024-06-26

## Status

Accepted

## Context

The desire to track the life cycle of an item as it travels through different states. IE: produce, lease, retry, 
dead letter. It would be useful to identify why the item entered each state and when. Originally, I had
decided to include the ClientID of the consumer who leased this item. However, this only provides limited
information about the path the item took through each of the states. In addition, the ClientID gives no clue as
when or why an item entered or exited a particular state, only which client leased, retried or completed the
item.

In addition, Since [5. Item ID is not immutable](0004-item-id-is-not-immutable.md) there is no universal id that
can be used to track the item through its life cycle.

## Decision

We are not pursuing a built-in audit system for Querator at this time. Users who wish such a system will need to
implement their own item tracking outside Querator. They can use the `reference` field or some internal field in
the payload to track the item, and log everytime the item is retried, or leased. If an item finds it's way
into a dead letter queue an external system can consume that item and then report on the audit trail using the
external system.

We reserve the right to implement such an audit trail in the future.

## Consequences

By not implementing a built-in audit system for Querator, users will need to invest additional resources to 
develop and maintain their own item tracking mechanisms. This may lead to increased complexity and potential 
inconsistencies in tracking methods across different users. Furthermore, relying on the reference field or internal
payload fields for tracking will not provide a comprehensive view of the item's life cycle,
(IE: scheduled, dead letter) potentially limiting the ability to identify and analyze the reasons behind state 
transitions. Additionally, the absence of a universal, immutable item ID will continue to hinder the ability to 
accurately track items throughout their life cycle.
