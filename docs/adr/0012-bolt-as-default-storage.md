# 12. bolt as default storage

Date: 2024-07-11

## Status

Accepted

## Context

In order to facilitate using Querator in environments with limited resources, we want to provide a default built 
in storage option.

## Decision

We started the project using https://github.com/tidwall/buntdb which appeared to be a simple K/V store with only
the features we needed to implement Querator. However, we discovered a few less than desirable issues using BuntDB.

* Bunt uses `string` as it's `value` type. This requires an unnecessary conversion `string(b)` to occur when
  storing anything but UTF-8 strings. Both protbuf and json marshalers use byte arrays and require conversion to
  string before use in BuntDB.
* In order for an index to be applied, `Set()` must ask `IndexJSON()` to parse the marshaled data in order to
  extract the index field. This is sub optimal, and it would have been nice to have provided that index value
  via `SetOptions{SecondaryIndexValue: "foo"}` or some such thing, such that `Set()` doesn't need to unmarshal
  the data I just marshaled.
* We ran into strange situation we couldn't identify. Panic's that occurred  within methods like
  `AscendGreaterOrEqual()` locked up without explanation, and they did not output the panic message to std error.

We are committed to using BoltDB instead of BuntDB for the remainder of the project. It is a well known and
maintained with a feature set similar to BuntDB.

## Consequences

I'm sure we could have overcome some of these issues, researched the code base to understand our problems,
or committed some changes back to the project. However, we want to focus on moving forward with the project
and not working on a key value store implementation. 
