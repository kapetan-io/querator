# 8. design goals

Date: 2024-07-04

## Status

Accepted

## Context

Understanding the high level goals of the project is important information for contributors and users alike. This
allows us to focus on what is important and not get side tracked with implementing features which clash with the
primary goals of the project.

## Decision

The primary goal of the project is to implement a highly efficient and simple to integrate with reserve based FIFO
queuing system.

[Querator](https://querator.io) is focused on simplicity and efficiency first. We want to appeal to users who
wish to embrace distributed architectures without needing to run expensive high performance clusters. Although 
our primary goal is to build an efficient reservation based queue, we believe our efficiency goals will also 
lend the project toward a high performance implementation. When deciding to do the efficient thing, or the
fast thing we will always choose efficient.

##### Goals
* Well known wire protocol HTTP/Protobuf/JSON
* Separation of item consumer and producers from underlying storage. This allows storage to scale independently of
  the number of clients and the size and frequency of the items queued.
* Complexity is handled by the server, with very simple client implementations.
* Build a service which encourages good distributed design, and can be used to solve many different distributed
  scale problems.

## Consequences

By choosing to prioritize simplicity and efficiency over high performance and scalability, we may not appeal to 
large enterprise customers with very large pockets who care more about performance than efficiency. We want to 
attract users who are more interested in high efficiency, high interoperability with third party products, and 
easy to use distributed software, rather than bleeding edge costly solutions.
