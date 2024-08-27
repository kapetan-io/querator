# 17. cluster operation

Date: 2024-08-26

## Status

Proposal

## Context

Querator needs to distribute logical queues and partitions across multiple instances within a cluster.
This distribution enables Querator to spread read and write workloads across the infrastructure,
thereby reducing single points of failure, increasing throughput, and optimizing hardware utilization.

## Decision

#### Leader Election
Querator will implement a leader election system to designate a cluster leader. The leader's
responsibilities include:
- Evenly distributing read/write loads across the cluster by assigning Logical Queues and partitions
to Querator instances.
- Communicating with each Querator instance in the cluster to inform them of their assigned Logical 
Queues and partitions.

This approach offers several advantages:
- The leader can maintain a comprehensive view of the cluster, enabling efficient resource allocation
and client management.
- In the event of leadership loss, the existing cluster configuration remains intact, allowing available
instances to continue serving clients until a new leader is elected.
- The leader can collect detailed usage and performance metrics from each instance to optimize logical
queue and partition placements.
- The leader can provide connection recommendations to clients for optimal utilization of available
Querator instances.

#### Leader Election Considerations
###### Split Brain
The leader may become isolated from the rest of the cluster during disruptive events (e.g., redeployment),
potentially rendering many partitions unavailable until a new leader is elected and the cluster
stabilizes. 

###### Slow Leader
The leader may become overwhelmed and unable to fulfill its responsibilities efficiently. Implement
self-monitoring for the leader to detect performance degradation and voluntarily step down if necessary.
If not possible, then the result is that the cluster might not be operating at peak efficiency, or 
some partitions are not being consumed or produced too.

###### Leader loses connectivity to a member
The leader may lose connectivity to a cluster member, impacting its ability to manage assignments for
that instance. We can mitigate this by designing the assignment algorithm to prioritize safety over
immediate efficiency. The algorithm should Remove partition assignments from instances before making 
new assignments. This ensures that only one Querator instance is consuming or producing to a partition
at any given time. Doing so, means we accept that some partitions may become temporarily unassigned
during leadership transitions or member connectivity issues.

### Implementation
Querator will implement
- An Evaluation loop which evaluates the state of the cluster and changes the cluster logical queue
and partition assignment configuration.
- A Reconciliation Loop which is constantly attempting to bring the cluster into line with the logical 
queue and partition assignment configuration.

## Consequences
The reliance upon a leader means the reliability and efficiency of the cluster is highly reliant upon
a robust leader election algorithm. Leader election algorithms also impact the geographical placements
and the maximum size of a cluster. With such a design we accept that a cluster is expected to be located 
in a geographically similar location, which is similar in consideration to k8s clusters. Cluster sizes
of 10k instances or more are likely not practical when using a leader election style system.

