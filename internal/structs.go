package internal

import (
	"context"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/tackle/clock"
	"sync/atomic"
)

type PartitionDistribution struct {
	// ProduceRequests is the batch of produce requests that are assigned to this partition
	ProduceRequests types.Batch[types.ProduceRequest]
	// ReserveRequests is the batch of reserve requests that are assigned to this partition
	ReserveRequests types.ReserveBatch
	// CompleteRequests is the batch of complete requests that are assigned to this partition
	CompleteRequests types.Batch[types.CompleteRequest]
	// Partition is the partition this distribution is for
	Partition store.Partition
	// Failures is a count of how many times the underlying storage has failed. Resets to
	// zero when storage stops failing. If the value is zero, then the partition is considered
	// active and has communication with the underlying storage.
	Failures atomic.Int32
	// Count is the total number of un-reserved items in the partition
	Count int
	// NumReserved is the total number of items reserved during the most recent distribution
	NumReserved int
	// MostRecentDeadline is the most recent deadline of this distribution. This could be
	// the ReserveDeadline, or it could be the DeadDeadline which ever is sooner. It is
	// used to notify LifeCycle of changes to the partition made by this distribution
	// as a hint for when an action might be needed on items in the partition.
	MostRecentDeadline clock.Time
}

func (p *PartitionDistribution) Reset() {
	p.ProduceRequests.Reset()
	p.ReserveRequests.Reset()
	p.CompleteRequests.Reset()
	p.NumReserved = 0
}

func (p *PartitionDistribution) Produce(req *types.ProduceRequest) {
	p.Count += len(req.Items)
	p.ProduceRequests.Add(req)
}

func (p *PartitionDistribution) Reserve(req *types.ReserveRequest) {
	// Use the Number of Requested items OR the count of items in the partition which ever is least.
	reserved := min(p.Count, req.NumRequested)
	// Increment the number of reserved items in this distribution.
	p.NumReserved += reserved
	// Decrement requested from the actual count
	p.Count -= reserved
	// Record which partition this request is assigned, so it can be retrieved by the client later.
	req.Partition = p.Partition.Info().PartitionNum
	// Add the request to this partitions reserve batch
	p.ReserveRequests.Add(req)
}

type QueueState struct {
	Reservations      types.ReserveBatch
	Producers         types.Batch[types.ProduceRequest]
	Completes         types.Batch[types.CompleteRequest]
	Distributions     []*PartitionDistribution
	LifeCycles        []*LifeCycle
	NextMaintenanceCh <-chan clock.Time
}

type Request struct {
	// The API method called
	Method MethodKind
	// Context is the context of the request
	Context context.Context
	// The request struct for this method
	Request any
	// Used to wait for this request to complete
	ReadyCh chan struct{}
	// The error to be returned to the caller
	Err error
}

type StorageRequest struct {
	// Items is the items returned by the storage request
	Items *[]*types.Item
	// ID is the unique id of the item requested
	IDs []types.ItemID
	// Options used when listing
	Options types.ListOptions
	// Partition is the partition this request is for
	Partition int
}
