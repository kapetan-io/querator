package internal

import (
	"context"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/tackle/clock"
	"log/slog"
	"sync/atomic"
)

const (
	LevelDebugAll = slog.LevelDebug
	LevelDebug    = slog.LevelDebug + 1
)

// Partition is the in memory representation of the state of a partition.
type Partition struct {
	// ProduceRequests is the batch of produce requests that are assigned to this partition
	ProduceRequests types.Batch[types.ProduceRequest]
	// ReserveRequests is the batch of reserve requests that are assigned to this partition
	ReserveRequests types.ReserveBatch
	// CompleteRequests is the batch of complete requests that are assigned to this partition
	CompleteRequests types.Batch[types.CompleteRequest]
	// LifeCycleRequests is a batch of lifecycle requests that are assigned to this partition
	LifeCycleRequests types.Batch[types.LifeCycleRequest]
	// Store is the storage for this partition
	Store store.Partition
	// Failures is a count of how many times the underlying storage has failed. Resets to
	// zero when storage stops failing. If the value is zero, then the partition is considered
	// active and has communication with the underlying storage.
	Failures atomic.Int32
	// Count is the total number of un-reserved items in the partition
	Count int
	// NumReserved is the total number of items reserved during the most recent distribution
	NumReserved int
	// MostRecentDeadline is the most recent deadline of this partition. This could be
	// the ReserveDeadline, or it could be the ExpireDeadline which ever is sooner. It is
	// used to notify LifeCycle of changes to the partition made by this partition
	// as a hint for when an action might be needed on items in the partition.
	MostRecentDeadline clock.Time
	// LifeCycle is the active life cycle associated with this partition
	LifeCycle *LifeCycle
	// Info is the info associated with this partition
	Info types.PartitionInfo // TODO: Stop using Store.Info().PartitionNum and use this instead
}

func (p *Partition) Reset() {
	p.ProduceRequests.Reset()
	p.ReserveRequests.Reset()
	p.CompleteRequests.Reset()
	p.NumReserved = 0
}

func (p *Partition) Produce(req *types.ProduceRequest) {
	p.Count += len(req.Items)
	p.ProduceRequests.Add(req)
}

func (p *Partition) Reserve(req *types.ReserveRequest) {
	// Use the Number of Requested items OR the count of items in the partition which ever is least.
	reserved := min(p.Count, req.NumRequested)
	// Increment the number of reserved items in this distribution.
	p.NumReserved += reserved
	// Decrement requested from the actual count
	p.Count -= reserved
	// Record which partition this request is assigned, so it can be retrieved by the client later.
	req.Partition = p.Store.Info().PartitionNum
	// Add the request to this partitions reserve batch
	p.ReserveRequests.Add(req)
}

func (q *QueueState) GetPartition(num int) *Partition {
	for _, p := range q.Partitions {
		if p.Info.PartitionNum == num {
			return p
		}
	}
	return nil
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
