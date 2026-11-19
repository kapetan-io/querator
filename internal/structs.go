package internal

import (
	"context"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/tackle/clock"
	"log/slog"
)

const (
	LevelDebugAll = slog.LevelDebug
	LevelDebug    = slog.LevelDebug + 1
)

type PartitionLifecycleState struct {
	NextLifecycleRun clock.Time
	NextScheduledRun clock.Time
	Failures         int
	UnfinishedRuns   int
}

type Partition struct {
	ProduceRequests   types.ProduceBatch
	LeaseRequests     types.LeaseBatch
	CompleteRequests  types.CompleteBatch
	RetryRequests     types.RetryBatch
	LifeCycleRequests types.LifeCycleBatch
	Store             store.Partition
	State             types.PartitionState
	Lifecycle         PartitionLifecycleState
	Info              types.PartitionInfo
}

func (p *Partition) Reset() {
	p.ProduceRequests.Reset()
	p.LeaseRequests.Reset()
	p.CompleteRequests.Reset()
	p.RetryRequests.Reset()
	p.LifeCycleRequests.Reset()
}

func (p *Partition) Produce(req *types.ProduceRequest) {
	p.State.UnLeased += len(req.Items)
	p.ProduceRequests.Add(req)
	req.Assigned = true
}

func (p *Partition) Lease(req *types.LeaseRequest) {
	lease := min(p.State.UnLeased, req.NumRequested)
	p.State.NumLeased += lease
	p.State.UnLeased -= lease
	req.Partition = p.Info.PartitionNum
	p.LeaseRequests.Add(req)
}

type Request struct {
	Method  MethodKind
	Context context.Context
	Request any
	ReadyCh readySignal
	Err     error
}

type StorageRequest struct {
	Items     *[]*types.Item
	IDs       []types.ItemID
	Options   types.ListOptions
	Partition int
	ListKind  types.ListKind
}
