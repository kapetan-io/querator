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
	NextLifecycleRun  clock.Time
	NextScheduledRun  clock.Time
	Failures          int
	UnfinishedRuns    int
	ScheduledFailures int
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
	estimatedLeases   int
}

func (p *Partition) Reset() {
	p.ProduceRequests.Reset()
	p.LeaseRequests.Reset()
	p.CompleteRequests.Reset()
	p.RetryRequests.Reset()
	p.LifeCycleRequests.Reset()
	p.estimatedLeases = 0
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
	p.estimatedLeases += lease
	req.Partition = p.Info.PartitionNum
	p.LeaseRequests.Add(req)
}

func (p *Partition) SettleLeases() {
	var leased int
	for _, req := range p.LeaseRequests.Requests {
		if req != nil {
			leased += len(req.Items)
		}
	}
	if unsaved := p.estimatedLeases - leased; unsaved > 0 {
		p.State.NumLeased -= unsaved
		p.State.UnLeased += unsaved
	}
	p.estimatedLeases = 0
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
