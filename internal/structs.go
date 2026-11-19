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
	ActionPass        LifecyclePass
	ScheduledPass     LifecyclePass
	Failures          int
	UnfinishedRuns    int
	ScheduledFailures int
}

func (s *PartitionLifecycleState) RunLifecycleBy(deadline clock.Time) bool {
	if s.ActionPass.Scan.MidPass() {
		s.ActionPass.StartNextPassBy(deadline)
		return false
	}
	if deadline.Before(s.NextLifecycleRun) {
		s.NextLifecycleRun = deadline
		return true
	}
	return false
}

func (s *PartitionLifecycleState) RunScheduledBy(at clock.Time) bool {
	if s.ScheduledPass.Scan.MidPass() {
		s.ScheduledPass.StartNextPassBy(at)
		return false
	}
	if at.Before(s.NextScheduledRun) {
		s.NextScheduledRun = at
		return true
	}
	return false
}

func (s *PartitionLifecycleState) RunDue(now clock.Time) bool {
	return passDue(&s.ActionPass, s.NextLifecycleRun.Sub(now)) || passDue(&s.ScheduledPass, s.NextScheduledRun.Sub(now))
}

func passDue(pass *LifecyclePass, untilRun clock.Duration) bool {
	if pass.Scan.MidPass() {
		return untilRun <= 0
	}
	return untilRun < 0
}

func (s *PartitionLifecycleState) ItemMoved(id types.ItemID) {
	if s.ActionPass.Scan.Unread(id) {
		s.ActionPass.Moved = true
	}
}

func (s *PartitionLifecycleState) RestartPasses(now clock.Time) {
	s.ActionPass.End()
	s.ScheduledPass.End()
	s.NextLifecycleRun = now
	s.NextScheduledRun = now
}

type LifecyclePass struct {
	Scan       types.LifeCycleScan
	NextPassAt clock.Time
	Unfinished bool
	Moved      bool
}

func (p *LifecyclePass) StartNextPassBy(deadline clock.Time) {
	if deadline.IsZero() {
		return
	}
	if p.NextPassAt.IsZero() || deadline.Before(p.NextPassAt) {
		p.NextPassAt = deadline
	}
}

func (p *LifecyclePass) End() {
	p.Scan.EndPass()
	p.NextPassAt = clock.Time{}
	p.Unfinished = false
	p.Moved = false
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
