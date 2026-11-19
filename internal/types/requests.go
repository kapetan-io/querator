package types

import (
	"context"
	"github.com/kapetan-io/tackle/clock"
)

type ListKind int

const (
	ListItems ListKind = iota
	ListScheduled

	UnSet = -1
)

type LeaseRequest struct {
	NumRequested   int
	RequestTimeout clock.Duration
	ClientID       string
	Context        context.Context

	Items     []*Item
	Partition int

	RequestDeadline clock.Time
	ReadyCh         chan struct{}
	Err             error
}

type ProduceRequest struct {
	RequestTimeout    clock.Duration
	Context           context.Context
	Items             []*Item
	RequestDeadline   clock.Time
	ReadyCh           chan struct{}
	Err               error
	Assigned          bool
	DeadLetterHandOff bool
}

type CompleteRequest struct {
	RequestTimeout  clock.Duration
	Context         context.Context
	Partition       int
	Ids             [][]byte
	RequestDeadline clock.Time
	ReadyCh         chan struct{}
	Err             error
}

type RetryRequest struct {
	RequestTimeout  clock.Duration
	Context         context.Context
	Partition       int
	Items           []RetryItem
	RequestDeadline clock.Time
	ReadyCh         chan struct{}
	Err             error
}

type RetryItem struct {
	ID      []byte
	RetryAt clock.Time
	Dead    bool
}
type ReloadRequest struct {
	Partitions []int
}

type ClearRequest struct {
	Scheduled   bool
	Queue       bool
	Destructive bool
}

type PauseRequest struct {
	Pause bool
}

type ShutdownRequest struct {
	Context context.Context
	ReadyCh chan struct{}
	Err     error
}

type ListOptions struct {
	Pivot     []byte
	Limit     int
	Namespace string
}

type LogicalStats struct {
	ProduceWaiting  int
	LeaseWaiting    int
	CompleteWaiting int
	InFlight        int

	Partitions []PartitionStats
}

type PartitionStats struct {
	Partition        int
	Total            int
	NumLeased        int
	Failures         int
	AverageAge       clock.Duration
	AverageLeasedAge clock.Duration
	Scheduled        int
	UnLeased         int
	NextLifecycleRun clock.Duration
}

type PartitionActionTaker interface {
	TakeAction(ctx context.Context, batch LifeCycleBatch, state *PartitionState) error
}

type LifeCycleRequest struct {
	RequestTimeout   clock.Duration
	Actions          []Action
	PartitionNum     int
	PartitionInfo    PartitionInfo
	PartitionStorage PartitionActionTaker
	PartitionState   *PartitionState
}
