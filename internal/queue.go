package internal

import (
	"context"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport"
	"sync"
)

// TODO: When migrating from one data store to another, some partitions may be
//  set to read only, in this case, it might be necessary to have multiple partitions
//  with the same partition idx.

// Queue is a collection of LogicalQueue and Remote instances which represent the current
// state of the partition distribution from the perspective of the current querator instance.
type Queue struct {
	info    types.QueueInfo
	ordered []*Logical
	logical []*Logical
	mutex   sync.RWMutex
	remote  []Remote
	idx     int
}

func NewQueue(info types.QueueInfo) *Queue {
	q := &Queue{
		logical: make([]*Logical, 0, len(info.PartitionInfo)),
		info:    info,
	}

	for _, info := range info.PartitionInfo {
		q.ordered = append(q.ordered, &Logical{})
	}

	// TODO: Setup ordered
	return q
}

func (q *Queue) GetAll(_ context.Context) []*Logical {
	defer q.mutex.RUnlock()
	q.mutex.RLock()

	var results []*Logical
	_ = copy(q.ordered, results)
	return results
}

func (q *Queue) GetNext(_ context.Context) (Remote, *Logical) {
	defer q.mutex.RUnlock()
	q.mutex.RLock()

	l := q.ordered[q.idx]
	q.idx++
	if q.idx >= len(q.ordered) {
		q.idx = 0
	}
	return nil, l
}

func (q *Queue) GetByPartition(_ context.Context, partition int) (Remote, *Logical, error) {
	defer q.mutex.RUnlock()
	q.mutex.RLock()

	if partition < 0 || partition >= len(q.ordered) {
		return nil, nil, transport.NewInvalidOption("partition is invalid; '%d' is out of bounds", partition)
	}

	return nil, q.logical[partition], nil
}
