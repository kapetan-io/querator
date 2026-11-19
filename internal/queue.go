package internal

import (
	"slices"
	"sync"

	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport/reply"
)

type Queue struct {
	info    types.QueueInfo
	ordered []*Logical
	logical []*Logical
	mutex   sync.RWMutex
	idx     int
}

func NewQueue(info types.QueueInfo) *Queue {
	q := &Queue{
		logical: make([]*Logical, len(info.PartitionInfo)),
		info:    info,
	}
	return q
}

func (q *Queue) AddLogical(ll ...*Logical) *Queue {
	defer q.mutex.Unlock()
	q.mutex.Lock()

	for _, l := range ll {
		for _, p := range l.conf.PartitionInfo {
			q.logical[p.PartitionNum] = l
		}
	}

	for _, add := range ll {
		if !slices.Contains(q.ordered, add) {
			q.ordered = append(q.ordered, add)
		}
	}
	return q
}

func (q *Queue) Info() types.QueueInfo {
	return q.info
}

func (q *Queue) GetAll() []*Logical {
	defer q.mutex.RUnlock()
	q.mutex.RLock()

	results := make([]*Logical, len(q.ordered))
	_ = copy(results, q.ordered)
	return results
}

func (q *Queue) GetNext() *Logical {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if len(q.ordered) == 0 {
		return nil
	}

	l := q.ordered[q.idx]
	q.idx = (q.idx + 1) % len(q.ordered)

	return l
}

func (q *Queue) GetByPartition(partition int) (*Logical, error) {
	defer q.mutex.RUnlock()
	q.mutex.RLock()

	if partition < 0 || partition >= len(q.logical) {
		return nil, reply.NewInvalidOption("partition is invalid; '%d' is not a valid partition", partition)
	}

	if q.logical[partition] == nil {
		return nil, reply.NewInvalidOption("partition is invalid; '%d' has no valid logical queue", partition)
	}

	return q.logical[partition], nil
}
