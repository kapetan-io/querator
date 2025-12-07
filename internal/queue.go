package internal

import (
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
	// remote  []Remote
	idx int
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

	// Adds a pointer to q.logical which is a list of logical pointers
	// corresponding to the partition index.
	// return q.logical[10] // returns the logical that owns partition 10
	for _, l := range ll {
		for _, p := range l.conf.PartitionInfo {
			q.logical[p.PartitionNum] = l
		}
	}

	// Adds a pointer to q.ordered which is a list of logical pointers
	// ordered by partition such that we can easily iterate through all
	// the logical queues this queue has.
skip:
	for _, add := range ll {
		for _, l := range q.ordered {
			// If this logical already exists, skip
			if l == add {
				break skip
			}
		}
		q.ordered = append(q.ordered, add)
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

func (q *Queue) GetNext() (Remote, *Logical) {
	defer q.mutex.Unlock()
	q.mutex.Lock()

	l := q.ordered[q.idx]
	q.idx++
	if q.idx >= len(q.ordered) {
		q.idx = 0
	}
	return nil, l
}

func (q *Queue) GetByPartition(partition int) (Remote, *Logical, error) {
	defer q.mutex.RUnlock()
	q.mutex.RLock()

	if partition < 0 || partition >= len(q.logical) {
		return nil, nil, transport.NewInvalidOption("partition is invalid; '%d' is not a valid partition", partition)
	}

	if q.logical[partition] == nil {
		// TODO: This is likely to not happen until we support the concept of 'remote'. We may need to remove this
		//  check later.
		return nil, nil, transport.NewInvalidOption("partition is invalid; '%d' has no valid logical queue", partition)
	}

	return nil, q.logical[partition], nil
}
