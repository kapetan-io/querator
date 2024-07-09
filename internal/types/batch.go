package types

type Ptr[T any] interface {
	*T
}

// Batch is a batch of requests structs
type Batch[T any] struct {
	Requests []*T
}

func (r *Batch[T]) Add(req *T) {
	r.Requests = append(r.Requests, req)
}

// Remove removes the reserve request from the batch
func (r *Batch[T]) Remove(req *T) {
	// Filter in place algorithm. Removes the request and moves all
	// items up the slice then resizes the slice
	n := 0
	for _, i := range r.Requests {
		if i != req {
			r.Requests[n] = i
			n++
		}
	}
	r.Requests = r.Requests[:n]
}

// ReserveBatch is a batch of reserve requests. It is unique from other Batch requests
// in that each request must be unique.
// TODO(thrawn01): An array of requests might not be the most efficient, perhaps a SkipList
type ReserveBatch struct {
	Requests []*ReserveRequest
	Total    int
}

func (r *ReserveBatch) Add(req *ReserveRequest) {
	r.Total += req.NumRequested
	r.Requests = append(r.Requests, req)
}

// Remove removes the reserve request from the batch
func (r *ReserveBatch) Remove(req *ReserveRequest) {
	r.Total -= req.NumRequested

	// Filter in place algorithm. Removes the request and moves all
	// items up the slice then resizes the slice
	n := 0
	for _, i := range r.Requests {
		if i != req {
			r.Requests[n] = i
			n++
		}
	}
	r.Requests = r.Requests[:n]
}

func (r *ReserveBatch) Iterator() ReserveBatchIterator {
	return ReserveBatchIterator{b: r}
}

// ReserveBatchIterator distributes items to the requests in the batch in an iterative fashion
type ReserveBatchIterator struct {
	b   *ReserveBatch
	pos int
}

func (it *ReserveBatchIterator) Next(item *Item) bool {
	count := len(it.b.Requests)
	// Find the next request in the batch which has not met its NumRequested limit
	for count != 0 {
		if len(it.b.Requests[it.pos].Items) == it.b.Requests[it.pos].NumRequested {
			it.pos++
			if it.pos == len(it.b.Requests) {
				it.pos = 0
			}
			count--
			continue
		}
		it.b.Requests[it.pos].Items = append(it.b.Requests[it.pos].Items, item)
		it.pos++
		if it.pos == len(it.b.Requests) {
			it.pos = 0
		}
		return true
	}
	return false
}
