package types

// Batch is a batch of requests structs
type Batch[T any] struct {
	Requests []*T
}

func (r *Batch[T]) Add(req *T) {
	if req == nil {
		return
	}
	r.Requests = append(r.Requests, req)
}

// Remove removes the request from the batch
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

func (r *Batch[T]) Reset() {
	r.Requests = r.Requests[:0]
}

type ProduceBatch struct {
	Requests []*ProduceRequest
}

func (r *ProduceBatch) Add(req *ProduceRequest) {
	if req == nil {
		return
	}
	r.Requests = append(r.Requests, req)
}

func (r *ProduceBatch) Reset() {
	// Remove all requests that have Assigned == true
	n := 0
	for _, req := range r.Requests {
		if !req.Assigned {
			r.Requests[n] = req
			n++
		}
	}
	r.Requests = r.Requests[:n]
}

// LeaseBatch is a batch of lease requests. It is unique from other Batch requests
// in that each request must be unique.
// TODO(thrawn01): An array of requests might not be the most efficient, perhaps a SkipList
type LeaseBatch struct {
	Requests       []*LeaseRequest
	TotalRequested int
}

func (r *LeaseBatch) Reset() {
	r.Requests = r.Requests[:0]
	r.TotalRequested = 0
}

func (r *LeaseBatch) Add(req *LeaseRequest) {
	r.TotalRequested += req.NumRequested
	r.Requests = append(r.Requests, req)
}

// MarkNil marks the request as nil. See FilterNils for explanation
func (r *LeaseBatch) MarkNil(idx int) {
	r.TotalRequested -= r.Requests[idx].NumRequested
	r.Requests[idx] = nil
}

// FilterNils filters out any nils in the Batch by using a filter in place algorithm which
// resizes the slice without altering the capacity. We filter out nils as it's more efficient
// and safer to mark requests as nil during iteration, then clean up the nils after.
func (r *LeaseBatch) FilterNils() {
	// Filter in place algorithm. Removes the request and moves all
	// items up the slice then resizes the slice
	n := 0
	for _, i := range r.Requests {
		if i != nil {
			r.Requests[n] = i
			n++
		}
	}
	r.Requests = r.Requests[:n]
}

func (r *LeaseBatch) Iterator() LeaseBatchIterator {
	return LeaseBatchIterator{b: r}
}

// LeaseBatchIterator distributes items to the requests in the batch in an iterative fashion
type LeaseBatchIterator struct {
	b   *LeaseBatch
	pos int
}

func (it *LeaseBatchIterator) Next(item *Item) bool {
	count := len(it.b.Requests)
	// Find the next request in the batch which has not met its NumRequested limit
	for count != 0 {
		// If we encounter a nil, then we skip it
		if it.b.Requests[it.pos] == nil {
			it.pos++
			if it.pos == len(it.b.Requests) {
				it.pos = 0
			}
			count--
			continue
		}
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

// CompleteBatch is a batch of complete requests
type CompleteBatch struct {
	Requests []*CompleteRequest
}

func (b *CompleteBatch) Add(req *CompleteRequest) {
	if req == nil {
		return
	}
	b.Requests = append(b.Requests, req)
}

func (b *CompleteBatch) Remove(req *CompleteRequest) {
	n := 0
	for _, i := range b.Requests {
		if i != req {
			b.Requests[n] = i
			n++
		}
	}
	b.Requests = b.Requests[:n]
}

func (b *CompleteBatch) Reset() {
	b.Requests = b.Requests[:0]
}

// RetryBatch is a batch of retry requests
type RetryBatch struct {
	Requests []*RetryRequest
}

func (b *RetryBatch) Add(req *RetryRequest) {
	if req == nil {
		return
	}
	b.Requests = append(b.Requests, req)
}

func (b *RetryBatch) Remove(req *RetryRequest) {
	n := 0
	for _, i := range b.Requests {
		if i != req {
			b.Requests[n] = i
			n++
		}
	}
	b.Requests = b.Requests[:n]
}

func (b *RetryBatch) Reset() {
	b.Requests = b.Requests[:0]
}

// LifeCycleBatch is a batch of lifecycle requests
type LifeCycleBatch struct {
	Requests []*LifeCycleRequest
}

func (b *LifeCycleBatch) Add(req *LifeCycleRequest) {
	if req == nil {
		return
	}
	b.Requests = append(b.Requests, req)
}

func (b *LifeCycleBatch) Remove(req *LifeCycleRequest) {
	n := 0
	for _, i := range b.Requests {
		if i != req {
			b.Requests[n] = i
			n++
		}
	}
	b.Requests = b.Requests[:n]
}

func (b *LifeCycleBatch) Reset() {
	b.Requests = b.Requests[:0]
}
