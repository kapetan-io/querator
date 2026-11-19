package types

type Batch[T any] struct {
	Requests []*T
}

func (r *Batch[T]) Add(req *T) {
	if req == nil {
		return
	}
	r.Requests = append(r.Requests, req)
}

func (r *Batch[T]) Remove(req *T) {
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

func (r *ProduceBatch) DropAbandonedHandOffs() {
	n := 0
	for _, req := range r.Requests {
		if req.DeadLetterHandOff && req.Context.Err() != nil {
			req.Err = req.Context.Err()
			close(req.ReadyCh)
			continue
		}
		r.Requests[n] = req
		n++
	}
	r.Requests = r.Requests[:n]
}

func (r *ProduceBatch) Reset() {
	n := 0
	for _, req := range r.Requests {
		if !req.Assigned {
			r.Requests[n] = req
			n++
		}
	}
	r.Requests = r.Requests[:n]
}

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

func (r *LeaseBatch) MarkNil(idx int) {
	r.TotalRequested -= r.Requests[idx].NumRequested
	r.Requests[idx] = nil
}

func (r *LeaseBatch) FilterNils() {
	n := 0
	for _, i := range r.Requests {
		if i != nil {
			r.Requests[n] = i
			n++
		}
	}
	r.Requests = r.Requests[:n]
}

func (r *LeaseBatch) Unassign(items []*Item) {
	if len(items) == 0 {
		return
	}
	unassigned := make(map[*Item]struct{}, len(items))
	for _, item := range items {
		unassigned[item] = struct{}{}
	}
	for _, req := range r.Requests {
		if req == nil {
			continue
		}
		n := 0
		for _, item := range req.Items {
			if _, ok := unassigned[item]; !ok {
				req.Items[n] = item
				n++
			}
		}
		clear(req.Items[n:])
		req.Items = req.Items[:n]
	}
}

func (r *LeaseBatch) Iterator() LeaseBatchIterator {
	return LeaseBatchIterator{b: r}
}

type LeaseBatchIterator struct {
	b   *LeaseBatch
	pos int
}

func (it *LeaseBatchIterator) Next(item *Item) bool {
	reqCount := len(it.b.Requests)
	for count := reqCount; count > 0; count-- {
		var request = it.b.Requests[it.pos]

		if request == nil || len(request.Items) >= request.NumRequested {
			it.pos = (it.pos + 1) % reqCount
			continue
		}

		request.Items = append(request.Items, item)
		it.pos = (it.pos + 1) % reqCount
		return true
	}
	return false
}

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
