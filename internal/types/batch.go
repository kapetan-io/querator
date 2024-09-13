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

func (r *Batch[T]) Reset() {
	// TODO: Do I need to nil out all the items in the request array before GC will collect them?
	r.Requests = r.Requests[:0]
}

// ReserveBatch is a batch of reserve requests. It is unique from other Batch requests
// in that each request must be unique.
// TODO(thrawn01): An array of requests might not be the most efficient, perhaps a SkipList
type ReserveBatch struct {
	Requests []*ReserveRequest
	Total    int
}

func (r *ReserveBatch) Reset() {
	r.Requests = r.Requests[:0]
	r.Total = 0
}

func (r *ReserveBatch) Add(req *ReserveRequest) {
	r.Total += req.NumRequested
	r.Requests = append(r.Requests, req)
}

// MarkNil marks the request as nil. See FilterNils for explanation
func (r *ReserveBatch) MarkNil(idx int) {
	r.Total -= r.Requests[idx].NumRequested
	r.Requests[idx] = nil
}

// FilterNils filters out any nils in the Batch by using a filter in place algorithm which
// resizes the slice without altering the capacity. We filter out nils as it's more efficient
// and safer to mark requests as nil during iteration, then clean up the nils after.
func (r *ReserveBatch) FilterNils() {
	//fmt.Printf("FilterNils Before: %s\n", joinClientId(r.Requests))
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
	//fmt.Printf("FilterNils After: %s\n", joinClientId(r.Requests))
}

// TODO: Remove
//func joinClientId(reqs []*ReserveRequest) string {
//	var buf bytes.Buffer
//
//	for _, req := range reqs {
//		if req != nil {
//			buf.WriteString(req.ClientID)
//		} else {
//			buf.WriteString("nil")
//		}
//		buf.WriteString(", ")
//	}
//	return buf.String()
//}

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
