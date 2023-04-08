package timing_wheel

import (
	"container/heap"
	"sync"
	"sync/atomic"
	"time"
)

type item struct {
	Value    any
	Priority int
	Index    int
}

// a min heap
type priorityQueue []*item

func newPriorityQueue(capacity int) priorityQueue {
	return make(priorityQueue, 0, capacity)
}

func (pq *priorityQueue) Len() int {
	return len(*pq)
}

func (pq *priorityQueue) Less(i, j int) bool {
	return (*pq)[i].Priority < (*pq)[j].Priority
}

func (pq *priorityQueue) Swap(i, j int) {
	(*pq)[i], (*pq)[j] = (*pq)[j], (*pq)[i]
	(*pq)[i].Index = i
	(*pq)[j].Index = j
}

func (pq *priorityQueue) Push(x any) {
	i := x.(*item)
	i.Index = pq.Len()
	*pq = append(*pq, i)
}

func (pq *priorityQueue) Pop() any {
	n := pq.Len()
	x := (*pq)[n-1]
	*pq = (*pq)[:n-1]
	x.Index = -1
	return x
}

func (pq *priorityQueue) PeekAndShift(max int) (*item, int) {
	if pq.Len() == 0 {
		return nil, 0
	}

	x := (*pq)[0]
	if x.Priority > max {
		return nil, x.Priority - max
	}
	heap.Remove(pq, 0)
	return x, 0
}

type DelayQueue struct {
	C chan any

	mu sync.Mutex
	pq priorityQueue

	sleeping int32
	wakeupC  chan struct{}
}

func NewDelayQueue(size int) *DelayQueue {
	return &DelayQueue{
		C:       make(chan any),
		pq:      newPriorityQueue(size),
		wakeupC: make(chan struct{}),
	}
}

// Offer insert to the dp
func (dq *DelayQueue) Offer(elem any, expiration int) {
	x := &item{
		Value:    elem,
		Priority: expiration,
	}

	// lock
	dq.mu.Lock()
	heap.Push(&dq.pq, x)
	index := x.Index
	dq.mu.Unlock()

	if index == 0 {
		if atomic.CompareAndSwapInt32(&dq.sleeping, 1, 0) {
			dq.wakeupC <- struct{}{}
		}
	}
}

// Poll
// start an infinite loop
// wait an element to expire and send to the channel C
func (dq *DelayQueue) Poll(exitC chan struct{}, nowF func() int) {
	for {
		now := nowF()

		dq.mu.Lock()
		item, delta := dq.pq.PeekAndShift(now)
		if item == nil {
			atomic.StoreInt32(&dq.sleeping, 1)
		}
		dq.mu.Unlock()

		if item == nil {
			if delta == 0 {
				// no items
				select {
				case <-dq.wakeupC:
					continue
				case <-exitC:
					goto exit
				}
			} else if delta > 0 {
				select {
				case <-dq.wakeupC:
					// a new item with an earlier expiration than the current earliest one is added
					continue
				case <-time.After(time.Duration(delta) * time.Millisecond):
					if atomic.SwapInt32(&dq.sleeping, 0) == 0 {
						<-dq.wakeupC
					}
					continue
				case <-exitC:
					goto exit
				}
			}
		}

		select {
		case dq.C <- item.Value:
			// send value
		case <-exitC:
			goto exit
		}

	}
exit:
	//reset the state
	atomic.StoreInt32(&dq.sleeping, 0)
}
