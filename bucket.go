package timing_wheel

import (
	"container/list"
	"sync"
	"sync/atomic"
)

type Bucket struct {
	expiration int64

	mu     sync.Mutex
	timers *list.List
}

func NewBucket() *Bucket {
	return &Bucket{
		expiration: -1,
		timers:     list.New(),
	}
}

func (b *Bucket) Expiration() int64 {
	return atomic.LoadInt64(&b.expiration)
}

func (b *Bucket) SetExpiration(exp int64) bool {
	return atomic.SwapInt64(&b.expiration, exp) != exp
}

func (b *Bucket) Add(t *Timer) {
	b.mu.Lock()

	e := b.timers.PushBack(t)
	t.element = e

	b.mu.Unlock()
}

func (b *Bucket) Remove(t *Timer) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if t.GetBucket() != b {
		return false
	}

	b.timers.Remove(t.element)
	t.SetBucket(nil)
	t.element = nil
	return true
}

func (b *Bucket) Flush(reinsert func(*Timer)) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for e := b.timers.Front(); e != nil; e = e.Next() {
		t := e.Value.(*Timer)
		b.Remove(t)

		// reinsert time : run or add to a lower-level wheel
		reinsert(t)
	}
	b.SetExpiration(-1)
}
