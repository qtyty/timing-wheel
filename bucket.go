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
	t.SetBucket(b)
	t.element = e

	b.mu.Unlock()
}

func (b *Bucket) remove(t *Timer) bool {
	if t.GetBucket() != b {
		return false
	}

	b.timers.Remove(t.element)
	t.SetBucket(nil)
	t.element = nil
	return true
}

func (b *Bucket) Remove(t *Timer) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.remove(t)
}

func (b *Bucket) Flush(reinsert func(*Timer)) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i, e := b.timers.Len(), b.timers.Front(); i > 0; i, e = i-1, e.Next() {
		t := e.Value.(*Timer)
		b.remove(t)

		// reinsert time : run or add to a lower-level wheel
		reinsert(t)
	}
	b.SetExpiration(-1)
}
