package timing_wheel

import (
	"container/list"
)

type Timer struct {
	expiration int64
	task       func()

	// the bucket that the list which this timer's element belongs to
	b       *Bucket
	element *list.Element
}

func (t *Timer) GetBucket() *Bucket {
	return t.b
}

func (t *Timer) SetBucket(b *Bucket) {
	t.b = b
}
