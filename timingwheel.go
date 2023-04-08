package timing_wheel

import (
	"errors"
	"sync/atomic"
	"time"
	"unsafe"
)

type TimingWheel struct {
	tick      int64
	wheelSize int64

	interval    int64
	currentTime int64
	buckets     []*Bucket
	queue       *DelayQueue

	overflowWheel unsafe.Pointer

	exitC     chan struct{}
	waitGroup WaitGroupWrapper
}

func NewTimerWheel(tick time.Duration, wheelSize int64) *TimingWheel {
	tickMs := int64(tick / time.Millisecond)
	if tickMs <= 0 {
		panic(errors.New("tick must be greater than or equal to 1ms"))
	}

	startMs := timeToMs(time.Now().UTC())
	return newTimingWheel(tickMs, wheelSize, startMs, NewDelayQueue(int(wheelSize)))
}

func newTimingWheel(tickMs int64, wheelSize int64, startMs int64, queue *DelayQueue) *TimingWheel {
	buckets := make([]*Bucket, wheelSize)
	for i := range buckets {
		buckets[i] = NewBucket()
	}
	return &TimingWheel{
		tick:          tickMs,
		wheelSize:     wheelSize,
		interval:      tickMs * wheelSize,
		currentTime:   truncate(startMs, tickMs),
		buckets:       buckets,
		queue:         queue,
		overflowWheel: nil,
		exitC:         make(chan struct{}),
	}
}

func (tw *TimingWheel) Add(t *Timer) bool {
	currentTime := atomic.LoadInt64(&tw.currentTime)
	if t.expiration < currentTime+tw.tick {
		return false
	} else if t.expiration < currentTime+tw.interval {
		// put it into its own bucket
		virtualID := t.expiration / tw.tick
		b := tw.buckets[virtualID%tw.wheelSize]
		b.Add(t)

		if b.SetExpiration(virtualID * tw.tick) {
			tw.queue.Offer(b, int(b.Expiration()))
		}

		return true
	} else {
		// overflow wheel
		overflowWheel := atomic.LoadPointer(&tw.overflowWheel)
		if overflowWheel == nil {
			atomic.CompareAndSwapPointer(
				&tw.overflowWheel,
				nil,
				unsafe.Pointer(newTimingWheel(
					tw.interval,
					tw.wheelSize,
					currentTime,
					tw.queue,
				)),
			)
		}
		return (*TimingWheel)(overflowWheel).Add(t)
	}
}

func (tw *TimingWheel) AddOrRun(t *Timer) {
	if !tw.Add(t) {
		go t.task()
	}
}

func (tw *TimingWheel) AdvanceClock(expiration int64) {
	currentTime := atomic.LoadInt64(&tw.currentTime)
	if expiration >= currentTime+tw.tick {
		currentTime = truncate(expiration, tw.tick)
		atomic.StoreInt64(&tw.currentTime, currentTime)

		overflowWheel := atomic.LoadPointer(&tw.overflowWheel)
		if overflowWheel != nil {
			(*TimingWheel)(overflowWheel).AdvanceClock(expiration)
		}
	}
}

func (tw *TimingWheel) Start() {
	tw.waitGroup.Wrap(func() {
		tw.queue.Poll(tw.exitC, func() int {
			return int(timeToMs(time.Now().UTC()))
		})
	})

	tw.waitGroup.Wrap(func() {
		for {
			select {
			case elem := <-tw.queue.C:
				b := elem.(*Bucket)
				tw.AdvanceClock(b.Expiration())
				b.Flush(tw.AddOrRun)
			case <-tw.exitC:
				return
			}
		}
	})

}

func (tw *TimingWheel) Stop() {
	close(tw.exitC)
	tw.waitGroup.Wait()
}

func (tw *TimingWheel) AfterFunc(d time.Duration, f func()) *Timer {
	t := &Timer{
		expiration: timeToMs(time.Now().UTC().Add(d)),
		task:       f,
	}
	tw.AddOrRun(t)
	return t
}

type Scheduler interface {
	// Next time must be utc time
	Next(time.Time) time.Time
}

func (tw *TimingWheel) SchedulerFunc(s Scheduler, f func()) *Timer {
	expiration := s.Next(time.Now().UTC())
	if expiration.IsZero() {
		return nil
	}
	var t *Timer
	t = &Timer{
		expiration: timeToMs(expiration),
		task: func() {
			expiration := s.Next(msToTime(t.expiration))
			if !expiration.IsZero() {
				t.expiration = timeToMs(expiration)
				tw.AddOrRun(t)
			}

			// actually execute the task
			f()
		},
	}
	tw.AddOrRun(t)
	return t
}
