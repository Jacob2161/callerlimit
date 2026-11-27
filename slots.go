// Package callerlimit limits what each caller of a server may use, fairly.
//
// Slots bounds how many requests use a resource at once. When every slot is
// taken, a request waits in its caller's queue, and the callers with queued
// requests wait in a ring: a released slot goes to the oldest request of the
// caller at the front of the ring, and that caller moves to the back. A lone
// caller may use every slot, and a caller that arrives later gets a slot once
// each caller ahead of it had a turn, so one busy caller cannot starve the
// rest. Waits are bounded in time and queues in length; a request that cannot
// wait is refused at once with an *OverloadError that says when to retry.
//
// Rates holds a token bucket per caller and limit, created on first use and
// dropped when idle.
//
// A caller is any comparable value that equals itself: an API key ID, a user
// ID, or a struct of several. A float caller must not be NaN.
package callerlimit

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"
)

// Defaults for a Slots whose arguments leave them zero.
const (
	// DefaultQueueTimeout is how long a request waits for a slot: a request
	// that holds its slot for milliseconds admits most requests that arrive
	// together within it.
	DefaultQueueTimeout = 250 * time.Millisecond
	// QueueFullRetry is the retry delay after a refusal for a full queue,
	// longer than after a wait that ran out, which may get a slot on its
	// next try.
	QueueFullRetry = 2 * time.Second
	// WaitExpiredRetry is the retry delay after a wait that ran out.
	WaitExpiredRetry = time.Second
)

var (
	// ErrOverloaded is wrapped by every *OverloadError.
	ErrOverloaded = errors.New("overloaded")
	// ErrInvalidCaller refuses a caller that does not equal itself, such as
	// a NaN float, which no map can find again.
	ErrInvalidCaller = errors.New("a caller must equal itself")
)

// OverloadReason says why a request was refused.
type OverloadReason string

const (
	// ReasonQueueFull refuses a request whose caller, or the whole pool,
	// already has as many requests waiting as it may.
	ReasonQueueFull OverloadReason = "queue_full"
	// ReasonWaitExpired refuses a request that waited its whole queue
	// timeout without a slot.
	ReasonWaitExpired OverloadReason = "wait_expired"
	// ReasonRate refuses a request its caller's token bucket has no token
	// for (Rates.Allow).
	ReasonRate OverloadReason = "rate"
)

// OverloadError refuses a request. A server usually answers it with 429 or
// 503 and a Retry-After of RetryAfter.
type OverloadError struct {
	// Pool is the name of the Slots or Rates that refused.
	Pool string
	// Reason says why.
	Reason OverloadReason
	// RetryAfter is how long the caller should wait before it retries.
	RetryAfter time.Duration
}

func (e *OverloadError) Error() string {
	return fmt.Sprintf("%s overloaded (%s); retry after %s", e.Pool, e.Reason, e.RetryAfter)
}

// Unwrap returns ErrOverloaded.
func (e *OverloadError) Unwrap() error { return ErrOverloaded }

// Occupancy is a pool's use at one moment.
type Occupancy struct {
	// Slots is the pool's size, and Reserved how many are in use.
	Slots    int
	Reserved int
	// Queued counts the requests waiting, QueuedCallers the callers they
	// belong to, and ReservingCallers the callers holding slots.
	Queued           int
	QueuedCallers    int
	ReservingCallers int
}

// NewSlotsArgs configures NewSlots.
type NewSlotsArgs struct {
	// Name names the pool in refusals and for OnOccupancy.
	Name string
	// Slots is how many requests may hold a slot at once; at least 1.
	Slots int
	// QueueTimeout is how long a request waits for a slot. Zero or negative
	// uses DefaultQueueTimeout.
	QueueTimeout time.Duration
	// MaximumQueued bounds the queued requests of all callers. Zero or
	// negative uses min(4096, max(256, 4 × Slots)).
	MaximumQueued int
	// MaximumCallerQueued bounds one caller's queued requests. Zero or
	// negative uses Slots, so a lone caller keeps the pool busy, but no more
	// than a quarter of MaximumQueued, so one caller leaves room for others.
	MaximumCallerQueued int
	// OnOccupancy, when set, is called with the pool's occupancy after every
	// change, under the pool's lock: it must be fast and must not call back
	// into the pool. Use it to set gauges.
	OnOccupancy func(Occupancy)
}

// Slots hands a resource's short-lived slots to callers in turns. It is safe
// for concurrent use. Every decision is made under one lock, so no request
// takes a slot ahead of one that waits.
type Slots[K comparable] struct {
	name                string
	slots               int
	queueTimeout        time.Duration
	maximumQueued       int
	maximumCallerQueued int
	onOccupancy         func(Occupancy)

	mutex       sync.Mutex
	reserved    int
	queuedCount int
	// callers counts the slots each caller uses, for callers that use any.
	callers map[K]int
	// waiting holds the queue of each caller with queued requests.
	waiting map[K]*callerQueue[K]
	// ring holds the callerQueues of waiting callers in turn order.
	ring list.List
}

// callerQueue is one caller's queued requests, oldest first, and its place in
// the ring.
type callerQueue[K comparable] struct {
	caller   K
	requests []*queuedRequest
	turn     *list.Element
}

// queuedRequest is one queued request. granted is closed once a slot is
// assigned to it; assigned says so under the pool's lock.
type queuedRequest struct {
	granted  chan struct{}
	assigned bool
}

// NewSlots returns a pool configured by args, which may be nil for one slot
// and the defaults.
func NewSlots[K comparable](args *NewSlotsArgs) *Slots[K] {
	if args == nil {
		args = &NewSlotsArgs{}
	}
	slots := max(args.Slots, 1)
	maximumQueued := args.MaximumQueued
	if maximumQueued <= 0 {
		maximumQueued = min(4096, max(256, 4*slots))
	}
	maximumCallerQueued := args.MaximumCallerQueued
	if maximumCallerQueued <= 0 {
		maximumCallerQueued = max(1, min(slots, maximumQueued/4))
	}
	queueTimeout := args.QueueTimeout
	if queueTimeout <= 0 {
		queueTimeout = DefaultQueueTimeout
	}
	pool := &Slots[K]{
		name:                args.Name,
		slots:               slots,
		queueTimeout:        queueTimeout,
		maximumQueued:       maximumQueued,
		maximumCallerQueued: maximumCallerQueued,
		onOccupancy:         args.OnOccupancy,
		callers:             map[K]int{},
		waiting:             map[K]*callerQueue[K]{},
	}
	pool.mutex.Lock()
	pool.publishOccupancyLocked()
	pool.mutex.Unlock()
	return pool
}

// Occupancy reads the pool's occupancy.
func (p *Slots[K]) Occupancy() Occupancy {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.occupancyLocked()
}

func (p *Slots[K]) occupancyLocked() Occupancy {
	return Occupancy{
		Slots:            p.slots,
		Reserved:         p.reserved,
		Queued:           p.queuedCount,
		QueuedCallers:    p.ring.Len(),
		ReservingCallers: len(p.callers),
	}
}

// publishOccupancyLocked reports the occupancy to OnOccupancy.
func (p *Slots[K]) publishOccupancyLocked() {
	if p.onOccupancy != nil {
		p.onOccupancy(p.occupancyLocked())
	}
}

// SlotsByCaller copies the number of slots each caller holds.
func (p *Slots[K]) SlotsByCaller() map[K]int {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	slots := make(map[K]int, len(p.callers))
	for caller, count := range p.callers {
		slots[caller] = count
	}
	return slots
}

// Acquire takes a slot for caller, waiting up to the queue timeout for its
// turn. It returns the slot's release, which is safe to call more than once,
// or an *OverloadError, or ctx's error when ctx ends first.
func (p *Slots[K]) Acquire(ctx context.Context, caller K) (func(), error) {
	if caller != caller {
		return nil, ErrInvalidCaller
	}
	// A request that already gave up takes nothing, even from a free slot.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mutex.Lock()
	// It may have given up while waiting for the lock.
	if err := ctx.Err(); err != nil {
		p.mutex.Unlock()
		return nil, err
	}
	if p.reserved < p.slots && p.queuedCount == 0 {
		p.grantSlotLocked(caller)
		p.publishOccupancyLocked()
		p.mutex.Unlock()
		return p.slotRelease(caller), nil
	}
	queue := p.waiting[caller]
	if (queue != nil && len(queue.requests) >= p.maximumCallerQueued) ||
		p.queuedCount >= p.maximumQueued {
		p.mutex.Unlock()
		return nil, &OverloadError{Pool: p.name, Reason: ReasonQueueFull, RetryAfter: QueueFullRetry}
	}
	if queue == nil {
		queue = &callerQueue[K]{caller: caller}
		queue.turn = p.ring.PushBack(queue)
		p.waiting[caller] = queue
	}
	queued := &queuedRequest{granted: make(chan struct{})}
	queue.requests = append(queue.requests, queued)
	p.queuedCount++
	queueTimeout := p.queueTimeout
	p.publishOccupancyLocked()
	p.mutex.Unlock()

	timer := time.NewTimer(queueTimeout)
	defer timer.Stop()
	select {
	case <-queued.granted:
		// The grant and a cancellation may both be ready; a request that
		// gave up passes the slot on.
		if err := ctx.Err(); err != nil {
			p.mutex.Lock()
			defer p.mutex.Unlock()
			p.releaseLocked(caller)
			return nil, err
		}
		return p.slotRelease(caller), nil
	case <-timer.C:
	case <-ctx.Done():
	}
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if queued.assigned {
		// The slot arrived as the wait ended. A request that is still
		// wanted keeps it; one that gave up passes it on.
		if ctx.Err() == nil {
			return p.slotRelease(caller), nil
		}
		p.releaseLocked(caller)
		return nil, ctx.Err()
	}
	p.dequeueLocked(p.waiting[caller], queued)
	p.publishOccupancyLocked()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, &OverloadError{Pool: p.name, Reason: ReasonWaitExpired, RetryAfter: WaitExpiredRetry}
}

// slotRelease returns the once-only function that gives a caller's slot back.
func (p *Slots[K]) slotRelease(caller K) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			p.mutex.Lock()
			defer p.mutex.Unlock()
			p.releaseLocked(caller)
		})
	}
}

// releaseLocked gives one of the caller's slots back and hands free slots to
// the waiting callers in ring order.
func (p *Slots[K]) releaseLocked(caller K) {
	if p.callers[caller] <= 1 {
		delete(p.callers, caller)
	} else {
		p.callers[caller]--
	}
	p.reserved--
	for p.reserved < p.slots && p.ring.Len() > 0 {
		queue := p.ring.Front().Value.(*callerQueue[K])
		next := queue.requests[0]
		p.dequeueLocked(queue, next)
		if queue.turn != nil {
			p.ring.MoveToBack(queue.turn)
		}
		p.grantSlotLocked(queue.caller)
		next.assigned = true
		close(next.granted)
	}
	p.publishOccupancyLocked()
}

// grantSlotLocked gives one slot to the caller.
func (p *Slots[K]) grantSlotLocked(caller K) {
	p.callers[caller]++
	p.reserved++
}

// dequeueLocked takes a queued request out of its caller's queue, and the
// caller out of the ring when it has none left.
func (p *Slots[K]) dequeueLocked(queue *callerQueue[K], queued *queuedRequest) {
	for index, request := range queue.requests {
		if request == queued {
			queue.requests = slices.Delete(queue.requests, index, index+1)
			p.queuedCount--
			break
		}
	}
	if len(queue.requests) == 0 {
		p.ring.Remove(queue.turn)
		queue.turn = nil
		delete(p.waiting, queue.caller)
	}
}
