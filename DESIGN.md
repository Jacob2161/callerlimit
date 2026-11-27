# Design

Callerlimit limits what each caller of a server may use: how many of its
requests hold a resource at once (`Slots`), and how fast it may send them
(`Rates`). Both are fair across callers rather than across requests, so one
busy caller cannot starve the rest. This document says how they work and
why.

## Slots

### Handing out slots

A pool has a fixed number of slots. A request takes a free slot at once only
when nobody is waiting; otherwise it would overtake requests already in
line. A request that cannot take a slot joins its caller's queue, and the
callers with queued requests wait in a ring:

1. A released slot goes to the oldest queued request of the caller at the
   front of the ring.
2. That caller moves to the back of the ring, still holding its other
   queued requests.
3. A caller whose queue empties leaves the ring.

A lone caller may therefore use every slot, and a caller that arrives later
gets a slot as soon as each caller ahead of it has had one turn. Turns are
per caller, not per request, so a caller with a hundred queued requests gets
the same share of releases as a caller with one.

### One lock

Every decision (taking a free slot, queuing, handing a released slot to the
next request, removing a request that gave up) is made under the pool's one
mutex. A released slot is assigned to the next request inside `release`,
before the lock is dropped, so no request that arrives afterwards, not even
one from the caller that released it, can take the slot first. The cost is
one short critical section per acquire and release, about 200 ns under
parallel load (`BenchmarkAcquireRelease`), which is small next to the
requests a slot guards.

### Waiting, and giving up

A queued request waits on a channel the releaser closes, a timer, and its
context. The grant and the end of the wait can race, so a request records
whether it was assigned a slot under the lock, and on waking:

- A request that was granted a slot and is still wanted keeps it.
- A request that was granted a slot but whose context ended passes the slot
  on to the next in line, so a cancelled request never leaks a slot.
- A request that was not granted one leaves its queue and returns its
  context's error, or `ReasonWaitExpired` after the queue timeout.

### Bounds and defaults

Waits and queues are bounded, so overload is refused at once with a retry
delay instead of piling up memory and latency:

- `QueueTimeout`, 250 ms by default: requests that hold a slot for
  milliseconds clear most bursts within it, and a request that waits longer
  is better told to retry.
- `MaximumQueued`, by default 4 × Slots, at least 256 and at most 4096, so
  the queue scales with the pool without growing unbounded.
- `MaximumCallerQueued`, by default Slots but at most a quarter of
  `MaximumQueued`, so a lone caller can keep the pool busy but cannot fill
  the queue alone.
- A full queue asks the caller to retry after 2 s (`QueueFullRetry`), longer
  than a wait that ran out (1 s, `WaitExpiredRetry`), since a full queue
  means more callers are ahead.

`OnOccupancy` runs under the lock after every change, so gauges see every
state in order; it must be fast and must not call back into the pool.

## Rates

`Rates` holds one `golang.org/x/time/rate` token bucket per caller and
limit.

- **Keyed by caller and limit.** A bucket is never reconfigured. A caller
  whose limit changes, such as on a plan upgrade, gets a fresh bucket under
  the new limit, while requests allowed under the old one keep using
  theirs. This avoids the races of changing a live limiter's rate.
- **Bounded and idle-expiring.** Buckets live in an otter cache, bounded by
  `MaximumBuckets` (100,000 by default) with W-TinyLFU eviction, and dropped
  after `IdleExpiry` (an hour by default) without use. A dropped bucket
  comes back full, which is what an idle caller would have earned anyway.
- **Never waits.** `Allow` reserves one token and cancels the reservation
  if it would have to wait, returning the delay as `RetryAfter`. `WaitN`
  is for byte rates, where waiting is right: it waits in burst-sized steps,
  so a request larger than the burst slows down instead of failing.
- **Close.** The cache runs one goroutine that drops idle buckets. `Close`
  stops it and waits for it, so a server that stops, or a test, leaves no
  goroutine behind.

## Callers

A caller is any comparable value that equals itself: an API key ID, a user
ID, or a struct of several. A NaN float does not equal itself, so no map
can find its entry again; both types refuse it with `ErrInvalidCaller`
before changing anything.

## Testing

The slot tests run in `testing/synctest` bubbles, so queue timeouts pass on
virtual time and `synctest.Wait` replaces polling for a request to queue.
`TestSlotRotationChurn` runs on real time on purpose: it races acquires,
cancellations, and releases across real goroutine scheduling, which a
bubble would serialize, and checks the slot bound under the race detector.
