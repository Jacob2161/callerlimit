package callerlimit

import (
	"context"
	"errors"
	"fmt"
	"math"
	mathrand "math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// waitQueued waits, inside a testing/synctest bubble, until every request
// started has queued or finished, and checks that count requests wait.
func waitQueued(t *testing.T, pool *Slots[string], count int) {
	t.Helper()
	synctest.Wait()
	if queued := pool.Occupancy().Queued; queued != count {
		t.Fatalf("%d requests wait in the pool, want %d", queued, count)
	}
}

// setQueueTimeout changes how long the pool's requests wait.
func setQueueTimeout(pool *Slots[string], queueTimeout time.Duration) {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	pool.queueTimeout = queueTimeout
}

// requirePoolDenial checks that err is the pool's refusal with retryAfter.
func requirePoolDenial(t *testing.T, err error, retryAfter time.Duration, context string) {
	t.Helper()
	denial, ok := errors.AsType[*OverloadError](err)
	if !ok || denial.Pool != "uploads" || denial.RetryAfter != retryAfter ||
		!errors.Is(err, ErrOverloaded) {
		t.Fatalf("%s got %v, want an uploads refusal to retry after %s", context, err, retryAfter)
	}
}

// requireEmptyPool checks that nothing is held, waiting, or tracked.
func requireEmptyPool(t *testing.T, pool *Slots[string]) {
	t.Helper()
	occupancy := pool.Occupancy()
	pool.mutex.Lock()
	tracked := len(pool.callers) + len(pool.waiting)
	pool.mutex.Unlock()
	if occupancy.Reserved != 0 || occupancy.Queued != 0 || occupancy.ReservingCallers != 0 ||
		tracked != 0 {
		t.Fatalf(
			"pool after every release: %+v, %d keys tracked; want it empty",
			occupancy,
			tracked,
		)
	}
}

// acquireInBackground starts an acquire and returns where its answer goes.
func acquireInBackground(
	ctx context.Context,
	pool *Slots[string],
	keyID string,
) chan acquireAnswer {
	answers := make(chan acquireAnswer, 1)
	go func() {
		release, err := pool.Acquire(ctx, keyID)
		answers <- acquireAnswer{release: release, err: err}
	}()
	return answers
}

// acquireAnswer is one background acquire's slot or refusal.
type acquireAnswer struct {
	release func()
	err     error
}

// receiveGrant waits for a background acquire that must get its slot.
func receiveGrant(t *testing.T, answers chan acquireAnswer, keyID string) func() {
	t.Helper()
	select {
	case granted := <-answers:
		if granted.err != nil {
			t.Fatalf("%s: %v", keyID, granted.err)
		}
		return granted.release
	case <-time.After(5 * time.Second):
		t.Fatalf("%s got no slot, want it to have its turn", keyID)
		return nil
	}
}

// TestLoneKeyUsesEverySlot checks that a lone key may hold the
// whole pool, and that its next request waits and then is refused.
func TestLoneKeyUsesEverySlot(t *testing.T) {
	for _, slots := range []int{1, 2, 7, 64} {
		t.Run(fmt.Sprint(slots), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				pool := NewSlots[string](
					&NewSlotsArgs{
						Name:         "uploads",
						Slots:        slots,
						QueueTimeout: 20 * time.Millisecond,
					},
				)
				var releases []func()
				for range slots {
					release, err := pool.Acquire(t.Context(), "lone")
					if err != nil {
						t.Fatalf("slot %d of %d for a lone key: %v", len(releases)+1, slots, err)
					}
					releases = append(releases, release)
				}
				_, err := pool.Acquire(t.Context(), "lone")
				requirePoolDenial(t, err, WaitExpiredRetry, "a lone key past the full pool")
				for _, release := range releases {
					release()
				}
				requireEmptyPool(t, pool)
			})
		})
	}
}

// TestSlotRotationCanceledRequest checks that a request that already gave up
// takes nothing, even from a free slot.
func TestSlotRotationCanceledRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool := NewSlots[string](&NewSlotsArgs{Name: "uploads", Slots: 2})
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := pool.Acquire(ctx, "canceled"); !errors.Is(err, context.Canceled) {
			t.Fatalf("a canceled request got %v, want context.Canceled", err)
		}
		requireEmptyPool(t, pool)
	})
}

// TestSlotRotationHandoff checks that a released slot goes to the waiting
// key under the mutex, so a request arriving afterwards, even from the key
// that released it, cannot take it first.
func TestSlotRotationHandoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool := NewSlots[string](
			&NewSlotsArgs{Name: "uploads", Slots: 2, QueueTimeout: time.Minute},
		)
		first, err := pool.Acquire(t.Context(), "busy")
		if err != nil {
			t.Fatal(err)
		}
		second, err := pool.Acquire(t.Context(), "busy")
		if err != nil {
			t.Fatal(err)
		}
		waiting := acquireInBackground(t.Context(), pool, "waiting")
		waitQueued(t, pool, 1)
		second()
		if occupancy := pool.Occupancy(); occupancy.Reserved != 2 || occupancy.Queued != 0 ||
			occupancy.ReservingCallers != 2 {
			t.Fatalf(
				"after the release the pool is %+v, want the slot handed to the waiter",
				occupancy,
			)
		}
		setQueueTimeout(pool, 10*time.Millisecond)
		for _, keyID := range []string{"busy", "barging"} {
			_, err = pool.Acquire(t.Context(), keyID)
			requirePoolDenial(t, err, WaitExpiredRetry, keyID+" arriving after the handoff")
		}
		receiveGrant(t, waiting, "the waiter")()
		first()
		requireEmptyPool(t, pool)
	})
}

// TestSlotRotationQueueLimits checks the per-key and total bounds on queued
// requests and the refusal delays.
func TestSlotRotationQueueLimits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool := NewSlots[string](
			&NewSlotsArgs{
				Name:                "uploads",
				Slots:               1,
				QueueTimeout:        time.Minute,
				MaximumQueued:       2,
				MaximumCallerQueued: 1,
			},
		)
		release, err := pool.Acquire(t.Context(), "first-caller")
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		first := acquireInBackground(ctx, pool, "first")
		waitQueued(t, pool, 1)
		_, err = pool.Acquire(t.Context(), "first")
		requirePoolDenial(t, err, QueueFullRetry, "a key's second queued request")
		second := acquireInBackground(ctx, pool, "second")
		waitQueued(t, pool, 2)
		_, err = pool.Acquire(t.Context(), "third")
		requirePoolDenial(t, err, QueueFullRetry, "a request past the total bound")
		cancel()
		for _, answers := range []chan acquireAnswer{first, second} {
			if canceled := <-answers; !errors.Is(canceled.err, context.Canceled) {
				t.Fatalf("a canceled waiter got %v, want context.Canceled", canceled.err)
			}
		}
		setQueueTimeout(pool, 10*time.Millisecond)
		_, err = pool.Acquire(t.Context(), "timeout")
		requirePoolDenial(t, err, WaitExpiredRetry, "a wait that ran out")
		release()
		release()
		requireEmptyPool(t, pool)
	})
}

// TestSlotRotationTurnOrder fills the pool with one busy key, queues 100 other
// keys behind its next request, and checks that each released slot goes to
// the next key in arrival order, and that the busy key, queued again after
// its turn, comes after all of them.
func TestSlotRotationTurnOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const slots, queuedKeys = 4, 100
		pool := NewSlots[string](
			&NewSlotsArgs{Name: "uploads", Slots: slots, QueueTimeout: time.Minute},
		)
		var releases []func()
		for range slots {
			release, err := pool.Acquire(t.Context(), "busy")
			if err != nil {
				t.Fatal(err)
			}
			releases = append(releases, release)
		}
		busyQueued := acquireInBackground(t.Context(), pool, "busy")
		waitQueued(t, pool, 1)
		queuedAnswers := make([]chan acquireAnswer, queuedKeys)
		for index := range queuedAnswers {
			queuedAnswers[index] = acquireInBackground(
				t.Context(),
				pool,
				fmt.Sprintf("key-%03d", index),
			)
			waitQueued(t, pool, index+2)
		}
		// The first slot freed is the busy key's own queued request's turn.
		releases[0]()
		releases[0] = receiveGrant(t, busyQueued, "busy")
		busyAgain := acquireInBackground(t.Context(), pool, "busy")
		waitQueued(t, pool, queuedKeys+1)
		for index, answers := range queuedAnswers {
			// Free one slot at a time, oldest first, so only the next key can
			// have been given it.
			releases[0]()
			releases = append(
				releases[1:],
				receiveGrant(t, answers, fmt.Sprintf("key-%03d", index)),
			)
			if occupancy := pool.Occupancy(); occupancy.Reserved != slots ||
				occupancy.Queued != queuedKeys-index {
				t.Fatalf(
					"after key-%03d's turn the pool is %+v, want %d queued requests",
					index,
					occupancy,
					queuedKeys-index,
				)
			}
		}
		releases[0]()
		releases[0] = receiveGrant(t, busyAgain, "busy after the other keys")
		for _, release := range releases {
			release()
		}
		requireEmptyPool(t, pool)
	})
}

// TestSlotRotationChurn runs on real time, on purpose: it races acquires,
// cancellations, and releases across real goroutine scheduling, which a
// synctest bubble would serialize. Its sleeps add jitter; nothing waits on
// them.
//
// It races acquires, timeouts, cancellations, and releases
// from many keys, checks the bound each time a slot is granted, and that
// the pool ends empty after repeated releases.
func TestSlotRotationChurn(t *testing.T) {
	for _, slots := range []int{1, 2, 5} {
		t.Run(fmt.Sprint(slots), func(t *testing.T) {
			pool := NewSlots[string](
				&NewSlotsArgs{
					Name:         "uploads",
					Slots:        slots,
					QueueTimeout: 2 * time.Millisecond,
				},
			)
			var requests sync.WaitGroup
			var violations sync.Map
			for worker := range 32 {
				requests.Go(func() {
					keyID := fmt.Sprintf("key-%d", worker%7)
					for attempt := range 200 {
						ctx, cancel := context.WithCancel(t.Context())
						if attempt%5 == 0 {
							// Cancel while the request may be waiting or just granted.
							go cancel()
						}
						release, err := pool.Acquire(ctx, keyID)
						if err == nil {
							if occupancy := pool.Occupancy(); occupancy.Reserved > occupancy.Slots {
								violations.Store(occupancy, true)
							}
							if mathrand.IntN(2) == 0 {
								time.Sleep(time.Duration(mathrand.IntN(200)) * time.Microsecond)
							}
							release()
							release()
						}
						cancel()
					}
				})
			}
			requests.Wait()
			violations.Range(func(occupancy any, _ any) bool {
				t.Errorf("held more than the pool: %+v", occupancy)
				return true
			})
			requireEmptyPool(t, pool)
		})
	}
}

// BenchmarkSlotRotationParallel measures a slot taken and given back by many
// goroutines across 100 keys, the request gate's work on every request.
func BenchmarkSlotRotationParallel(b *testing.B) {
	pool := NewSlots[string](&NewSlotsArgs{Name: "requests", Slots: 2048})
	keyIDs := make([]string, 100)
	for index := range keyIDs {
		keyIDs[index] = fmt.Sprintf("key-%03d", index)
	}
	var next atomic.Int64
	b.RunParallel(func(requests *testing.PB) {
		keyID := keyIDs[int(next.Add(1))%len(keyIDs)]
		ctx := context.Background()
		for requests.Next() {
			release, err := pool.Acquire(ctx, keyID)
			if err != nil {
				b.Error(err)
				return
			}
			release()
		}
	})
}

// TestSlotRotationKeysAlternate queues three requests of a busy key and then
// one each of two other keys, and checks that released slots go to the busy
// key's oldest first, then alternate by key: busy, a, b, busy, busy.
func TestSlotRotationKeysAlternate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool := NewSlots[string](
			&NewSlotsArgs{
				Name:                "uploads",
				Slots:               1,
				QueueTimeout:        time.Minute,
				MaximumCallerQueued: 3,
			},
		)
		release, err := pool.Acquire(t.Context(), "busy")
		if err != nil {
			t.Fatal(err)
		}
		type queuedTurn struct {
			label   string
			answers chan acquireAnswer
		}
		queue := func(keyID string, label string) queuedTurn {
			// Read the count first: the request may queue before the read.
			queued := pool.Occupancy().Queued
			answers := acquireInBackground(t.Context(), pool, keyID)
			waitQueued(t, pool, queued+1)
			return queuedTurn{label: label, answers: answers}
		}
		busyFirst := queue("busy", "busy's first")
		busySecond := queue("busy", "busy's second")
		busyThird := queue("busy", "busy's third")
		_, err = pool.Acquire(t.Context(), "busy")
		requirePoolDenial(t, err, QueueFullRetry, "busy past its queued requests")
		keyA := queue("a", "a")
		keyB := queue("b", "b")
		if occupancy := pool.Occupancy(); occupancy.Queued != 5 || occupancy.QueuedCallers != 3 {
			t.Fatalf("queued pool: %+v, want 5 queued requests from 3 keys", occupancy)
		}
		for _, turn := range []queuedTurn{busyFirst, keyA, keyB, busySecond, busyThird} {
			release()
			release = receiveGrant(t, turn.answers, turn.label)
		}
		release()
		requireEmptyPool(t, pool)
	})
}

// TestNaNCallerIsRefused checks that a caller no map can find again is
// refused before it changes anything.
func TestNaNCallerIsRefused(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool := NewSlots[float64](
			&NewSlotsArgs{Name: "uploads", Slots: 1, QueueTimeout: 10 * time.Millisecond},
		)
		release, err := pool.Acquire(t.Context(), 1)
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		if _, err := pool.Acquire(t.Context(), math.NaN()); !errors.Is(err, ErrInvalidCaller) {
			t.Fatalf("a NaN caller: %v", err)
		}
		if occupancy := pool.Occupancy(); occupancy.Queued != 0 || occupancy.ReservingCallers != 1 {
			t.Fatalf("the NaN caller changed the pool: %+v", occupancy)
		}
	})
}

// TestCallerSlots checks that CallerSlots counts the slots each caller holds,
// leaves out callers that hold none, and returns a copy.
func TestCallerSlots(t *testing.T) {
	pool := NewSlots[string](&NewSlotsArgs{Name: "uploads", Slots: 3})
	var releases []func()
	for _, caller := range []string{"alice", "alice", "bob"} {
		release, err := pool.Acquire(t.Context(), caller)
		if err != nil {
			t.Fatal(err)
		}
		releases = append(releases, release)
	}
	reserved := pool.CallerSlots()
	if len(reserved) != 2 || reserved["alice"] != 2 || reserved["bob"] != 1 {
		t.Fatalf("CallerSlots() = %v, want alice 2 and bob 1", reserved)
	}
	reserved["alice"] = 99
	releases[2]()
	if again := pool.CallerSlots(); len(again) != 1 || again["alice"] != 2 {
		t.Fatalf("after bob's release CallerSlots() = %v, want alice 2 only", again)
	}
	releases[0]()
	releases[1]()
}
