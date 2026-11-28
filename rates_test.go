package callerlimit

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"golang.org/x/time/rate"
)

// TestRatesAllow checks that a caller's bucket refuses past its burst with
// the delay until its next token, that callers have separate buckets, and
// that a changed limit starts a fresh bucket.
func TestRatesAllow(t *testing.T) {
	rates, err := NewRates[string](&NewRatesArgs{Name: "requests"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rates.Close)
	slow := Limit{Rate: 1, Burst: 2}
	for range 2 {
		if err := rates.Allow("alice", slow); err != nil {
			t.Fatalf("within the burst: %v", err)
		}
	}
	err = rates.Allow("alice", slow)
	overload, ok := errors.AsType[*OverloadError](err)
	if !ok || !errors.Is(err, ErrOverloaded) || overload.Pool != "requests" ||
		overload.Reason != ReasonRate || overload.RetryAfter <= 0 ||
		overload.RetryAfter > time.Second {
		t.Fatalf("past the burst: %v, want a requests rate refusal within a second", err)
	}
	if err := rates.Allow("bob", slow); err != nil {
		t.Fatalf("another caller: %v", err)
	}
	if err := rates.Allow("alice", Limit{Rate: 10, Burst: 5}); err != nil {
		t.Fatalf("a new limit: %v", err)
	}
	first, second := rates.Limiter("alice", slow), rates.Limiter("alice", slow)
	if first != second {
		t.Fatal("the same caller and limit got two buckets")
	}
	if err := rates.Allow("carol", Limit{Rate: 1, Burst: 0}); err == nil {
		t.Fatal("a bucket with no burst allowed a request")
	}
}

// TestWaitN checks waits larger than the burst, an unlimited bucket, and a
// cancelled wait.
func TestWaitN(t *testing.T) {
	limiter := rate.NewLimiter(rate.Limit(1<<20), 1<<10)
	if err := WaitN(t.Context(), limiter, 64<<10); err != nil {
		t.Fatalf("64 KiB in 1 KiB steps: %v", err)
	}
	if err := WaitN(t.Context(), rate.NewLimiter(rate.Inf, 0), 1<<30); err != nil {
		t.Fatalf("unlimited: %v", err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := WaitN(cancelled, rate.NewLimiter(1, 1), 10); err == nil {
		t.Fatal("a cancelled wait succeeded")
	}
	if err := WaitN(t.Context(), rate.NewLimiter(1, 0), 1); err == nil {
		t.Fatal("a wait on a limiter with no burst succeeded")
	}
}

// TestOnOccupancy checks that the hook sees every change.
func TestOnOccupancy(t *testing.T) {
	var seen []Occupancy
	pool := NewSlots[string](
		&NewSlotsArgs{Name: "uploads", Slots: 2, OnOccupancy: func(occupancy Occupancy) {
			seen = append(seen, occupancy)
		}},
	)
	release, err := pool.Acquire(t.Context(), "alice")
	if err != nil {
		t.Fatal(err)
	}
	release()
	release()
	if len(seen) != 3 || seen[0].Reserved != 0 || seen[1].Reserved != 1 ||
		seen[1].ReservingCallers != 1 || seen[2].Reserved != 0 || seen[2].Slots != 2 {
		t.Fatalf("occupancy seen %+v, want empty, one slot reserved, empty again", seen)
	}
}

func BenchmarkAcquireRelease(b *testing.B) {
	pool := NewSlots[int](&NewSlotsArgs{Name: "requests", Slots: 64})
	ctx := context.Background()
	b.RunParallel(func(parallel *testing.PB) {
		caller := 0
		for parallel.Next() {
			caller++
			release, err := pool.Acquire(ctx, caller%16)
			if err != nil {
				continue
			}
			release()
		}
	})
}

func BenchmarkRatesAllow(b *testing.B) {
	rates, err := NewRates[int](nil)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(rates.Close)
	limit := Limit{Rate: rate.Inf}
	b.RunParallel(func(parallel *testing.PB) {
		caller := 0
		for parallel.Next() {
			caller++
			_ = rates.Allow(caller%1024, limit)
		}
	})
}

// TestRatesClose checks that Close stops the goroutine that drops idle
// buckets and waits for it: synctest.Test fails if any goroutine the bubble
// started is still running when the test returns. A second Close does
// nothing.
func TestRatesClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rates, err := NewRates[string](nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := rates.Allow("alice", Limit{Rate: 1, Burst: 1}); err != nil {
			t.Fatalf("Allow = %v, want nil", err)
		}
		rates.Close()
		rates.Close()
	})
}

// TestRatesIdleExpiry checks that a bucket used within IdleExpiry is the
// same bucket, and one idle longer is dropped and made again, full.
func TestRatesIdleExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rates, err := NewRates[string](&NewRatesArgs{IdleExpiry: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		defer rates.Close()
		limit := Limit{Rate: rate.Every(time.Hour), Burst: 1}
		first := rates.Limiter("alice", limit)
		if !first.Allow() {
			t.Fatal("a new bucket refused its first token, want it full")
		}
		time.Sleep(30 * time.Second)
		if again := rates.Limiter("alice", limit); again != first {
			t.Fatal("a bucket used within IdleExpiry was made again, want the same bucket")
		}
		time.Sleep(2 * time.Minute)
		rates.buckets.CleanUp()
		fresh := rates.Limiter("alice", limit)
		if fresh == first {
			t.Fatal("a bucket idle past IdleExpiry was kept, want it dropped")
		}
		if !fresh.Allow() {
			t.Fatal("a remade bucket refused its first token, want it full")
		}
	})
}

// TestRatesMaximumBuckets checks that the cache holds no more than
// MaximumBuckets buckets once its pending work is done, however many callers
// there were.
func TestRatesMaximumBuckets(t *testing.T) {
	const maximumBuckets = 100
	rates, err := NewRates[int](&NewRatesArgs{MaximumBuckets: maximumBuckets})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rates.Close)
	limit := Limit{Rate: 1, Burst: 1}
	for caller := range 10 * maximumBuckets {
		rates.Limiter(caller, limit)
	}
	rates.buckets.CleanUp()
	if stored := rates.buckets.EstimatedSize(); stored > maximumBuckets {
		t.Fatalf("%d buckets stored after 1,000 callers, want at most %d", stored, maximumBuckets)
	}
}
