package callerlimit

import (
	"context"
	"errors"
	"testing"
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
	slow := Limit{Rate: 1, Burst: 2}
	for range 2 {
		if err := rates.Allow("alice", slow); err != nil {
			t.Fatalf("within the burst: %v", err)
		}
	}
	err = rates.Allow("alice", slow)
	overload, ok := errors.AsType[*OverloadError](err)
	if !ok || !errors.Is(err, ErrOverloaded) || overload.Pool != "requests" || overload.Reason != ReasonRate ||
		overload.RetryAfter <= 0 || overload.RetryAfter > time.Second {
		t.Fatalf("past the burst: %v", err)
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
	pool := NewSlots[string](&NewSlotsArgs{Name: "uploads", Slots: 2, OnOccupancy: func(occupancy Occupancy) {
		seen = append(seen, occupancy)
	}})
	release, err := pool.Acquire(t.Context(), "alice")
	if err != nil {
		t.Fatal(err)
	}
	release()
	release()
	if len(seen) != 3 || seen[0].Reserved != 0 || seen[1].Reserved != 1 || seen[1].ReservingCallers != 1 ||
		seen[2].Reserved != 0 || seen[2].Slots != 2 {
		t.Fatalf("occupancy seen %+v", seen)
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
	limit := Limit{Rate: rate.Inf}
	b.RunParallel(func(parallel *testing.PB) {
		caller := 0
		for parallel.Next() {
			caller++
			_ = rates.Allow(caller%1024, limit)
		}
	})
}
