package callerlimit

import (
	"context"
	"errors"
	"time"

	otter "github.com/maypok86/otter/v2"
	"golang.org/x/time/rate"
)

const (
	// DefaultMaximumBuckets is the most buckets a Rates holds when
	// NewRatesArgs.MaximumBuckets is zero or negative.
	DefaultMaximumBuckets = 100_000
	// DefaultIdleExpiry is how long an unused bucket is held when
	// NewRatesArgs.IdleExpiry is zero or negative.
	DefaultIdleExpiry = time.Hour
)

// Limit is a token bucket's configuration.
type Limit struct {
	// Rate is the tokens added a second. rate.Inf allows everything.
	Rate rate.Limit
	// Burst is the most tokens the bucket holds, and so the most requests
	// allowed at once after an idle spell.
	Burst int
}

// NewRatesArgs configures NewRates.
type NewRatesArgs struct {
	// Name names the buckets in refusals.
	Name string
	// MaximumBuckets bounds the buckets held, one per caller and limit; past
	// it the cache (otter's W-TinyLFU) drops the ones it expects least to be
	// used again, and may briefly hold a few more. Zero or negative uses
	// DefaultMaximumBuckets.
	MaximumBuckets int
	// IdleExpiry drops a bucket unused this long. Zero or negative uses
	// DefaultIdleExpiry. A dropped bucket is made again, full, on its
	// caller's next request.
	IdleExpiry time.Duration
}

// Rates holds a token bucket for each caller and limit, created full on first
// use and dropped when idle. A caller whose limit changes gets a fresh bucket
// under the new limit, while requests allowed under the old one keep using
// theirs. It is safe for concurrent use. Its cache runs a goroutine that
// drops idle buckets; Close stops it.
type Rates[K comparable] struct {
	name    string
	buckets *otter.Cache[limitedCaller[K], *rate.Limiter]
}

// limitedCaller names a bucket: its caller and the limit it was made with.
type limitedCaller[K comparable] struct {
	caller K
	limit  Limit
}

// NewRates returns per-caller buckets configured by args, which may be nil.
func NewRates[K comparable](args *NewRatesArgs) (*Rates[K], error) {
	if args == nil {
		args = &NewRatesArgs{}
	}
	maximumBuckets := args.MaximumBuckets
	if maximumBuckets <= 0 {
		maximumBuckets = DefaultMaximumBuckets
	}
	idleExpiry := args.IdleExpiry
	if idleExpiry <= 0 {
		idleExpiry = DefaultIdleExpiry
	}
	buckets, err := otter.New(&otter.Options[limitedCaller[K], *rate.Limiter]{
		MaximumSize:      maximumBuckets,
		ExpiryCalculator: otter.ExpiryAccessing[limitedCaller[K], *rate.Limiter](idleExpiry),
	})
	if err != nil {
		return nil, err
	}
	return &Rates[K]{name: args.Name, buckets: buckets}, nil
}

// Limiter returns caller's bucket under limit, creating it full on first
// use. Buckets are never reconfigured: a new limit gets a new bucket. A
// caller that does not equal itself, such as NaN, gets a new bucket each
// time; Allow refuses one.
func (r *Rates[K]) Limiter(caller K, limit Limit) *rate.Limiter {
	key := limitedCaller[K]{caller: caller, limit: limit}
	bucket, found := r.buckets.GetIfPresent(key)
	if found {
		return bucket
	}
	fresh := rate.NewLimiter(limit.Rate, limit.Burst)
	bucket, inserted := r.buckets.SetIfAbsent(key, fresh)
	if !inserted {
		return bucket
	}
	return fresh
}

// Close stops the goroutine that drops idle buckets and returns once it has
// stopped. Closing twice does nothing. The buckets stay usable, but idle ones
// are then dropped only as the cache fills, so close a Rates when its server
// stops.
func (r *Rates[K]) Close() {
	r.buckets.StopAllGoroutines()
}

// Allow takes one token from caller's bucket, or returns an *OverloadError
// whose RetryAfter is when one will be available. It never waits.
func (r *Rates[K]) Allow(caller K, limit Limit) error {
	if caller != caller {
		return ErrInvalidCaller
	}
	reservation := r.Limiter(caller, limit).Reserve()
	if !reservation.OK() {
		return &OverloadError{Pool: r.name, Reason: ReasonRate, RetryAfter: time.Second}
	}
	if delay := reservation.Delay(); delay > 0 {
		reservation.Cancel()
		return &OverloadError{Pool: r.name, Reason: ReasonRate, RetryAfter: delay}
	}
	return nil
}

// WaitN waits for n tokens from limiter in steps no larger than its burst, so
// a request larger than the burst, such as a large upload counted in bytes,
// slows down instead of failing. A limiter of rate.Inf never waits.
func WaitN(ctx context.Context, limiter *rate.Limiter, n int) error {
	if limiter.Limit() == rate.Inf {
		return nil
	}
	burst := limiter.Burst()
	if burst <= 0 {
		if n <= 0 {
			return nil
		}
		return errors.New("waiting on a limiter with no burst")
	}
	for n > 0 {
		step := min(n, burst)
		if err := limiter.WaitN(ctx, step); err != nil {
			return err
		}
		n -= step
	}
	return nil
}
