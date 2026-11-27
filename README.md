# callerlimit

[![Go Reference](https://pkg.go.dev/badge/github.com/jacob2161/callerlimit.svg)](https://pkg.go.dev/github.com/jacob2161/callerlimit)
[![CI](https://github.com/jacob2161/callerlimit/actions/workflows/ci.yml/badge.svg)](https://github.com/jacob2161/callerlimit/actions/workflows/ci.yml)

Fair per-caller limits for servers: concurrency slots handed to waiting
callers in turns, so one busy caller cannot starve the rest, and per-caller
token buckets that expire when idle.

```bash
go get github.com/jacob2161/callerlimit
```

## Usage

```go
uploads := callerlimit.NewSlots[string](&callerlimit.NewSlotsArgs{Name: "uploads", Slots: 8})

release, err := uploads.Acquire(ctx, apiKeyID)
if overload, ok := errors.AsType[*callerlimit.OverloadError](err); ok {
	w.Header().Set("Retry-After", strconv.Itoa(max(1, int(overload.RetryAfter.Seconds()))))
	http.Error(w, "too many uploads at once", http.StatusTooManyRequests)
	return
}
if err != nil {
	return // the request's context ended
}
defer release()

requests, err := callerlimit.NewRates[string](&callerlimit.NewRatesArgs{Name: "requests"})
defer requests.Close()
if err := requests.Allow(apiKeyID, callerlimit.Limit{Rate: 50, Burst: 50}); err != nil {
	// *callerlimit.OverloadError with the delay until the next token
}
```

## Slots

A semaphore that is fair across callers, not across requests:

- A request takes a free slot when nobody waits. Otherwise it joins its
  caller's queue, and callers with queued requests wait in a ring: a released
  slot goes to the oldest request of the caller at the front, which then moves
  to the back.
- A lone caller may use every slot; a caller that arrives later gets one as
  soon as each caller ahead of it had a turn.
- Waits are bounded (250 ms by default) and so are queues, per caller and in
  total, so overload is refused at once with a retry delay instead of piling up.
- `OnOccupancy` reports slots, reserved slots, queued requests, and callers,
  for gauges.

About 200 ns per acquire and release under parallel load.

## Rates

A `golang.org/x/time/rate` bucket per caller and limit, in a bounded cache
(otter's W-TinyLFU) that drops idle buckets; a dropped bucket comes back
full. A caller whose limit changes gets a fresh bucket, while requests
allowed under the old one keep theirs. `WaitN` waits for more tokens than the
burst in burst-sized steps, for byte rates. The cache runs a goroutine that
drops idle buckets; `Close` stops it when the server stops.

[DESIGN.md](DESIGN.md) explains how slots are handed out and why.

Tested on the two newest Go releases. Licensed under the MIT license.
