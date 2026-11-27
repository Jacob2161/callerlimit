# Changelog

This module follows semantic versioning. Before v1, a minor version may
change the API; each such change says what callers must do.

## Unreleased

This is a breaking change and ships as v0.2.0.

- `Slots.SlotsByCaller` is now `Slots.CallerSlots`, with the same result.
  Rename the call.
- `Rates.Close` stops the goroutine that drops idle buckets and waits for
  it. Call it when the server stops; until now that goroutine ran for the
  life of the process.
- Every exported constant, field, and method has its own doc comment, and
  DESIGN.md explains how slots and buckets work.
- CI also checks formatting, `go mod tidy -diff`, and `govulncheck`.

## v0.1.0 (2026-11-27)

First release.

- `Slots` bounds how many requests use a resource at once and hands
  released slots to waiting callers in turns, with bounded waits and
  queues and an `*OverloadError` that says when to retry.
- `Rates` holds a token bucket per caller and limit that expires when
  idle, and `WaitN` waits for byte counts larger than a bucket's burst.
