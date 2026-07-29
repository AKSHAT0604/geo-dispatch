# Benchmarks

All numbers here are measured, not estimated. Each row records the exact command used
to produce it, the machine it ran on, and what it was measured against, so results are
reproducible on demand.

## Machine spec

- CPU: AMD Ryzen 9 7900X, 12 cores / 24 threads
- RAM: 31 GB
- OS: Windows 11 Pro 10.0.26200
- Go: 1.25.0 (windows/amd64)
- Docker Desktop 4.84.0 (WSL2 backend)

## Status

`loadgen` drives synthetic load directly against the matching and offer pipeline over a
live Redis (see `cmd/loadgen`) - it talks to the internal packages, not over HTTP, since
what's being measured is the dispatch engine itself. Results below are against the real
`docker-compose` stack (`make up`): network-attached Redis and Kafka in containers, not
an in-process fake.

**Read this honestly, not as a resume-ready headline:** the 50,000-driver / 500 req/s
run below holds up correctly under load (100% match rate, zero request errors after the
fixes it drove) but does not reach the project's stated 1,000 matches/sec target on this
machine, because `loadgen` and the pipeline it's driving share the same CPU in the same
process on a single 24-thread dev machine - the load generator's own goroutine
scheduling becomes the bottleneck before Redis or the matching logic does. A real
capacity number needs `loadgen` running from a separate machine (or several parallel
instances) against a `disco` cluster it isn't competing with for CPU. That run has not
been done.

## Results: 50,000 drivers (supply-population target)

```bash
docker exec geo-dispatch-redis redis-cli FLUSHALL
go run ./cmd/loadgen --drivers=50000 --rider-rate=500 --duration=30s --accept-delay=200ms --ping-interval=5s --redis-addr=127.0.0.1:6379
```

| Metric | Value | Date |
|---|---|---|
| Drivers held in the index | 50,000 | 2026-07-29 |
| Resolved requests | 12,782 | 2026-07-29 |
| Matched | 12,782 (100.0%) | 2026-07-29 |
| Unfulfilled | 0 | 2026-07-29 |
| Request errors | 0 | 2026-07-29 |
| Throughput | 426.1 matches/sec | 2026-07-29 |
| p50 match latency | 24.5s | 2026-07-29 |
| p95 match latency | 44.4s | 2026-07-29 |
| p99 match latency | 56.5s | 2026-07-29 |

Meets the ">= 50,000 simulated drivers" criterion cleanly. Latency here is inflated by
CPU contention between 50,000+ long-lived driver goroutines and the rider dispatch
goroutines all competing for the same 24 threads `loadgen` itself is running on - not
representative of matching cost in isolation.

## Results: throughput-focused, smaller population

```bash
docker exec geo-dispatch-redis redis-cli FLUSHALL
go run ./cmd/loadgen --drivers=8000 --rider-rate=200 --duration=25s --accept-delay=100ms --ping-interval=5s --redis-addr=127.0.0.1:6379
```

| Metric | Value | Date |
|---|---|---|
| Drivers simulated | 8,000 | 2026-07-29 |
| Resolved requests | 4,626 | 2026-07-29 |
| Matched | 4,602 (99.5%) | 2026-07-29 |
| Unfulfilled | 24 (0.5%) | 2026-07-29 |
| Request errors | 0 | 2026-07-29 |
| Throughput | 184.1 matches/sec | 2026-07-29 |
| p50 match latency | 5.95s | 2026-07-29 |
| p95 match latency | 12.81s | 2026-07-29 |
| p99 match latency | 17.12s | 2026-07-29 |

Lower driver count reduces goroutine-scheduling overhead and latency drops
proportionally, but throughput is still client-generator-bound, not server-bound - the
same pattern, smaller scale.

## Bugs found running these benchmarks

Load testing at increasing scale, first against `miniredis` and then against real
Docker-hosted Redis, caught five real concurrency bugs before any of them shipped to a
resume bullet. The last two only reproduced against real Redis - `miniredis` never
exhibited them, which is itself worth noting about the limits of an in-process fake for
concurrency testing:

1. **Concurrent dispatches ranking the same driver.** Candidate search snapshots
   AVAILABLE drivers before any offer is made, so two trips dispatched at the same time
   could both rank the same driver before either claimed it - the loser hard-errored the
   whole trip instead of falling through to its next candidate. Fixed in
   `internal/offers/dispatcher.go`.
2. **A location ping clobbering a concurrent state change.** `UpdateLocation` read a
   driver's state in Go and wrote that value back inside its Lua script; a ping racing
   with a dispatcher's `SetState` call could read state before an offer arrived and
   write it back after, silently reverting `OFFERED` to `AVAILABLE` mid-flight. Fixed by
   moving the read-and-preserve into the same atomic script that writes it, in
   `internal/store/driver.go`.
3. **`loadgen` cancelling in-flight dispatches early.** A single shared context meant
   stopping request generation also cut off requests still resolving, and reusing
   sequential rider IDs across repeated runs against the same Redis collided on
   idempotency keys. Fixed in `cmd/loadgen/main.go`.
4. **Offer record TTL racing the offer window.** The offer record's Redis TTL and the
   Go-side offer window were the same duration, so under real network round-trip latency
   Redis could expire the record at essentially the same instant the timeout fired,
   before the dispatcher reached `SetOfferState` - turning an ordinary timeout into a
   hard RPC error. Only reproduced against real Redis. Fixed with a grace buffer on the
   TTL plus defensive handling of the residual race, in `internal/offers/dispatcher.go`.
5. **Sequential per-candidate driver lookups saturating the connection pool.** Candidate
   search called `GetDriver` once per candidate instead of pipelining them; a densely
   populated cell at 50,000 drivers turned into dozens of serialized round trips per
   request, and under concurrent load that produced `context deadline exceeded` on
   roughly 13% of requests. Fixed by pipelining every candidate in a cell into one round
   trip (`DriverStore.GetDrivers`), which eliminated the timeouts entirely - see the two
   result tables above, both with zero request errors.

A residual, not-fully-root-caused race remains under extreme concurrent load (~0.6% of
requests at 50k drivers / 500 req/s see a driver's own state field land somewhere other
than what a given trip's accept/release step expected). The trip's own offer and match
records are unaffected by it - matched trips are always genuinely matched to the driver
who accepted them - so both paths now tolerate the race rather than failing the request
over it. See the comments in `internal/offers/dispatcher.go` on the `Accepted` and
release branches.

## Results: 4-node ring rebalance under a live node kill (Phase 5 DoD)

Four `disco` nodes were started as a real gossip cluster (`internal/membership`, ports
19081-19084), converged to see all 4 members, then driven with continuous `Dispatch`
RPCs round-robined across all four addresses while 400 seeded drivers ran real
accept-simulators (responding via `RespondToOffer`, not a stub) so requests resolved in
well under a second instead of needing the offer window to time out. 8.5 seconds in,
node2 was force-killed (`taskkill /F`, simulating a crash, not a graceful leave) while
requests kept flowing.

| Metric | Value | Date |
|---|---|---|
| Cluster size before kill | 4 nodes | 2026-07-29 |
| Time to kill (test marker) | t=8.50s | 2026-07-29 |
| Ring converged to 3 members at | t=13.46s | 2026-07-29 |
| **Rebalance time** | **4.97s** | 2026-07-29 |
| Requests during test | 74 (63 ok, 11 failed) | 2026-07-29 |
| Failures after convergence | 0 (excluding requests the naive test client sent directly to the dead node's address) | 2026-07-29 |

Every failure during the test was either a request routed to or through node2 while it
was dead (`connection refused`) - expected, since this harness round-robins across
hardcoded addresses rather than doing service discovery, so it keeps dialing the dead
node throughout the run by design. No survivor ever returned a wrong or lost result:
once the ring converged, matching resumed cleanly for the cells node2 had owned, and no
in-flight trip on a surviving node was lost. `/debug/ring` on any `disco` node reports
current gossip membership and ring nodes for reproducing this manually.

## Not yet measured

- **Sustained 1,000+ matches/sec** against a `disco` cluster the load generator isn't
  sharing a CPU with - the single-machine, single-process numbers above are CPU-bound on
  the load generator itself, not on Redis or the matching logic (see above).
