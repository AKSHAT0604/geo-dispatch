# Benchmarks

All numbers here are measured, not estimated. Each row records the exact command used
to produce it, the machine it ran on, and what it was measured against, so results are
reproducible on demand. Numbers below are explicitly labeled by what backed them - do
not read a dev-machine microbenchmark number as the production-scale target.

## Machine spec

- CPU: AMD Ryzen 9 7900X, 12 cores / 24 threads
- RAM: 31 GB
- OS: Windows 11 Pro 10.0.26200
- Go: 1.25.0 (windows/amd64)

## Status

`loadgen` drives synthetic load directly against the matching and offer pipeline over a
live Redis (see [docs/DECISIONS.md](DECISIONS.md) and `cmd/loadgen`). The results below
were captured against an in-process `miniredis` instance on loopback, standing in for
Redis while this environment's Docker/WSL2 setup was still being completed - **these
are dev-machine sanity-check numbers, not the target-scale benchmark** the project's
success criteria call for (50,000+ drivers, sustained 1,000+ matches/sec, network-
attached Redis and Kafka via `docker-compose`). They exist to prove the pipeline is
correct and stable under concurrent load, not to be quoted as production numbers.

The full docker-compose stack (`make up`) and `make bench` are what produce the
real target-scale numbers; that run is still pending in this environment and should be
done before using any throughput/latency figure here on a resume.

## Preliminary results (in-process, miniredis, not network-attached)

| Metric | Value | Command | Date |
|---|---|---|---|
| Drivers simulated | 3,000 | `go run ./cmd/loadgen --drivers=3000 --rider-rate=60 --duration=30s --accept-delay=1s --ping-interval=3s --redis-addr=<miniredis>` | 2026-07-29 |
| Resolved requests | 1,751 | same run | 2026-07-29 |
| Matched | 1,575 (89.9%) | same run | 2026-07-29 |
| Unfulfilled | 176 (10.1%) | same run | 2026-07-29 |
| Request errors | 0 | same run | 2026-07-29 |
| Throughput | 52.5 matches/sec | same run | 2026-07-29 |
| p50 match latency | 1.11s | same run | 2026-07-29 |
| p95 match latency | 3.33s | same run | 2026-07-29 |
| p99 match latency | 6.36s | same run | 2026-07-29 |

Latency here is dominated by the offer window and simulated driver `--accept-delay`
(1s), not matching cost - `loadgen` measures full request-to-resolution time, including
however many offer/reoffer rounds a trip needed. It is not a proxy for raw candidate
search speed; `dispatch_ring_expansion` (see `internal/metrics`) is the metric to watch
for that once Prometheus is scraping a running cluster.

## Bugs this run found

Running load at increasing scale caught three real concurrency bugs before any of them
shipped to a resume bullet:

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

## Reproducing target-scale numbers

Once `make up` brings up Kafka, Redis, and Prometheus:

```bash
make build
go run ./cmd/loadgen --drivers=50000 --rider-rate=1000 --duration=60s
```

Sweep driver population (10k, 50k, 100k) and record p50/p95/p99 and sustained
matches/sec at each. Ring rebalance time (Phase 5's node-failure scenario) should be
captured separately: run 4 `disco` nodes gossiping via `internal/membership`, kill one
under load, and time how long `internal/hashring` takes to converge and matching to
resume for the cells that node owned.
