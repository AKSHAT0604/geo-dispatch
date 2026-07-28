# geo-dispatch

A distributed rider-driver matching service modeled on the architecture of Uber's
DISCO dispatch system: supply and demand tracked as separate services with explicit
state machines, an H3 hexagonal spatial index used as the sharding key, and matching
implemented as a ranked search over candidates rather than a nearest-neighbour lookup.

**Status: in progress.** See the phase checklist below for what is built so far.

## Architecture

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full diagram and service
breakdown.

## Quickstart

```bash
make up      # start Kafka, Redis, Prometheus
make build   # compile supply, demand, disco, surge, loadgen
make test    # run the test suite
```

## Benchmarks

Headline numbers land in [docs/BENCHMARKS.md](docs/BENCHMARKS.md) once Phase 6 load
testing is complete.

## Design decisions

Every non-obvious architectural choice — hexagons over squares, sharding by cell
rather than driver, consistent hashing over modulo, ETA ranking over straight-line
distance, and more — is written up with its rejected alternative in
[docs/DECISIONS.md](docs/DECISIONS.md).

## Phase checklist

- [x] Phase 0 — scaffolding, infra, health checks
- [x] Phase 1 — H3 indexing, Redis driver location store
- [ ] Phase 2 — matching engine
- [ ] Phase 3 — offer lifecycle, idempotent trip creation
- [ ] Phase 4 — Kafka event pipeline, surge pricing
- [ ] Phase 5 — consistent hash ring, sharding, gossip membership
- [ ] Phase 6 — load generation, benchmarking
- [ ] Phase 7 — websocket gateway, map UI (optional)
