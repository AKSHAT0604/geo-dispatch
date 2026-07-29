# geo-dispatch

A distributed rider-driver matching service modeled on the architecture of Uber's
DISCO dispatch system: supply and demand tracked as separate services with explicit
state machines, an H3 hexagonal spatial index used as the sharding key, and matching
implemented as a ranked search over candidates rather than a nearest-neighbour lookup.

Built as five services around a shared Redis and Kafka backbone: **disco** owns every
match decision and shards itself across a consistent hash ring; **supply-service** and
**demand-service** are thin HTTP fronts for drivers and riders; **surge-aggregator**
computes a rolling supply/demand multiplier per cell from the same event stream; and a
**gateway** pushes trip lifecycle events to a live map over WebSocket.

## Architecture

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full diagram and service
breakdown.

## Quickstart

```bash
make up      # start Kafka, Redis, Prometheus (docker compose)
make build   # compile all five services
make run     # start disco, supply-service, demand-service, surge-aggregator, gateway
```

The map UI is then live at `http://localhost:8086`. Run `make stop` to tear the
services down (`make down` stops the infra containers separately).

Try it:

```bash
# A driver reports its position.
curl -X POST localhost:8081/drivers/driver-1/location \
  -d '{"lat":17.385,"lng":78.4867}'

# A rider requests a ride. Idempotency-Key makes a retry of this exact
# call return the same trip instead of creating a second one.
curl -X POST localhost:8082/trips \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{"rider_id":"rider-1","lat":17.385,"lng":78.4867}'

# The driver polls for what it's been offered, then responds.
curl localhost:8081/drivers/driver-1/offer
curl -X POST localhost:8081/drivers/driver-1/offer/respond -d '{"response":"ACCEPTED"}'
```

## Benchmarks

Measured against the real `docker-compose` stack: 50,000 drivers held with a 100% match
rate and zero request errors, and a 4-node cluster rebalancing in 4.97s after a live
node kill with no in-flight trip lost. Five concurrency bugs load testing caught and
fixed along the way - three only reproduced against real Docker-hosted Redis, never
against an in-process fake - are in [docs/BENCHMARKS.md](docs/BENCHMARKS.md), read
honestly rather than as a resume-ready headline (it says exactly what wasn't measured
too). Surge pricing's rise-and-decay curve under synthetic demand is in
[docs/SURGE.md](docs/SURGE.md).

## Design decisions

Every non-obvious architectural choice — hexagons over squares, sharding by cell
rather than driver, consistent hashing over modulo, ETA ranking over straight-line
distance, offer timeouts over direct assignment, idempotency keys, and Redis as the
sole source of truth — is written up with its rejected alternative in
[docs/DECISIONS.md](docs/DECISIONS.md).

## Phase checklist

- [x] Phase 0 — scaffolding, infra, health checks
- [x] Phase 1 — H3 indexing, Redis driver location store
- [x] Phase 2 — matching engine
- [x] Phase 3 — offer lifecycle, idempotent trip creation
- [x] Phase 4 — Kafka event pipeline, surge pricing (see [docs/SURGE.md](docs/SURGE.md))
- [x] Phase 5 — consistent hash ring, sharding, gossip membership
- [x] Phase 6 — load generation, benchmarking (see [docs/BENCHMARKS.md](docs/BENCHMARKS.md))
- [x] Phase 7 — websocket gateway, Leaflet + H3 map UI
