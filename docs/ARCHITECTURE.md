# Architecture

```
                    +-------------------+
   driver pings --> |  supply-service   | --+
                    |  (state machine)  |   |
                    +-------------------+   |
                                            v
                    +-------------------+  +---------------------+
   ride request --> |  demand-service   |->|   disco (matcher)   |
                    |  (idempotent)     |  |  k-ring expansion   |
                    +-------------------+  |  ranking + offers   |
                                           +----------+----------+
                                                      |
                            +-------------------------+------------+
                            v                                      v
                    +---------------+                    +------------------+
                    |     Redis     |                    |      Kafka       |
                    | cell -> drivers|                   | trip.lifecycle   |
                    | offer state    |                   | driver.location  |
                    +---------------+                    +--------+---------+
                                                                  |
                                                                  v
                                                        +--------------------+
                                                        | surge-aggregator   |
                                                        | supply/demand ratio|
                                                        +--------------------+

   shard-coordinator: consistent hash ring over H3 cell IDs, memberlist gossip
```

## Services

- **supply-service** (`cmd/supply`) owns driver state. Every driver is a state machine:
  `OFFLINE -> AVAILABLE -> OFFERED -> EN_ROUTE -> ON_TRIP -> AVAILABLE`. Illegal
  transitions are rejected, not silently accepted.
- **demand-service** (`cmd/demand`) accepts ride requests, assigns an idempotency key,
  and persists trip intent before any matching begins.
- **disco** (`cmd/disco`) runs the matching loop. It is the only component that decides
  assignments: candidate search via H3 k-ring expansion, ranking by estimated travel
  time, and offer lifecycle management.
- **surge-aggregator** (`cmd/surge`) is a Kafka consumer computing a rolling
  supply/demand ratio per H3 cell and writing a surge multiplier back to Redis.
- **shard-coordinator** (`internal/hashring` + `internal/membership`) owns the
  consistent hash ring over H3 cell IDs and answers "which node owns this cell".

## Packages

| Package | Responsibility |
|---|---|
| `internal/h3index` | H3 cell math and k-ring helpers |
| `internal/hashring` | Hand-rolled consistent hashing with virtual nodes |
| `internal/membership` | `memberlist` gossip wrapper |
| `internal/statemachine` | Driver and trip state machines |
| `internal/matching` | Candidate selection, ETA ranking |
| `internal/offers` | Offer lifecycle, timeouts, reoffer |
| `internal/events` | Kafka producers and consumers |
| `internal/store` | Redis access layer |
| `internal/metrics` | Prometheus collectors |

This document is filled in incrementally as each phase lands; see `docs/DECISIONS.md`
for the rationale behind each major design choice.
