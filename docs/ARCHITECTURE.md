# Architecture

```
                    +-------------------+
   driver pings --> |  supply-service   | --+
   (HTTP)           |  (state machine)  |   |
                    +-------------------+   | gRPC
                                            v
                    +-------------------+  +---------------------+
   ride request --> |  demand-service   |->|   disco (matcher)   |<--+ gossip
   (HTTP)           |  (idempotent)     |  |  k-ring expansion   |   | (SWIM,
                    +-------------------+  |  ranking + offers   |<--+ memberlist)
                                           |  hash-ring routing   |
                                           +----------+----------+
                                                      |
                            +-------------------------+------------+
                            v                                      v
                    +---------------+                    +------------------+
                    |     Redis     |                    |      Kafka       |
                    | cell -> drivers|                   | trip.lifecycle   |
                    | offer state    |                   | driver.location  |
                    | trip state     |                   | offer.events     |
                    | surge multiplier|                  +--------+---------+
                    +---------------+                             |
                            ^                          +----------+----------+
                            |                           v                     v
                            |                 +--------------------+  +--------------+
                            +-----------------+| surge-aggregator   |  |   gateway    |
                                               | supply/demand ratio|  | WS -> map UI |
                                               +--------------------+  +--------------+
```

Every `disco` instance is a node on a consistent hash ring over H3 cell IDs
(`internal/hashring`, kept in sync with cluster membership by `internal/membership`'s
SWIM gossip). A request for a cell this node doesn't own is forwarded over gRPC to
whichever node does (`internal/router`), rather than answered incorrectly. Redis is the
only place any of this state actually lives - every service reads it fresh on every
call, so there is no per-node cache to rebuild after a node joins, leaves, or crashes;
see [docs/DECISIONS.md](DECISIONS.md#7-why-is-redis-the-source-of-truth-and-node-state-only-a-cache).

## Services

- **supply-service** (`cmd/supply`) is drivers' HTTP front door: location pings, polling
  for a currently open offer, and responding to it. It holds no matching logic - offer
  responses are forwarded to disco over gRPC, which routes them to whichever node is
  actually running that trip's dispatch loop.
- **demand-service** (`cmd/demand`) is riders' HTTP front door: one endpoint that
  assigns nothing itself, just forwards to disco's `Dispatch` RPC with the rider's
  `Idempotency-Key`.
- **disco** (`cmd/disco`) is the only component that decides assignments: candidate
  search via H3 k-ring expansion, ranking by estimated travel time, the offer/reoffer
  loop, hash-ring routing between nodes, and a periodic reconciliation sweep that
  resumes any trip left mid-flight by a node that crashed.
- **surge-aggregator** (`cmd/surge`) consumes `driver.location` and `trip.lifecycle`,
  maintaining a rolling supply/demand ratio per H3 cell and writing a multiplier back to
  Redis for disco to read when quoting a trip. Pricing never touches matching.
- **gateway** (`cmd/gateway`) consumes `trip.lifecycle` and rebroadcasts it over
  WebSocket to a Leaflet + H3 map, so every dispatch decision is visible live.

## Packages

| Package | Responsibility |
|---|---|
| `internal/h3index` | H3 cell math and k-ring helpers |
| `internal/hashring` | Hand-rolled consistent hashing with virtual nodes |
| `internal/membership` | `memberlist` gossip wrapper, gossips each node's gRPC address |
| `internal/router` | Routes a request to the node owning its cell, local or gRPC |
| `internal/statemachine` | Driver, trip, and offer state machines |
| `internal/matching` | Candidate selection, ETA ranking |
| `internal/offers` | Offer lifecycle, reoffer loop, handoff reconciliation |
| `internal/events` | Kafka producers and event payload types |
| `internal/store` | Redis access layer (drivers, trips, offers, surge) |
| `internal/surge` | Sliding-window supply/demand ratio and multiplier curve |
| `internal/metrics` | Prometheus collectors |
| `internal/gateway` | WebSocket broadcast hub |
| `api/proto` | `DispatchService`: `Dispatch` and `RespondToOffer` |

See [docs/DECISIONS.md](DECISIONS.md) for the rationale behind each major design
choice, and [docs/BENCHMARKS.md](BENCHMARKS.md) for measured results.
