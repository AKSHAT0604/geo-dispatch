# Design Decisions

Each answer below names the alternative that was rejected and why. These are written as
interview answers, not as documentation prose.

## 1. Why hexagons rather than squares or geohash?

A hexagonal grid gives every cell six neighbours, all at the same distance from its
center. A square grid gives four edge-adjacent neighbours at distance 1 and four
corner-adjacent neighbours at distance sqrt(2); a k-ring search over squares therefore
expands unevenly depending on direction, which distorts the pickup radius the search
is meant to represent. Geohash cells are rectangular for the same reason, and its
prefix-based structure also has cells that jump size unpredictably at the equator and
grid boundaries. H3, Uber's own library, was picked specifically because it's the
production choice for the workload being reproduced: rest of the system's tooling
(k-ring expansion, cell-to-parent aggregation for the surge window) assumes hexagons
and would need reimplementing on any square-based scheme. The rejected alternative
is a quadtree/geohash grid keyed by string prefix.

## 2. Why shard by cell ID rather than by driver ID or trip ID?

_To be answered once Phase 5 (sharding) is complete._

## 3. Why consistent hashing rather than modulo hashing?

_To be answered once Phase 5 (hash ring) is complete._

## 4. Why rank by travel time rather than straight-line distance?

_To be answered once Phase 2 (matching engine) is complete._

## 5. Why an offer state machine with timeouts rather than direct assignment?

_To be answered once Phase 3 (offer lifecycle) is complete._

## 6. Why idempotency keys on trip creation?

_To be answered once Phase 3 (offer lifecycle) is complete._

## 7. Why is Redis the source of truth and node state only a cache?

_To be answered once Phase 5 (sharding) is complete._
