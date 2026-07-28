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

Straight-line distance is a poor proxy for pickup time: a driver 400m away across a
highway with no crossing for a kilometer is farther, in the metric that actually
matters to the rider, than one 800m away on a direct surface street. Ranking by ETA
means the number being optimized is the number the rider experiences. The rejected
alternative - sorting candidates by haversine distance - is what `HaversineEstimator`
implements as a baseline, precisely so it can be swapped for `CongestionAwareEstimator`
behind the same `ETAEstimator` interface without touching the ranking code that
consumes it. That interface is also the intended seam for a learned ETA model (a
separate project) to plug in later without another rewrite of `internal/matching`.

Ranking also isn't pure ETA: a small idle-time discount lets a driver who has been
waiting significantly longer occasionally beat one that's marginally closer. Without
it, the same centrally-located drivers absorb every trip while drivers idling at the
edge of a cell's coverage never get offered one - real dispatch has to balance rider
wait time against driver earnings equity, not just minimize the former.

## 5. Why an offer state machine with timeouts rather than direct assignment?

_To be answered once Phase 3 (offer lifecycle) is complete._

## 6. Why idempotency keys on trip creation?

_To be answered once Phase 3 (offer lifecycle) is complete._

## 7. Why is Redis the source of truth and node state only a cache?

_To be answered once Phase 5 (sharding) is complete._
