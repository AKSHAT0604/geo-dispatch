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

Direct assignment assumes the top-ranked driver will always accept, which isn't true in
practice: a driver's app can be backgrounded, their connection can drop, or they can
simply decline. Without a timeout, a single unresponsive driver blocks the trip
indefinitely - dispatch has no way to notice and move on. Modeling the offer as its own
state machine (PENDING -> ACCEPTED | DECLINED | TIMED_OUT) makes "no response" a first-
class outcome with a bounded wait (the offer window) rather than an unhandled case, and
lets the dispatcher reoffer to the next-ranked candidate with backoff instead of
retrying the same driver or giving up immediately. Exhausting the round cap marks the
trip UNFULFILLED explicitly - failing to match is a valid, observable outcome, not a
hang. The rejected alternative is offering the whole ranked list to every driver
simultaneously and taking the first accept, which trades a clean explicit lifecycle for
a race that's harder to reason about and wastes offers on drivers who were never going
to be reached anyway.

## 6. Why idempotency keys on trip creation?

Mobile clients retry aggressively on flaky networks, and a request that appears to time
out from the client's point of view may well have succeeded on the server - the retry
is indistinguishable from a genuinely new request unless the client marks it. Without
an idempotency key, that retry creates a second trip, and the rider ends up with two
drivers converging on them. `CreateTrip` performs the existence check and the creation
in a single Lua script specifically because a plain GET-then-SET has a race window: two
copies of the same retry, arriving close together, can both pass the GET before either
writes, producing exactly the duplicate the key was meant to prevent. The 24-hour TTL
on the key is generous enough to outlive any realistic retry storm without holding the
deduplication record forever.

## 7. Why is Redis the source of truth and node state only a cache?

_To be answered once Phase 5 (sharding) is complete._
