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

Matching is fundamentally a geographic query: find AVAILABLE drivers near a request's
origin. Sharding by cell ID means every driver a request could possibly match against
lives on the same node (or a small, deterministic set of neighbouring-cell nodes for
k-ring expansion near a shard boundary), so a single match can usually be answered
without a single cross-node call. Sharding by driver ID or trip ID instead would
scatter a request's candidate pool across every node in the cluster essentially at
random - every match would become a scatter-gather query, trading a fast local Redis
read for a fan-out RPC to every shard, for no benefit, since driver ID and trip ID carry
no relationship to what a request actually needs to search over.

## 3. Why consistent hashing rather than modulo hashing?

Modulo hashing (`node = hash(key) % N`) ties every key's owner to the current node
count: adding or removing a single node changes `N` and reshuffles the owner of nearly
every key, which for this system means nearly every H3 cell changes hands at once on
any scaling event or node failure - exactly the "kill one mid-load" scenario Phase 5's
definition of done stresses. Consistent hashing fixes node positions on a ring
independent of `N`; adding the Nth node only takes over the keys nearest its new
position on the ring, relocating roughly `1/N` of keys rather than nearly all of them
(`internal/hashring`'s test asserts this directly). Virtual nodes (150 replicas per
physical node here) exist because a single hash per physical node means that node's
share of the ring depends entirely on where that one hash landed - one node could end
up owning 5% of the ring and another 40% by chance. Many replicas per node average that
out to a roughly even split regardless of which physical node they belong to.

This is also the same property the author's Distributed File System project shards on,
which is deliberate: it is the concept this project is built to demonstrate a second,
independent application of.

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

Ownership of an H3 cell moves between nodes whenever the ring changes - a node joins,
leaves, or crashes - and whichever node owns a cell next must be able to serve correct
matches for it immediately, without a warm-up period where it doesn't yet know which
drivers are there. That's only possible if the data those decisions are based on
doesn't live solely in the memory of whichever node happened to own the cell before.
In this system every read - driver location, driver state, trip state, offer state -
already goes straight to Redis on every call; there is no per-node cache to invalidate
or rebuild in the first place, which is a stronger version of the same principle: if
node-local state doesn't exist, there's nothing for a handoff to get out of sync.

The one piece of real in-memory state is the goroutine actually running a trip's
reoffer loop and the channel it's waiting on for a driver's response - and that's
exactly what's lost if its node crashes. `Reconciler` (`internal/offers/reconcile.go`)
is what makes that recoverable: it finds trips left in Redis's `OFFERED` state with no
live offer and resumes them with a fresh match, so whichever node ends up owning that
cell rebuilds everything it needs from Redis rather than from anything the old owner
held locally. The rejected alternative - each node maintaining its own in-memory view
of the drivers and trips in the cells it owns - would need an explicit rebuild step on
every handoff and a way to detect staleness in the meantime; reading from Redis on
every call sidesteps both problems at the cost of a network round trip Redis is fast
enough to absorb.
