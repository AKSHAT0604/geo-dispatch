# Design Decisions

Each answer below names the alternative that was rejected and why. These are written as
interview answers, not as documentation prose.

## 1. Why hexagons rather than squares or geohash?

_To be answered once Phase 1 (H3 indexing) is complete._

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
