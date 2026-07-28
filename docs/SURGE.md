# Surge response under synthetic demand

Phase 4 definition of done: driving synthetic demand into a single cell should produce
a visible surge multiplier rise, then a decay after demand stops. This is captured
directly from `internal/surge.TestSyntheticDemandProducesRiseThenDecay`, which drives
the `Aggregator` on simulated timestamps (no real waiting) and logs the ratio and
multiplier at each step:

```
go test ./internal/surge/... -v -run TestSyntheticDemandProducesRiseThenDecay
```

```
t=   0s  ratio=0.00  multiplier=1.00x  (baseline, no demand yet)
t=  10s  ratio=0.60  multiplier=1.00x  (demand ramping)
t=  20s  ratio=1.20  multiplier=1.00x  (demand ramping)
t=  30s  ratio=1.80  multiplier=1.20x  (demand ramping)
t=  40s  ratio=2.40  multiplier=1.20x  (demand ramping)
t=  50s  ratio=3.00  multiplier=1.75x  (demand ramping)
t=  60s  ratio=3.60  multiplier=1.75x  (demand ramping)
t=  70s  ratio=4.20  multiplier=1.75x  (demand ramping)
t=  80s  ratio=4.80  multiplier=1.75x  (demand ramping)
t=  90s  ratio=5.40  multiplier=2.50x  (demand ramping)
t= 100s  ratio=5.40  multiplier=2.50x  (demand stopped, decaying)
t= 120s  ratio=5.40  multiplier=2.50x  (demand stopped, decaying)
t= 140s  ratio=4.80  multiplier=1.75x  (demand stopped, decaying)
t= 160s  ratio=3.60  multiplier=1.75x  (demand stopped, decaying)
t= 180s  ratio=2.40  multiplier=1.20x  (demand stopped, decaying)
t= 200s  ratio=1.20  multiplier=1.00x  (demand stopped, decaying)
t= 220s  ratio=0.00  multiplier=1.00x  (demand stopped, decaying)
t= 240s  ratio=0.00  multiplier=1.00x  (demand stopped, decaying)
```

## Setup

- 5 drivers held `AVAILABLE` in one H3 cell throughout, re-pinged every 30s so they
  never age out of the aggregator's 2-minute sliding window.
- Demand ramps from t=10s to t=90s: 3 new `REQUESTED` trips every 10s, none resolved.
- Demand stops at t=90s: no further requests. The decay from t=100s onward is driven
  entirely by open trips aging out of the sliding window as simulated time advances -
  no explicit `MATCHED`/`UNFULFILLED` events are needed to bring the multiplier back
  down, which is the sliding window doing its job.

## Reading the curve

The ratio (`open_requests / available_drivers`) climbs roughly linearly while demand
outpaces the fixed supply of 5 drivers, crossing each of `DefaultMultiplierConfig`'s
step thresholds (1.5, 3, 5, 8) in turn. It plateaus at 5.40 once requests stop arriving,
holds there while the oldest requests are still within the 2-minute window, then decays
back down through the same steps in reverse and returns to baseline (1.0x) once every
request from the ramp has aged out - at t=220s, 130 seconds after the last request was
made at t=90s, consistent with the window plus the aggregator's per-step granularity.
