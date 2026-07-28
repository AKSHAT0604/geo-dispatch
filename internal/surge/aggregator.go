// Package surge computes a rolling supply/demand ratio per H3 cell from
// trip and driver location events, and maps that ratio to a surge
// multiplier. Pricing is deliberately kept separate from matching: disco
// reads a multiplier to quote a trip, but candidate ranking never sees it.
package surge

import (
	"math"
	"sync"
	"time"

	"github.com/uber/h3-go/v4"
)

// DefaultWindow is the sliding window the aggregator looks back over.
const DefaultWindow = 5 * time.Minute

type cellState struct {
	openTrips        map[string]time.Time // tripID -> last-seen-open timestamp
	availableDrivers map[string]time.Time // driverID -> last AVAILABLE ping timestamp
}

// Aggregator maintains, per H3 cell, the set of currently open trip
// requests and currently available drivers observed within a sliding
// window, and derives a supply/demand ratio from them.
type Aggregator struct {
	mu     sync.Mutex
	window time.Duration
	cells  map[h3.Cell]*cellState
}

// NewAggregator returns an Aggregator with the given sliding window. A
// non-positive window falls back to DefaultWindow.
func NewAggregator(window time.Duration) *Aggregator {
	if window <= 0 {
		window = DefaultWindow
	}
	return &Aggregator{window: window, cells: make(map[h3.Cell]*cellState)}
}

// RecordTrip updates cell's open-request set from a trip lifecycle
// transition: REQUESTED and OFFERED keep a trip open; MATCHED and
// UNFULFILLED close it.
func (a *Aggregator) RecordTrip(cell h3.Cell, tripID, state string, at time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	c := a.cellFor(cell)
	switch state {
	case "REQUESTED", "OFFERED":
		c.openTrips[tripID] = at
	case "MATCHED", "UNFULFILLED":
		delete(c.openTrips, tripID)
	}
}

// RecordDriver updates cell's available-driver set from a driver location
// ping: AVAILABLE marks the driver present as of at; any other state
// removes it, since a driver mid-trip isn't supply relieving demand.
func (a *Aggregator) RecordDriver(cell h3.Cell, driverID, state string, at time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	c := a.cellFor(cell)
	if state == "AVAILABLE" {
		c.availableDrivers[driverID] = at
	} else {
		delete(c.availableDrivers, driverID)
	}
}

func (a *Aggregator) cellFor(cell h3.Cell) *cellState {
	c, ok := a.cells[cell]
	if !ok {
		c = &cellState{openTrips: make(map[string]time.Time), availableDrivers: make(map[string]time.Time)}
		a.cells[cell] = c
	}
	return c
}

// Ratio returns open_requests / available_drivers for cell as of now,
// counting only entries seen within the sliding window. No open requests
// yields 0 (no pressure). Open requests with zero available drivers yields
// +Inf, which Multiplier clamps to its ceiling rather than propagating.
func (a *Aggregator) Ratio(cell h3.Cell, now time.Time) float64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	c, ok := a.cells[cell]
	if !ok {
		return 0
	}
	openTrips := countWithinWindow(c.openTrips, now, a.window)
	availableDrivers := countWithinWindow(c.availableDrivers, now, a.window)
	if openTrips == 0 {
		return 0
	}
	if availableDrivers == 0 {
		return math.Inf(1)
	}
	return float64(openTrips) / float64(availableDrivers)
}

// Multiplier is a convenience combining Ratio with cfg's step function.
func (a *Aggregator) Multiplier(cell h3.Cell, now time.Time, cfg MultiplierConfig) float64 {
	return cfg.Multiplier(a.Ratio(cell, now))
}

// Cells returns every cell the aggregator currently holds state for, so a
// caller can sweep multipliers for all of them without needing to know
// cell IDs in advance.
func (a *Aggregator) Cells() []h3.Cell {
	a.mu.Lock()
	defer a.mu.Unlock()
	cells := make([]h3.Cell, 0, len(a.cells))
	for c := range a.cells {
		cells = append(cells, c)
	}
	return cells
}

// Prune evicts entries older than the window so memory doesn't grow
// unboundedly over a long-running process. Call it periodically (e.g. once
// per window) rather than on every read.
func (a *Aggregator) Prune(now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for cell, c := range a.cells {
		pruneMap(c.openTrips, now, a.window)
		pruneMap(c.availableDrivers, now, a.window)
		if len(c.openTrips) == 0 && len(c.availableDrivers) == 0 {
			delete(a.cells, cell)
		}
	}
}

func countWithinWindow(m map[string]time.Time, now time.Time, window time.Duration) int {
	n := 0
	for _, ts := range m {
		if now.Sub(ts) <= window {
			n++
		}
	}
	return n
}

func pruneMap(m map[string]time.Time, now time.Time, window time.Duration) {
	for id, ts := range m {
		if now.Sub(ts) > window {
			delete(m, id)
		}
	}
}
