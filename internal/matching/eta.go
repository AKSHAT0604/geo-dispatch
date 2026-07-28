// Package matching implements candidate search and ranking: given a ride
// request's location, find nearby AVAILABLE drivers and order them by
// estimated pickup time. disco is the only caller that turns a ranking into
// an actual assignment; this package only produces the ordering.
package matching

import (
	"fmt"
	"time"

	"github.com/uber/h3-go/v4"

	"github.com/AKSHAT0604/geo-dispatch/internal/h3index"
)

// Location is a plain lat/lng pair, kept independent of any driver or trip
// type so ETAEstimator implementations don't need to know about either.
type Location struct {
	Lat, Lng float64
}

// ETAEstimator estimates travel time from one point to another. This is
// deliberately the seam where a real ETA model plugs in later without
// touching the ranking logic that consumes it; see docs/DECISIONS.md.
type ETAEstimator interface {
	Estimate(from, to Location) (time.Duration, error)
}

// AssumedSpeedKmh is the flat travel speed HaversineEstimator assumes when
// nothing else is known: a rough citywide average, not tuned per road
// class.
const AssumedSpeedKmh = 25.0

// HaversineEstimator estimates ETA as great-circle distance divided by a
// fixed assumed speed. It is the baseline every other estimator is judged
// against.
type HaversineEstimator struct {
	SpeedKmh float64
}

// NewHaversineEstimator returns a HaversineEstimator using AssumedSpeedKmh.
func NewHaversineEstimator() *HaversineEstimator {
	return &HaversineEstimator{SpeedKmh: AssumedSpeedKmh}
}

func (e *HaversineEstimator) Estimate(from, to Location) (time.Duration, error) {
	speed := e.SpeedKmh
	if speed <= 0 {
		speed = AssumedSpeedKmh
	}
	distKm := h3.GreatCircleDistanceKm(h3.NewLatLng(from.Lat, from.Lng), h3.NewLatLng(to.Lat, to.Lng))
	hours := distKm / speed
	return time.Duration(hours * float64(time.Hour)), nil
}

// CongestionProvider supplies a multiplicative slowdown factor for an H3
// cell, derived from recent trip speeds observed there. 1.0 means no
// adjustment; a factor above 1.0 stretches the haversine estimate to
// reflect a cell currently moving slower than free flow.
type CongestionProvider interface {
	CongestionFactor(cell h3.Cell) float64
}

// InMemoryCongestionProvider is a CongestionProvider backed by a plain map.
// It is the default until a provider fed by completed-trip speeds (a later
// phase) replaces it; cells with no recorded factor are treated as
// free-flowing.
type InMemoryCongestionProvider struct {
	factors map[h3.Cell]float64
}

// NewInMemoryCongestionProvider returns an InMemoryCongestionProvider with
// no recorded factors; every cell starts free-flowing (factor 1.0).
func NewInMemoryCongestionProvider() *InMemoryCongestionProvider {
	return &InMemoryCongestionProvider{factors: make(map[h3.Cell]float64)}
}

// Set records the congestion factor for a cell.
func (p *InMemoryCongestionProvider) Set(cell h3.Cell, factor float64) {
	p.factors[cell] = factor
}

func (p *InMemoryCongestionProvider) CongestionFactor(cell h3.Cell) float64 {
	if f, ok := p.factors[cell]; ok && f > 0 {
		return f
	}
	return 1.0
}

// CongestionAwareEstimator adjusts a baseline haversine estimate by the
// origin cell's current congestion factor. It is the default ETAEstimator:
// straight-line distance alone systematically under-estimates travel time
// in slow cells and over-estimates it in fast-flowing ones.
type CongestionAwareEstimator struct {
	Base       ETAEstimator
	Provider   CongestionProvider
	Resolution int
}

// NewCongestionAwareEstimator returns a CongestionAwareEstimator over a
// HaversineEstimator baseline, reading congestion from provider at the
// given H3 resolution.
func NewCongestionAwareEstimator(provider CongestionProvider, resolution int) *CongestionAwareEstimator {
	return &CongestionAwareEstimator{
		Base:       NewHaversineEstimator(),
		Provider:   provider,
		Resolution: resolution,
	}
}

func (e *CongestionAwareEstimator) Estimate(from, to Location) (time.Duration, error) {
	base, err := e.Base.Estimate(from, to)
	if err != nil {
		return 0, err
	}

	cell, err := h3index.CellFor(from.Lat, from.Lng, e.Resolution)
	if err != nil {
		return 0, fmt.Errorf("cell for origin: %w", err)
	}

	factor := e.Provider.CongestionFactor(cell)
	if factor <= 0 {
		factor = 1.0
	}
	return time.Duration(float64(base) * factor), nil
}
