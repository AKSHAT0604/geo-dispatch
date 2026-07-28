// Package events defines the Kafka topics and payload types the system
// publishes lifecycle transitions to, and a Publisher interface so callers
// don't need a live broker to be tested.
package events

import "time"

// Topics. driver.location is keyed by driver ID, trip.lifecycle and
// offer.events by trip ID, so per-entity ordering is preserved regardless
// of partition count.
const (
	TopicDriverLocation = "driver.location"
	TopicTripLifecycle  = "trip.lifecycle"
	TopicOfferEvents    = "offer.events"
)

// DriverLocationEvent is published on every location ping. Cell is carried
// as a string (H3's own encoding) so consumers - the surge aggregator,
// analytics - never need to recompute geography from lat/lng.
type DriverLocationEvent struct {
	DriverID  string    `json:"driver_id"`
	Lat       float64   `json:"lat"`
	Lng       float64   `json:"lng"`
	Cell      string    `json:"cell"`
	State     string    `json:"state"`
	Timestamp time.Time `json:"timestamp"`
}

// TripLifecycleEvent is published on every trip state transition. Cell is
// the trip's origin cell.
type TripLifecycleEvent struct {
	TripID    string    `json:"trip_id"`
	State     string    `json:"state"`
	Cell      string    `json:"cell"`
	DriverID  string    `json:"driver_id,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// OfferEvent is published on every offer state transition.
type OfferEvent struct {
	TripID    string    `json:"trip_id"`
	DriverID  string    `json:"driver_id"`
	Round     int       `json:"round"`
	State     string    `json:"state"`
	Cell      string    `json:"cell"`
	Timestamp time.Time `json:"timestamp"`
}
