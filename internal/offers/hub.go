// Package offers runs the offer/reoffer loop: given a trip and its ranked
// candidates, offer to each in turn, waiting for a response or a timeout,
// until one accepts or the round cap is reached.
package offers

import "sync"

// Response is a driver's answer to an open offer, or the absence of one.
type Response int

const (
	// NoResponse means the offer window elapsed with no answer: a timeout.
	NoResponse Response = iota
	Accepted
	Declined
)

// ResponseHub routes a driver's accept/decline call to the dispatcher loop
// currently waiting on it. Only one offer is ever open per trip, so it's
// keyed by trip ID.
type ResponseHub struct {
	mu      sync.Mutex
	waiters map[string]chan Response
}

// NewResponseHub returns an empty ResponseHub.
func NewResponseHub() *ResponseHub {
	return &ResponseHub{waiters: make(map[string]chan Response)}
}

// open registers a waiter for tripID's current offer and returns the
// channel a response will arrive on. A second open for the same trip
// replaces the first: that only happens once the prior round has already
// been closed by the dispatcher, so there is never a real waiter to lose.
func (h *ResponseHub) open(tripID string) chan Response {
	h.mu.Lock()
	defer h.mu.Unlock()
	ch := make(chan Response, 1)
	h.waiters[tripID] = ch
	return ch
}

// close removes tripID's waiter, if any.
func (h *ResponseHub) close(tripID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.waiters, tripID)
}

// Respond delivers a driver's decision for tripID's currently open offer.
// It returns false if there is no open offer for that trip - already timed
// out, already responded to, or the trip ID is unknown - which callers
// should treat as a stale response to ignore rather than an error.
func (h *ResponseHub) Respond(tripID string, resp Response) bool {
	h.mu.Lock()
	ch, ok := h.waiters[tripID]
	h.mu.Unlock()
	if !ok {
		return false
	}
	select {
	case ch <- resp:
		return true
	default:
		return false // already responded to this round
	}
}
