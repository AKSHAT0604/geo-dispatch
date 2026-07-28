package statemachine

import "fmt"

// TripState is a stage in a ride request's lifecycle.
type TripState string

const (
	TripRequested   TripState = "REQUESTED"
	TripOffered     TripState = "OFFERED"
	TripMatched     TripState = "MATCHED"
	TripUnfulfilled TripState = "UNFULFILLED"
)

// tripTransitions enumerates every legal next state. A trip enters OFFERED
// once its first offer goes out and stays there across every reoffer round
// - the round-by-round detail lives in the offer state machine, not here -
// until it either matches or exhausts its reoffer rounds. REQUESTED can
// also go straight to UNFULFILLED: candidate search can legitimately find
// no one at all, in which case no offer is ever made.
var tripTransitions = map[TripState]map[TripState]bool{
	TripRequested: {TripOffered: true, TripUnfulfilled: true},
	TripOffered:   {TripMatched: true, TripUnfulfilled: true},
}

// ErrIllegalTripTransition is returned when a requested trip state change
// is not reachable from the current state.
type ErrIllegalTripTransition struct {
	From, To TripState
}

func (e ErrIllegalTripTransition) Error() string {
	return fmt.Sprintf("illegal trip transition: %s -> %s", e.From, e.To)
}

// ValidateTripTransition returns nil if moving a trip from `from` to `to`
// is legal, and ErrIllegalTripTransition otherwise.
func ValidateTripTransition(from, to TripState) error {
	if allowed, ok := tripTransitions[from]; ok && allowed[to] {
		return nil
	}
	return ErrIllegalTripTransition{From: from, To: to}
}
