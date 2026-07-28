package statemachine

import "fmt"

// OfferState is a stage in a single offer's lifecycle: one round of asking
// one driver to take one trip.
type OfferState string

const (
	OfferPending  OfferState = "PENDING"
	OfferAccepted OfferState = "ACCEPTED"
	OfferDeclined OfferState = "DECLINED"
	OfferTimedOut OfferState = "TIMED_OUT"
)

// offerTransitions enumerates every legal next state. All three outcomes
// are terminal: a driver who declines or times out doesn't get asked again
// on the same offer - the dispatcher opens a fresh offer for the next
// candidate instead.
var offerTransitions = map[OfferState]map[OfferState]bool{
	OfferPending: {OfferAccepted: true, OfferDeclined: true, OfferTimedOut: true},
}

// ErrIllegalOfferTransition is returned when a requested offer state
// change is not reachable from the current state.
type ErrIllegalOfferTransition struct {
	From, To OfferState
}

func (e ErrIllegalOfferTransition) Error() string {
	return fmt.Sprintf("illegal offer transition: %s -> %s", e.From, e.To)
}

// ValidateOfferTransition returns nil if moving an offer from `from` to
// `to` is legal, and ErrIllegalOfferTransition otherwise.
func ValidateOfferTransition(from, to OfferState) error {
	if allowed, ok := offerTransitions[from]; ok && allowed[to] {
		return nil
	}
	return ErrIllegalOfferTransition{From: from, To: to}
}
