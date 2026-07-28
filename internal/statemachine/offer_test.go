package statemachine

import "testing"

func TestValidateOfferTransitionAllowsAllThreeOutcomes(t *testing.T) {
	for _, to := range []OfferState{OfferAccepted, OfferDeclined, OfferTimedOut} {
		if err := ValidateOfferTransition(OfferPending, to); err != nil {
			t.Errorf("ValidateOfferTransition(PENDING, %s): %v", to, err)
		}
	}
}

func TestValidateOfferTransitionRejectsTransitionsOutOfTerminalStates(t *testing.T) {
	terminal := []OfferState{OfferAccepted, OfferDeclined, OfferTimedOut}
	for _, from := range terminal {
		for _, to := range []OfferState{OfferPending, OfferAccepted, OfferDeclined, OfferTimedOut} {
			if from == to {
				continue
			}
			err := ValidateOfferTransition(from, to)
			if err == nil {
				t.Errorf("ValidateOfferTransition(%s, %s) = nil, want error", from, to)
			}
			if _, ok := err.(ErrIllegalOfferTransition); !ok {
				t.Errorf("ValidateOfferTransition(%s, %s) error type = %T, want ErrIllegalOfferTransition", from, to, err)
			}
		}
	}
}
