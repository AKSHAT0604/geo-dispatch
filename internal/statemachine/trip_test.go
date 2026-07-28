package statemachine

import "testing"

func TestValidateTripTransitionAllowsHappyPaths(t *testing.T) {
	cases := [][2]TripState{
		{TripRequested, TripOffered},
		{TripOffered, TripMatched},
		{TripOffered, TripUnfulfilled},
	}
	for _, c := range cases {
		if err := ValidateTripTransition(c[0], c[1]); err != nil {
			t.Errorf("ValidateTripTransition(%s, %s): %v", c[0], c[1], err)
		}
	}
}

func TestValidateTripTransitionRejectsIllegalJumps(t *testing.T) {
	cases := [][2]TripState{
		{TripRequested, TripMatched},
		{TripRequested, TripUnfulfilled},
		{TripMatched, TripOffered},
		{TripUnfulfilled, TripOffered},
		{TripMatched, TripUnfulfilled},
	}
	for _, c := range cases {
		err := ValidateTripTransition(c[0], c[1])
		if err == nil {
			t.Errorf("ValidateTripTransition(%s, %s) = nil, want error", c[0], c[1])
		}
		if _, ok := err.(ErrIllegalTripTransition); !ok {
			t.Errorf("ValidateTripTransition(%s, %s) error type = %T, want ErrIllegalTripTransition", c[0], c[1], err)
		}
	}
}
