package statemachine

import "testing"

func TestValidateDriverTransitionAllowsHappyPath(t *testing.T) {
	path := []DriverState{
		DriverOffline, DriverAvailable, DriverOffered, DriverEnRoute, DriverOnTrip, DriverAvailable,
	}
	for i := 0; i < len(path)-1; i++ {
		if err := ValidateDriverTransition(path[i], path[i+1]); err != nil {
			t.Fatalf("ValidateDriverTransition(%s, %s): %v", path[i], path[i+1], err)
		}
	}
}

func TestValidateDriverTransitionRejectsIllegalJumps(t *testing.T) {
	cases := []struct{ from, to DriverState }{
		{DriverOffline, DriverOffered},
		{DriverOffline, DriverOnTrip},
		{DriverAvailable, DriverEnRoute},
		{DriverAvailable, DriverOnTrip},
		{DriverOffered, DriverOnTrip},
		{DriverEnRoute, DriverOffered},
		{DriverOnTrip, DriverOffered},
		{DriverOnTrip, DriverEnRoute},
	}
	for _, c := range cases {
		err := ValidateDriverTransition(c.from, c.to)
		if err == nil {
			t.Errorf("ValidateDriverTransition(%s, %s) = nil, want error", c.from, c.to)
		}
		if _, ok := err.(ErrIllegalDriverTransition); !ok {
			t.Errorf("ValidateDriverTransition(%s, %s) error type = %T, want ErrIllegalDriverTransition", c.from, c.to, err)
		}
	}
}
