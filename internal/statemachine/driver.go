// Package statemachine defines the driver and trip lifecycles as explicit
// state machines. Illegal transitions are rejected, not silently accepted.
package statemachine

import "fmt"

// DriverState is a stage in a driver's lifecycle.
type DriverState string

const (
	DriverOffline   DriverState = "OFFLINE"
	DriverAvailable DriverState = "AVAILABLE"
	DriverOffered   DriverState = "OFFERED"
	DriverEnRoute   DriverState = "EN_ROUTE"
	DriverOnTrip    DriverState = "ON_TRIP"
)

// driverTransitions enumerates every legal next state for a given state.
// OFFLINE -> AVAILABLE -> OFFERED -> EN_ROUTE -> ON_TRIP -> AVAILABLE is the
// happy path; the extra edges handle a driver going offline mid-shift and an
// offer expiring or being declined.
var driverTransitions = map[DriverState]map[DriverState]bool{
	DriverOffline:   {DriverAvailable: true},
	DriverAvailable: {DriverOffered: true, DriverOffline: true},
	DriverOffered:   {DriverEnRoute: true, DriverAvailable: true},
	DriverEnRoute:   {DriverOnTrip: true, DriverAvailable: true},
	DriverOnTrip:    {DriverAvailable: true},
}

// ErrIllegalDriverTransition is returned when a requested driver state
// change is not reachable from the current state.
type ErrIllegalDriverTransition struct {
	From, To DriverState
}

func (e ErrIllegalDriverTransition) Error() string {
	return fmt.Sprintf("illegal driver transition: %s -> %s", e.From, e.To)
}

// ValidateDriverTransition returns nil if moving a driver from `from` to
// `to` is legal, and ErrIllegalDriverTransition otherwise.
func ValidateDriverTransition(from, to DriverState) error {
	if allowed, ok := driverTransitions[from]; ok && allowed[to] {
		return nil
	}
	return ErrIllegalDriverTransition{From: from, To: to}
}
