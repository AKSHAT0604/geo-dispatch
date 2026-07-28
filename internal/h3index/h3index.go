// Package h3index wraps Uber's H3 hexagonal spatial index with the handful
// of operations the rest of the system needs: mapping a coordinate to a
// cell, expanding outward in rings, and measuring grid distance between
// cells.
package h3index

import "github.com/uber/h3-go/v4"

// DefaultResolution is the H3 resolution used across the system unless a
// caller overrides it. At resolution 8 the average hexagon edge is ~0.46km:
// fine-grained enough that a k-ring of 2 already covers a plausible pickup
// radius, coarse enough that a cell holds several drivers before ring
// expansion is needed.
const DefaultResolution = 8

// CellFor returns the H3 cell containing (lat, lng) at the given resolution.
func CellFor(lat, lng float64, res int) (h3.Cell, error) {
	return h3.LatLngToCell(h3.NewLatLng(lat, lng), res)
}

// KRing returns every cell within k grid steps of origin, including origin
// itself at k=0.
func KRing(origin h3.Cell, k int) ([]h3.Cell, error) {
	return origin.GridDisk(k)
}

// DistanceCells returns the number of grid steps between two cells at the
// same resolution.
func DistanceCells(a, b h3.Cell) (int, error) {
	return a.GridDistance(b)
}
