package h3index

import "testing"

func TestCellForIsStableAndValid(t *testing.T) {
	// Hyderabad, roughly.
	cell, err := CellFor(17.3850, 78.4867, DefaultResolution)
	if err != nil {
		t.Fatalf("CellFor: %v", err)
	}
	if !cell.IsValid() {
		t.Fatalf("CellFor returned an invalid cell: %v", cell)
	}
	if cell.Resolution() != DefaultResolution {
		t.Fatalf("resolution = %d, want %d", cell.Resolution(), DefaultResolution)
	}

	again, err := CellFor(17.3850, 78.4867, DefaultResolution)
	if err != nil {
		t.Fatalf("CellFor (second call): %v", err)
	}
	if cell != again {
		t.Fatalf("CellFor is not deterministic: %v != %v", cell, again)
	}
}

func TestKRingIncludesOriginAndGrowsWithK(t *testing.T) {
	origin, err := CellFor(17.3850, 78.4867, DefaultResolution)
	if err != nil {
		t.Fatalf("CellFor: %v", err)
	}

	ring0, err := KRing(origin, 0)
	if err != nil {
		t.Fatalf("KRing(0): %v", err)
	}
	if len(ring0) != 1 || ring0[0] != origin {
		t.Fatalf("KRing(0) = %v, want [origin]", ring0)
	}

	ring2, err := KRing(origin, 2)
	if err != nil {
		t.Fatalf("KRing(2): %v", err)
	}
	found := false
	for _, c := range ring2 {
		if c == origin {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("KRing(2) does not contain origin")
	}
	// A full k=2 hexagonal disk has 1 + 6 + 12 = 19 cells baring pentagon
	// distortion, which does not occur at this location.
	if len(ring2) != 19 {
		t.Fatalf("len(KRing(2)) = %d, want 19", len(ring2))
	}
}

func TestDistanceCellsIsZeroForSameCellAndSymmetric(t *testing.T) {
	a, err := CellFor(17.3850, 78.4867, DefaultResolution)
	if err != nil {
		t.Fatalf("CellFor: %v", err)
	}
	b, err := CellFor(17.4000, 78.5000, DefaultResolution)
	if err != nil {
		t.Fatalf("CellFor: %v", err)
	}

	if d, err := DistanceCells(a, a); err != nil || d != 0 {
		t.Fatalf("DistanceCells(a, a) = %d, %v; want 0, nil", d, err)
	}

	dAB, err := DistanceCells(a, b)
	if err != nil {
		t.Fatalf("DistanceCells(a, b): %v", err)
	}
	dBA, err := DistanceCells(b, a)
	if err != nil {
		t.Fatalf("DistanceCells(b, a): %v", err)
	}
	if dAB != dBA {
		t.Fatalf("DistanceCells not symmetric: a->b=%d b->a=%d", dAB, dBA)
	}
	if dAB == 0 {
		t.Fatalf("DistanceCells(a, b) = 0, want > 0 for distinct coordinates")
	}
}
