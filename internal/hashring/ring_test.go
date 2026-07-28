package hashring

import (
	"fmt"
	"math"
	"testing"
)

func TestLookupIsStableAcrossRepeatedCalls(t *testing.T) {
	r := New(DefaultVirtualNodes)
	r.AddNode("node-a")
	r.AddNode("node-b")

	key := "cell-123"
	first, ok := r.Lookup(key)
	if !ok {
		t.Fatalf("Lookup: no owner found")
	}
	for i := 0; i < 100; i++ {
		got, _ := r.Lookup(key)
		if got != first {
			t.Fatalf("Lookup(%q) is not stable: got %s, first was %s", key, got, first)
		}
	}
}

func TestLookupWithNoNodesReturnsFalse(t *testing.T) {
	r := New(DefaultVirtualNodes)
	if _, ok := r.Lookup("cell-1"); ok {
		t.Fatalf("Lookup on empty ring = ok, want false")
	}
}

// TestAddNodeRelocatesRoughlyOneOverN is the phase 5 definition-of-done
// test: it proves the core property that separates consistent hashing from
// modulo hashing - adding the Nth node relocates roughly 1/N of keys, not
// all of them.
func TestAddNodeRelocatesRoughlyOneOverN(t *testing.T) {
	const numKeys = 20000
	keys := make([]string, numKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("cell-%d", i)
	}

	r := New(DefaultVirtualNodes)
	r.AddNode("node-0")

	owners := make(map[string]string, numKeys)
	for _, k := range keys {
		owners[k], _ = r.Lookup(k)
	}

	for n := 2; n <= 8; n++ {
		r.AddNode(fmt.Sprintf("node-%d", n-1))

		moved := 0
		newOwners := make(map[string]string, numKeys)
		for _, k := range keys {
			owner, _ := r.Lookup(k)
			newOwners[k] = owner
			if owner != owners[k] {
				moved++
			}
		}

		wantFraction := 1.0 / float64(n)
		gotFraction := float64(moved) / float64(numKeys)

		// Virtual nodes smooth distribution but don't make it exact;
		// allow a generous band around the theoretical 1/N.
		if math.Abs(gotFraction-wantFraction) > 0.15 {
			t.Errorf("n=%d: moved %.1f%% of keys, want close to %.1f%% (1/%d)", n, gotFraction*100, wantFraction*100, n)
		}
		// The actual contrast with modulo hashing: adding a node must not
		// reshuffle most of the keyspace.
		if gotFraction > 0.6 {
			t.Errorf("n=%d: moved %.1f%% of keys - consistent hashing should relocate a minority, not most", n, gotFraction*100)
		}

		owners = newOwners
	}
}

func TestRemoveNodeOnlyMovesThatNodesKeys(t *testing.T) {
	const numKeys = 10000
	keys := make([]string, numKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("cell-%d", i)
	}

	r := New(DefaultVirtualNodes)
	for _, id := range []string{"node-a", "node-b", "node-c", "node-d"} {
		r.AddNode(id)
	}

	before := make(map[string]string, numKeys)
	for _, k := range keys {
		before[k], _ = r.Lookup(k)
	}

	r.RemoveNode("node-b")

	for _, k := range keys {
		after, _ := r.Lookup(k)
		if before[k] != "node-b" {
			if after != before[k] {
				t.Fatalf("key %q owner changed from %s to %s after removing an unrelated node", k, before[k], after)
			}
		} else if after == "node-b" {
			t.Fatalf("key %q still owned by removed node-b", k)
		}
	}
}

func TestNodesReturnsDistinctPhysicalNodes(t *testing.T) {
	r := New(10)
	r.AddNode("node-a")
	r.AddNode("node-b")

	got := r.Nodes()
	if len(got) != 2 {
		t.Fatalf("Nodes() = %v, want 2 distinct nodes", got)
	}

	r.RemoveNode("node-a")
	got = r.Nodes()
	if len(got) != 1 || got[0] != "node-b" {
		t.Fatalf("Nodes() after removal = %v, want [node-b]", got)
	}
}

func TestDistributionAcrossNodesIsReasonablyBalanced(t *testing.T) {
	const numKeys = 20000
	const numNodes = 5

	r := New(DefaultVirtualNodes)
	for i := 0; i < numNodes; i++ {
		r.AddNode(fmt.Sprintf("node-%d", i))
	}

	counts := make(map[string]int)
	for i := 0; i < numKeys; i++ {
		owner, _ := r.Lookup(fmt.Sprintf("cell-%d", i))
		counts[owner]++
	}

	want := numKeys / numNodes
	for node, count := range counts {
		deviation := math.Abs(float64(count-want)) / float64(want)
		if deviation > 0.25 {
			t.Errorf("node %s owns %d keys (%.1f%% of even split), want within 25%% of %d", node, count, float64(count)/float64(want)*100, want)
		}
	}
}
