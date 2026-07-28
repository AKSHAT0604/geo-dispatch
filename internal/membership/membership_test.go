package membership

import (
	"testing"
	"time"
)

func newTestNode(t *testing.T, name string, seeds []string) *Membership {
	t.Helper()
	m, err := New(Config{
		NodeName: name,
		BindAddr: "127.0.0.1",
		BindPort: 0,
		Seeds:    seeds,
	})
	if err != nil {
		t.Fatalf("New(%s): %v", name, err)
	}
	t.Cleanup(func() { m.Shutdown() })
	return m
}

// waitFor polls cond until it's true or timeout elapses, failing the test
// otherwise. Gossip convergence is asynchronous, so tests can't assert on
// state immediately after a join/leave call returns.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

func TestJoinAddsBothNodesToBothRings(t *testing.T) {
	node1 := newTestNode(t, "node-1", nil)
	node2 := newTestNode(t, "node-2", []string{node1.JoinAddr()})

	waitFor(t, 5*time.Second, func() bool {
		return len(node1.Ring().Nodes()) == 2 && len(node2.Ring().Nodes()) == 2
	}, "both rings to contain 2 nodes")

	for _, id := range []string{"node-1", "node-2"} {
		if _, ok := node1.Ring().Lookup(id + "-lookup-key"); !ok {
			t.Fatalf("node1's ring has no owner for a test key after both nodes joined")
		}
	}
}

func TestLeaveRemovesNodeFromPeerRing(t *testing.T) {
	node1 := newTestNode(t, "node-1", nil)
	node2 := newTestNode(t, "node-2", []string{node1.JoinAddr()})

	waitFor(t, 5*time.Second, func() bool {
		return len(node1.Ring().Nodes()) == 2 && len(node2.Ring().Nodes()) == 2
	}, "both rings to contain 2 nodes before leave")

	if err := node2.Leave(2 * time.Second); err != nil {
		t.Fatalf("Leave: %v", err)
	}

	waitFor(t, 5*time.Second, func() bool {
		nodes := node1.Ring().Nodes()
		return len(nodes) == 1 && nodes[0] == "node-1"
	}, "node1's ring to drop node-2 after it leaves")
}

func TestThreeNodeClusterConverges(t *testing.T) {
	node1 := newTestNode(t, "node-1", nil)
	node2 := newTestNode(t, "node-2", []string{node1.JoinAddr()})
	node3 := newTestNode(t, "node-3", []string{node1.JoinAddr()})

	waitFor(t, 5*time.Second, func() bool {
		return len(node1.Ring().Nodes()) == 3 &&
			len(node2.Ring().Nodes()) == 3 &&
			len(node3.Ring().Nodes()) == 3
	}, "all three rings to converge on 3 nodes")

	// Every node's ring must agree on who owns a given key - that's the
	// entire point of keeping the ring in lockstep with membership.
	key := "cell-shared-key"
	owner1, _ := node1.Ring().Lookup(key)
	owner2, _ := node2.Ring().Lookup(key)
	owner3, _ := node3.Ring().Lookup(key)
	if owner1 != owner2 || owner2 != owner3 {
		t.Fatalf("rings disagree on owner of %q: node1=%s node2=%s node3=%s", key, owner1, owner2, owner3)
	}
}
