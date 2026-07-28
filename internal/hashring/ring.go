// Package hashring implements consistent hashing with virtual nodes over a
// 64-bit ring. It is hand-rolled rather than imported from a library: the
// property it exists to demonstrate - adding the Nth node relocates
// roughly 1/N of keys, not all of them, unlike modulo hashing - is the
// entire point of building it.
package hashring

import (
	"sort"
	"strconv"
	"sync"

	"github.com/cespare/xxhash/v2"
)

// DefaultVirtualNodes is the number of ring positions each physical node
// gets. Without virtual nodes, load skew across a handful of physical
// nodes is severe: a single hash per node means each node's share of the
// ring depends entirely on where that one hash happened to land, and
// adding or removing a node can dump an uneven share of keys onto whatever
// node happens to be its clockwise neighbour. 150 replicas smooths that
// out to within a few percent in practice.
const DefaultVirtualNodes = 150

// Ring is a consistent hash ring mapping arbitrary string keys (H3 cell
// IDs, in this system) to node IDs. Safe for concurrent use.
type Ring struct {
	mu           sync.RWMutex
	virtualNodes int
	hashes       []uint64          // ring positions, kept sorted ascending
	owners       map[uint64]string // ring position -> owning node ID
}

// New returns an empty Ring using virtualNodes replicas per physical node
// (DefaultVirtualNodes if virtualNodes is not positive).
func New(virtualNodes int) *Ring {
	if virtualNodes <= 0 {
		virtualNodes = DefaultVirtualNodes
	}
	return &Ring{
		virtualNodes: virtualNodes,
		owners:       make(map[uint64]string),
	}
}

// AddNode places virtualNodes replicas of nodeID onto the ring. Only the
// keys whose nearest clockwise replica moves to one of these new positions
// change owner - every other key's nearest replica is untouched, which is
// what keeps relocation proportional to 1/N instead of total.
func (r *Ring) AddNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := 0; i < r.virtualNodes; i++ {
		h := hashReplica(nodeID, i)
		if _, exists := r.owners[h]; exists {
			continue // hash collision on this exact position: first writer wins
		}
		r.owners[h] = nodeID
		r.hashes = insertSorted(r.hashes, h)
	}
}

// RemoveNode removes nodeID and all of its replicas from the ring. Keys it
// owned fall through to their new nearest clockwise replica; every other
// key's owner is unaffected.
func (r *Ring) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := 0; i < r.virtualNodes; i++ {
		h := hashReplica(nodeID, i)
		if r.owners[h] != nodeID {
			continue
		}
		delete(r.owners, h)
		r.hashes = removeSorted(r.hashes, h)
	}
}

// Lookup returns the node ID owning key: the node whose replica is first
// at or after key's hash on the ring, wrapping around to the first replica
// if key's hash is past every replica. ok is false only if the ring has no
// nodes at all.
func (r *Ring) Lookup(key string) (nodeID string, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.hashes) == 0 {
		return "", false
	}

	h := hashKey(key)
	i := sort.Search(len(r.hashes), func(i int) bool { return r.hashes[i] >= h })
	if i == len(r.hashes) {
		i = 0
	}
	return r.owners[r.hashes[i]], true
}

// Nodes returns the distinct physical node IDs currently on the ring.
func (r *Ring) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	seen := make(map[string]bool, len(r.owners))
	nodes := make([]string, 0, len(r.owners))
	for _, id := range r.owners {
		if !seen[id] {
			seen[id] = true
			nodes = append(nodes, id)
		}
	}
	return nodes
}

func hashKey(key string) uint64 {
	return xxhash.Sum64String(key)
}

func hashReplica(nodeID string, replica int) uint64 {
	return xxhash.Sum64String(nodeID + "#" + strconv.Itoa(replica))
}

// insertSorted inserts h into the ascending-sorted hashes slice.
func insertSorted(hashes []uint64, h uint64) []uint64 {
	i := sort.Search(len(hashes), func(i int) bool { return hashes[i] >= h })
	hashes = append(hashes, 0)
	copy(hashes[i+1:], hashes[i:])
	hashes[i] = h
	return hashes
}

// removeSorted removes h from the ascending-sorted hashes slice, if present.
func removeSorted(hashes []uint64, h uint64) []uint64 {
	i := sort.Search(len(hashes), func(i int) bool { return hashes[i] >= h })
	if i < len(hashes) && hashes[i] == h {
		hashes = append(hashes[:i], hashes[i+1:]...)
	}
	return hashes
}
