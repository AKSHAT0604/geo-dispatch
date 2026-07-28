// Package membership wraps hashicorp/memberlist's SWIM gossip protocol and
// keeps a hashring.Ring in sync with cluster membership. This is plumbing,
// not the interview centrepiece - unlike hashring, it is deliberately a
// thin wrapper over an existing library rather than hand-rolled.
package membership

import (
	"fmt"
	"time"

	"github.com/hashicorp/memberlist"

	"github.com/AKSHAT0604/geo-dispatch/internal/hashring"
)

// Config bounds how this node joins the cluster.
type Config struct {
	NodeName     string   // this node's unique identifier; also its ring node ID
	BindAddr     string   // defaults to memberlist's own default if empty
	BindPort     int      // 0 lets the OS assign an ephemeral port (tests)
	Seeds        []string // existing cluster members to join through, "host:port"
	VirtualNodes int      // hashring.DefaultVirtualNodes if not positive
}

// Membership gossips cluster membership and maintains a hashring.Ring that
// always reflects it: nodes joining or leaving are added to or removed
// from the ring automatically via memberlist's event delegate.
type Membership struct {
	list *memberlist.Memberlist
	ring *hashring.Ring
}

// eventDelegate adapts memberlist's join/leave callbacks to ring updates.
type eventDelegate struct {
	ring *hashring.Ring
}

func (d *eventDelegate) NotifyJoin(n *memberlist.Node)  { d.ring.AddNode(n.Name) }
func (d *eventDelegate) NotifyLeave(n *memberlist.Node) { d.ring.RemoveNode(n.Name) }
func (d *eventDelegate) NotifyUpdate(*memberlist.Node)  {} // metadata changes don't move ring positions

// New starts gossiping under cfg and returns a Membership whose ring
// already contains this node. If Seeds is non-empty, it attempts to join
// the existing cluster through them before returning.
func New(cfg Config) (*Membership, error) {
	ring := hashring.New(cfg.VirtualNodes)

	mlCfg := memberlist.DefaultLocalConfig()
	mlCfg.Name = cfg.NodeName
	if cfg.BindAddr != "" {
		mlCfg.BindAddr = cfg.BindAddr
	}
	mlCfg.BindPort = cfg.BindPort
	mlCfg.AdvertisePort = cfg.BindPort
	mlCfg.Events = &eventDelegate{ring: ring}

	list, err := memberlist.Create(mlCfg)
	if err != nil {
		return nil, fmt.Errorf("create memberlist: %w", err)
	}

	// memberlist does not fire NotifyJoin for the local node on Create, so
	// it has to be added to its own ring explicitly.
	ring.AddNode(cfg.NodeName)

	if len(cfg.Seeds) > 0 {
		if _, err := list.Join(cfg.Seeds); err != nil {
			return nil, fmt.Errorf("join cluster: %w", err)
		}
	}

	return &Membership{list: list, ring: ring}, nil
}

// Ring returns the hash ring kept in sync with cluster membership.
func (m *Membership) Ring() *hashring.Ring { return m.ring }

// LocalNode returns this node's own memberlist entry, including the
// address and port other nodes can join through.
func (m *Membership) LocalNode() *memberlist.Node { return m.list.LocalNode() }

// JoinAddr returns "host:port" other nodes can pass as a seed to join
// through this one.
func (m *Membership) JoinAddr() string {
	n := m.list.LocalNode()
	return fmt.Sprintf("%s:%d", n.Addr.String(), n.Port)
}

// Members returns the names of every node currently known to be alive.
func (m *Membership) Members() []string {
	nodes := m.list.Members()
	names := make([]string, len(nodes))
	for i, n := range nodes {
		names[i] = n.Name
	}
	return names
}

// Leave gracefully leaves the cluster, giving other members up to timeout
// to broadcast the departure before Shutdown stops gossiping entirely.
func (m *Membership) Leave(timeout time.Duration) error {
	return m.list.Leave(timeout)
}

// Shutdown stops gossiping immediately without notifying other members.
func (m *Membership) Shutdown() error {
	return m.list.Shutdown()
}
