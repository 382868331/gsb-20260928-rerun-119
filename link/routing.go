package link

// Stop reasons recorded when dynamic routing cannot advance a packet.
const (
	ReasonNoRoute     = "no_route"
	ReasonRoutingLoop = "routing_loop"
)

// LinkFact is one link-state fact known at the moment a routing decision was
// made. Decisions keep these facts so a later review can see exactly why a
// next hop was chosen.
type LinkFact struct {
	Link         string `json:"link"`
	Up           bool   `json:"up"`
	BandwidthBps uint64 `json:"bandwidth_bps"`
}

// RouteDecision records one next-hop choice together with the facts known at
// decision time. Chosen is empty when the packet stopped instead.
type RouteDecision struct {
	TimeNS int64      `json:"time_ns"`
	Seq    int        `json:"seq"`
	At     Node       `json:"at"`
	Dest   Node       `json:"dest"`
	Chosen string     `json:"chosen,omitempty"`
	Stop   string     `json:"stop,omitempty"`
	Facts  []LinkFact `json:"facts"`
}

// chooseHop picks the next link for a packet standing at node at. The first
// candidate link that is currently up wins; every candidate contributes a
// fact to the recorded decision. It returns a stop reason when no candidate
// is usable (no_route) or when the only usable link would revisit a node the
// packet already passed (routing_loop).
func (n *Network) chooseHop(at Node, dir Direction, seq int, visited []Node) (HopSpec, string) {
	dest := Node(A)
	if dir == DirectionAtoB {
		dest = Node(B)
	}
	candidates := n.routes[at][dest]
	facts := make([]LinkFact, 0, len(candidates))
	chosen := ""
	for _, id := range candidates {
		l := n.links[id]
		if l == nil {
			continue
		}
		facts = append(facts, LinkFact{Link: id, Up: l.up, BandwidthBps: l.rules.bandwidth(dir)})
		if chosen == "" && l.up {
			chosen = id
		}
	}
	decision := RouteDecision{
		TimeNS: int64(n.now), Seq: seq, At: at, Dest: dest, Facts: facts,
	}
	if chosen == "" {
		decision.Stop = ReasonNoRoute
		n.routeDecisions = append(n.routeDecisions, decision)
		return HopSpec{}, ReasonNoRoute
	}
	link := n.links[chosen]
	to := otherEnd(link, at)
	for _, seen := range visited {
		if seen == to {
			decision.Stop = ReasonRoutingLoop
			n.routeDecisions = append(n.routeDecisions, decision)
			return HopSpec{Link: chosen, From: at, To: to}, ReasonRoutingLoop
		}
	}
	decision.Chosen = chosen
	n.routeDecisions = append(n.routeDecisions, decision)
	return HopSpec{Link: chosen, From: at, To: to}, ""
}

func otherEnd(l *networkLink, at Node) Node {
	if l.spec.Ends[0] == at {
		return l.spec.Ends[1]
	}
	return l.spec.Ends[0]
}

// UpdateRoute replaces the ordered candidate links used to reach dest from
// node from. Only packets that have not yet entered their next link follow
// the new table; packets already serializing or in flight keep their hop.
func (n *Network) UpdateRoute(from, dest Node, links []string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return ErrClosed
	}
	if err := n.checkRoute(from, dest, links); err != nil {
		return err
	}
	if n.routes[from] == nil {
		n.routes[from] = map[Node][]string{}
	}
	n.routes[from][dest] = append([]string(nil), links...)
	n.record(Event{Time: n.now, Status: StatusRerouted, Link: links[0], Stage: "route"})
	return nil
}

func (n *Network) checkRoute(from, dest Node, links []string) error {
	if len(links) == 0 || from == dest {
		return ErrInvalid
	}
	for _, id := range links {
		l := n.links[id]
		if l == nil {
			return ErrInvalid
		}
		if l.spec.Ends[0] != from && l.spec.Ends[1] != from {
			return ErrInvalid
		}
	}
	return nil
}

// RouteDecisions returns every recorded next-hop decision in order.
func (n *Network) RouteDecisions() []RouteDecision {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]RouteDecision, len(n.routeDecisions))
	copy(out, n.routeDecisions)
	return out
}
