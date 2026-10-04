package link

import (
	"sort"
)

// route picks the next hop for p leaving node at, using only link states
// that have already occurred. The configured path is preferred; when its
// next hop is missing or down, the router searches the currently-up links
// for any simple path to the destination. Every choice is recorded with
// the down-link facts known at decision time.
func (n *Network) route(p *livePacket, at Node) (HopSpec, *networkLink, string) {
	dest := Node(p.to)
	if hop, link, ok := n.preferredHop(p.dir, at); ok {
		if p.visited[hop.To] {
			n.recordRoute(p, at, HopSpec{}, ReasonRoutingLoop)
			return HopSpec{}, nil, ReasonRoutingLoop
		}
		if link.up {
			n.recordRoute(p, at, hop, "")
			return hop, link, ""
		}
	}
	hop, link, found := n.searchRoute(p, at, dest)
	if !found {
		n.recordRoute(p, at, HopSpec{}, ReasonNoRoute)
		return HopSpec{}, nil, ReasonNoRoute
	}
	n.recordRoute(p, at, hop, "")
	return hop, link, ""
}

// preferredHop returns the configured hop leaving at, if the path has one.
func (n *Network) preferredHop(dir Direction, at Node) (HopSpec, *networkLink, bool) {
	for _, hop := range n.paths[dir] {
		if hop.From == at {
			return hop, n.links[hop.Link], true
		}
	}
	return HopSpec{}, nil, false
}

// searchRoute finds a simple path over up links from at to dest, avoiding
// nodes the packet already visited so a reroute can never circle. Link IDs
// are scanned in sorted order to keep the choice deterministic.
func (n *Network) searchRoute(p *livePacket, at, dest Node) (HopSpec, *networkLink, bool) {
	ids := make([]string, 0, len(n.links))
	for id, l := range n.links {
		if l.up {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	type step struct {
		node Node
		hop  HopSpec
	}
	seen := map[Node]bool{at: true}
	queue := []step{{node: at}}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, id := range ids {
			l := n.links[id]
			var next Node
			switch cur.node {
			case l.spec.Ends[0]:
				next = l.spec.Ends[1]
			case l.spec.Ends[1]:
				next = l.spec.Ends[0]
			default:
				continue
			}
			if seen[next] || p.visited[next] {
				continue
			}
			hop := cur.hop
			if hop.Link == "" {
				hop = HopSpec{Link: id, From: cur.node, To: next}
			}
			if next == dest {
				return hop, n.links[hop.Link], true
			}
			seen[next] = true
			queue = append(queue, step{node: next, hop: hop})
		}
	}
	return HopSpec{}, nil, false
}

// recordRoute appends the decision with the down-link facts known now.
func (n *Network) recordRoute(p *livePacket, at Node, hop HopSpec, reason string) {
	rd := RouteDecision{
		TimeNS: int64(n.now), Seq: p.seq, At: at,
		Link: hop.Link, To: hop.To, Down: n.downLinks(),
		Candidates: n.candidateLinks(p.dir, at), Reason: reason,
	}
	n.routes = append(n.routes, rd)
}

// candidateLinks snapshots every link leaving node at at the current instant:
// its up/down state and the bandwidth the flow's direction would serialize
// with. The copy is taken once, at decision time, so later bandwidth updates
// cannot change what an old record claims the router knew.
func (n *Network) candidateLinks(dir Direction, at Node) []CandidateLink {
	ids := make([]string, 0, len(n.links))
	for id := range n.links {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	candidates := make([]CandidateLink, 0, len(ids))
	for _, id := range ids {
		l := n.links[id]
		var next Node
		switch at {
		case l.spec.Ends[0]:
			next = l.spec.Ends[1]
		case l.spec.Ends[1]:
			next = l.spec.Ends[0]
		default:
			continue
		}
		candidates = append(candidates, CandidateLink{
			Link: id, To: next, Up: l.up,
			BandwidthBytesPerSecond: l.rules.bandwidth(dir),
		})
	}
	return candidates
}

func (n *Network) downLinks() []string {
	var down []string
	for id, l := range n.links {
		if !l.up {
			down = append(down, id)
		}
	}
	sort.Strings(down)
	return down
}

// checkHops validates a candidate path: every hop must name a real link
// between the stated nodes, hops must chain, no node may repeat (a loop
// would advance events forever), and the last hop must reach the
// destination.
func (n *Network) checkHops(path []HopSpec, start, end Node) error {
	if len(path) == 0 {
		return ErrNoRoute
	}
	if path[0].From != start {
		return ErrInvalid
	}
	seen := map[Node]bool{start: true}
	for i, hop := range path {
		l := n.links[hop.Link]
		if l == nil || hop.From == hop.To {
			return ErrInvalid
		}
		if (hop.From != l.spec.Ends[0] && hop.From != l.spec.Ends[1]) ||
			(hop.To != l.spec.Ends[0] && hop.To != l.spec.Ends[1]) {
			return ErrInvalid
		}
		if i > 0 && path[i-1].To != hop.From {
			return ErrInvalid
		}
		if seen[hop.To] {
			return ErrRoutingLoop
		}
		seen[hop.To] = true
	}
	if path[len(path)-1].To != end {
		return ErrNoRoute
	}
	return nil
}
