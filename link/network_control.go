package link

import (
	"sort"
	"time"
)

// LinkUsage reports byte occupancy and current rules for inspection/tests.
type LinkUsage struct {
	Up         bool
	Used       int
	QueueBytes int
}

// LinkUsage returns a copy of the link's current byte occupancy.
func (n *Network) LinkUsage(id string) (LinkUsage, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	l := n.links[id]
	if l == nil {
		return LinkUsage{}, ErrInvalid
	}
	return LinkUsage{Up: l.up, Used: l.used, QueueBytes: l.rules.QueueBytes}, nil
}

// UpdateLink changes rules only for traffic that has not yet entered the link.
// Packets queued on this link are already admitted there, so they continue
// under their old serialization schedule; they are not moved into the past.
func (n *Network) UpdateLink(id string, rules LinkRules) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return ErrClosed
	}
	l := n.links[id]
	if l == nil {
		return ErrInvalid
	}
	if err := validateRules(rules); err != nil {
		return err
	}
	l.rules = rules
	n.record(Event{Time: n.now, Status: StatusRuleUpdate, Link: id, Stage: StageLink})
	n.tickLinks()
	return nil
}

// SetLinkDown breaks the segment in both directions. Queued and in-flight
// packets on that segment receive an explicit link_down fate. Packets waiting
// on earlier hops are untouched and can later use a replacement path.
func (n *Network) SetLinkDown(id string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return ErrClosed
	}
	l := n.links[id]
	if l == nil {
		return ErrInvalid
	}
	if !l.up {
		return nil
	}
	l.up = false
	l.used = 0
	n.record(Event{Time: n.now, Status: StatusLinkDown, Link: id, Stage: StageLink})
	var flying []*livePacket
	for _, dir := range []Direction{DirectionAtoB, DirectionBtoA} {
		for _, p := range l.queues[dir] {
			flying = append(flying, p)
		}
		l.queues[dir] = nil
	}
	for _, p := range n.live {
		if p.flying && p.link == id {
			flying = append(flying, p)
		}
	}
	sort.Slice(flying, func(i, j int) bool { return flying[i].id < flying[j].id })
	for _, p := range flying {
		n.dropLive(p, ReasonLinkDown, id, StageLink)
	}
	l.busy = map[Direction]time.Duration{}
	return nil
}

// SetLinkUp restores a broken segment. Already-enqueued earlier traffic can
// then proceed through whatever the current path says.
func (n *Network) SetLinkUp(id string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return ErrClosed
	}
	l := n.links[id]
	if l == nil {
		return ErrInvalid
	}
	l.up = true
	n.record(Event{Time: n.now, Status: StatusLinkUp, Link: id, Stage: StageLink})
	n.tickLinks()
	return nil
}

// UpdatePath reroutes one direction. Only packets that have not yet entered
// a link are affected. A packet serialized or propagating on an old segment
// keeps that segment; when it reaches a switch node it uses the new suffix.
func (n *Network) UpdatePath(dir Direction, path []HopSpec) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return ErrClosed
	}
	if dir != DirectionAtoB && dir != DirectionBtoA {
		return ErrBadEndpoint
	}
	if err := n.checkReroute(dir, path); err != nil {
		return err
	}
	n.paths[dir] = append([]HopSpec(nil), path...)
	n.record(Event{Time: n.now, Status: StatusRerouted, Stage: "path"})
	return nil
}

func (n *Network) checkReroute(dir Direction, path []HopSpec) error {
	var start, end Node
	if dir == DirectionAtoB {
		start, end = Node(A), Node(B)
	} else {
		start, end = Node(B), Node(A)
	}
	return n.checkHops(path, start, end)
}

// Wait registers a virtual blocked reader at an endpoint.
func (n *Network) Wait(dst Endpoint) <-chan Delivery {
	ch := make(chan Delivery, 1)
	n.mu.Lock()
	defer n.mu.Unlock()
	if dst != A && dst != B {
		ch <- Delivery{Err: ErrBadEndpoint}
		return ch
	}
	if n.closed {
		ch <- Delivery{Time: n.now, Err: ErrClosed}
		return ch
	}
	n.waiters[dst] = append(n.waiters[dst], readWaiter{dst: dst, notify: ch})
	return ch
}

// Close gives every unfinished packet an explicit fate and ends all waits.
func (n *Network) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil
	}
	n.closed = true
	pending := make([]*livePacket, 0, len(n.live))
	for _, p := range n.live {
		pending = append(pending, p)
	}
	sort.Slice(pending, func(i, j int) bool { return pending[i].id < pending[j].id })
	for _, p := range pending {
		reason := ReasonClosedInFlight
		switch {
		case p.stage == StageReceiver && !p.flying:
			reason = ReasonClosedReceiver
		case p.stage == StageLink && !p.flying:
			reason = ReasonClosedQueued
		}
		p.stage = ""
		n.dropLive(p, reason, p.link, p.stage)
	}
	for _, l := range n.links {
		l.used = 0
		l.queues = map[Direction][]*livePacket{DirectionAtoB: {}, DirectionBtoA: {}}
	}
	for _, r := range n.receivers {
		r.used = 0
		r.q = nil
	}
	n.sched = nil
	for dst, waiters := range n.waiters {
		for _, waiter := range waiters {
			waiter.notify <- Delivery{Time: n.now, Err: ErrClosed}
		}
		n.waiters[dst] = nil
	}
	return nil
}
