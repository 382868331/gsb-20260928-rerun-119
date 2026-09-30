package link

import "time"

// UpdateSegment changes rules of one segment. Only packets that enter the
// segment afterwards use the new rules; queued and in-flight packets keep
// their scheduled serialization and arrival.
func (n *Network) UpdateSegment(name string, cfg SegmentCfg) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return ErrClosed
	}
	seg, ok := n.segs[name]
	if !ok {
		return ErrNoSegment
	}
	if err := validateSegment(cfg); err != nil {
		return err
	}
	seg.cfg = cfg
	return nil
}

// Reroute changes the path for packets sent from ep afterwards. Packets that
// have already entered a segment finish that segment and consult the new route
// for every hop they have not entered yet.
func (n *Network) Reroute(ep Endpoint, route []string) error {
	if ep != A && ep != B {
		return ErrBadEndpoint
	}
	if len(route) == 0 {
		return ErrNoRoute
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return ErrClosed
	}
	copied := make([]string, len(route))
	for i, name := range route {
		if _, ok := n.segs[name]; !ok {
			return ErrNoRoute
		}
		copied[i] = name
	}
	n.routes[ep] = copied
	for key, f := range n.active {
		if key.from != ep {
			continue
		}
		// route[hop] is the segment f is currently queued on, propagating
		// through, or about to enter. That segment and every earlier one are
		// immutable; only segments f has not entered can change.
		if f.location == LocationQueue || f.location == LocationInFlight {
			prefix := append([]string(nil), f.route[:f.hop+1]...)
			nextHop := f.hop + 1
			if nextHop < len(copied) {
				f.route = append(prefix, copied[nextHop:]...)
			} else {
				f.route = prefix
			}
		} else if f.hop < len(f.route) {
			f.route = append([]string(nil), copied...)
		}
	}
	return nil
}

// Break marks a segment down for a short outage. Packets queued on or flying
// through it are dropped with link_down; new packets entering while it is down
// are dropped the same way. Other segments keep forwarding.
func (n *Network) Break(name string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	seg, ok := n.segs[name]
	if !ok {
		return ErrNoSegment
	}
	if n.closed {
		return ErrClosed
	}
	seg.down = true
	queued := append([]*flight(nil), seg.queued...)
	sortFlights(queued)
	for _, f := range queued {
		n.dropActive(f, ReasonLinkDown, name, LocationQueue, n.now)
	}
	seg.queued = nil
	seg.used = 0
	inflight := make([]*flight, 0, len(seg.inflight))
	for _, f := range seg.inflight {
		inflight = append(inflight, f)
	}
	sortFlights(inflight)
	for _, f := range inflight {
		n.dropActive(f, ReasonLinkDown, name, LocationInFlight, n.now)
	}
	seg.inflight = map[int64]*flight{}
	seg.busyAB = n.now
	seg.busyBA = n.now
	return nil
}

// Recover ends an outage. Queued and new traffic flows normally again; packets
// already dropped during the outage are not resurrected.
func (n *Network) Recover(name string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	seg, ok := n.segs[name]
	if !ok {
		return ErrNoSegment
	}
	if n.closed {
		return ErrClosed
	}
	seg.down = false
	return nil
}

// Close gives every unfinished packet an explicit disposition and ends waits.
func (n *Network) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil
	}
	n.closed = true
	n.heap = &eventHeap{}
	names := make([]string, 0, len(n.segs))
	for name := range n.segs {
		names = append(names, name)
	}
	sortStrings(names)
	for _, name := range names {
		seg := n.segs[name]
		queued := append([]*flight(nil), seg.queued...)
		sortFlights(queued)
		for _, f := range queued {
			n.closeEvent(f, name, LocationQueue)
		}
		inflight := make([]*flight, 0, len(seg.inflight))
		for _, f := range seg.inflight {
			inflight = append(inflight, f)
		}
		sortFlights(inflight)
		for _, f := range inflight {
			n.closeEvent(f, name, LocationInFlight)
		}
		seg.queued = nil
		seg.inflight = map[int64]*flight{}
		seg.used = 0
	}
	for _, ep := range []Endpoint{A, B} {
		rc := n.recvs[ep]
		if rc == nil {
			continue
		}
		accepted := append([]*flight(nil), rc.accepted...)
		sortFlights(accepted)
		for _, f := range accepted {
			n.closeEvent(f, "", LocationReceiver)
		}
		rc.accepted = nil
		rc.used = 0
	}
	n.active = map[flightKey]*flight{}
	for dst := range n.waiters {
		for _, w := range n.waiters[dst] {
			w.notify <- Delivery{Time: n.now, Err: ErrClosed}
		}
		n.waiters[dst] = nil
	}
	for dst := range n.delivered {
		n.delivered[dst] = nil
	}
	return nil
}

func (n *Network) closeEvent(f *flight, segName, loc string) {
	n.record(Event{
		Time: n.now, Seq: f.seq, From: f.from, To: f.to, Bytes: len(f.data),
		Status: StatusDropped, Reason: ReasonClosedWhileQueue,
		Segment: segName, Location: loc,
	})
}

// Wait registers a virtual blocked reader. Close ends the wait with ErrClosed.
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

func (n *Network) Clock() time.Duration {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.now
}

func (n *Network) Events() []Event {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]Event, len(n.events))
	copy(out, n.events)
	return out
}

// Decisions returns the random draws taken so far, suitable for saving and
// later replay with WithReplay.
func (n *Network) Decisions() []Decision {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]Decision, len(n.decisions))
	copy(out, n.decisions)
	return out
}

// ReplayError reports whether a replay diverged from the saved draw log.
func (n *Network) ReplayError() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.replayErr != nil {
		return n.replayErr
	}
	if n.replay != nil && n.replayUsed != len(n.replay) {
		return ErrReplayMismatch
	}
	return nil
}
