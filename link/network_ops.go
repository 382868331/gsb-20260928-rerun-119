package link

import (
	"container/heap"
	"time"
)

// Additional record statuses for path-wide events.
const (
	StatusSerializing = "serializing"
	StatusHopArrived  = "hop_arrived"
	StatusLinkDown    = "link_down"
	StatusLinkUp      = "link_up"
	StatusRerouted    = "rerouted"
)

func (n *Network) Clock() time.Duration {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.now
}

func (n *Network) SetClock(t time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if t > n.now {
		n.now = t
	}
}

func (n *Network) Events() []Event {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]Event, len(n.events))
	copy(out, n.events)
	return out
}

func (n *Network) Decisions() []Decision {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]Decision, len(n.decisions))
	copy(out, n.decisions)
	return out
}

// Send admits one packet at the current virtual time. Data is copied.
func (n *Network) Send(from Endpoint, seq int, data []byte) (SubmitResult, error) {
	to, err := peer(from)
	if err != nil {
		return SubmitResult{}, err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return SubmitResult{Status: StatusClosed, Reason: ReasonLinkClosed}, ErrClosed
	}
	size := len(data)
	dir := flowDirection(from, to)
	hops := n.paths[dir]
	first := n.links[hops[0].Link]
	reason := ""
	switch {
	case size == 0:
		reason = ReasonEmptyPacket
	case size > first.rules.QueueBytes:
		reason = ReasonPacketTooLarge
	case !first.up:
		reason = ReasonLinkDown
	}
	if reason == "" && first.rules.loss(dir) > 0 {
		if _, dropped := n.random(first, dir, seq); dropped {
			reason = ReasonRandomLoss
		}
	}
	if reason == "" && first.used+size > first.rules.QueueBytes {
		reason = ReasonQueueFull
	}
	if reason != "" {
		n.record(Event{
			Time: n.now, Seq: seq, From: from, To: to, Bytes: size,
			Status: StatusDropped, Reason: reason, Link: hops[0].Link, Stage: StageLink,
		})
		return SubmitResult{Status: StatusDropped, Reason: reason}, nil
	}
	p := &livePacket{
		id:   n.nextID,
		seq:  seq,
		from: from,
		to:   to,
		dir:  dir,
		data: append([]byte(nil), data...),
		at:   Node(from),
	}
	n.nextID++
	n.live[p.id] = p
	n.record(Event{
		Time: n.now, Seq: seq, From: from, To: to, Bytes: size,
		Status: StatusQueued, Link: hops[0].Link, Stage: StageLink,
	})
	n.enqueueLink(first, p)
	n.tickLinks()
	return SubmitResult{Status: StatusQueued}, nil
}

func (n *Network) random(link *networkLink, dir Direction, seq int) (float64, bool) {
	loss := link.rules.loss(dir)
	var draw float64
	if n.replayAt < len(n.replay) {
		d := n.replay[n.replayAt]
		n.replayAt++
		draw = d.Draw
	} else {
		draw = n.rng[dir].Float64()
	}
	dropped := draw < loss
	n.decisions = append(n.decisions, Decision{
		TimeNS: int64(n.now), Seq: seq, Link: link.spec.ID,
		Draw: draw, LossRate: loss, Dropped: dropped,
	})
	return draw, dropped
}

func (n *Network) enqueueLink(link *networkLink, p *livePacket) {
	link.used += len(p.data)
	p.stage = StageLink
	p.link = link.spec.ID
	p.cur = n.hopFor(link.spec.ID, p)
	link.queues[p.dir] = append(link.queues[p.dir], p)
}

func (n *Network) hopFor(linkID string, p *livePacket) HopSpec {
	for _, hop := range n.paths[p.dir] {
		if hop.Link == linkID {
			return hop
		}
	}
	return HopSpec{Link: linkID, From: p.at}
}

func (n *Network) tickLinks() {
	ids := make([]string, 0, len(n.links))
	for id := range n.links {
		ids = append(ids, id)
	}
	for _, id := range ids {
		n.tryStart(id, DirectionAtoB)
		n.tryStart(id, DirectionBtoA)
	}
}

func (n *Network) tryStart(id string, dir Direction) {
	link := n.links[id]
	if !link.up || len(link.queues[dir]) == 0 || link.busy[dir] > n.now {
		return
	}
	if link.rules.BandwidthBytesPerSecond > 0 {
		other := DirectionBtoA
		if dir == DirectionBtoA {
			other = DirectionAtoB
		}
		if link.busy[other] > n.now {
			return
		}
	}
	p := link.queues[dir][0]
	link.queues[dir] = link.queues[dir][1:]
	serial := serializeTime(len(p.data), link.rules.bandwidth(dir))
	depart := n.now + serial
	arrive := depart + link.rules.Latency
	link.busy[dir] = depart
	p.depart = depart
	p.arrive = arrive
	p.flying = true
	n.token++
	p.token = n.token
	heap.Push(&n.sched, scheduleItem{
		at: arrive, kind: "link_arrive", id: p.id, seq: p.seq,
		link: id, dir: dir, token: p.token, order: n.token,
	})
	n.record(Event{
		Time: n.now, Seq: p.seq, From: p.from, To: p.to, Bytes: len(p.data),
		Status: StatusSerializing, Arrive: arrive, Link: id, Stage: StageLink,
	})
}

// Advance runs all events due by t and returns deliveries with no waiters.
func (n *Network) Advance(t time.Duration) []Delivery {
	n.mu.Lock()
	defer n.mu.Unlock()
	var delivered []Delivery
	for len(n.sched) > 0 && n.sched[0].at <= t {
		item := heap.Pop(&n.sched).(scheduleItem)
		if item.at > n.now {
			n.now = item.at
		}
		p := n.live[item.id]
		if p == nil || p.token != item.token {
			continue
		}
		switch item.kind {
		case "link_arrive":
			delivered = append(delivered, n.linkArrive(p, item)...)
		case "receiver_arrive":
			delivered = append(delivered, n.receiverArrive(p)...)
		}
		n.tickLinks()
	}
	if t > n.now {
		n.now = t
	}
	return delivered
}

func (n *Network) linkArrive(p *livePacket, item scheduleItem) []Delivery {
	link := n.links[item.link]
	link.used -= len(p.data)
	if link.used < 0 {
		link.used = 0
	}
	reached := p.cur.To
	p.at = reached
	p.stage = ""
	p.link = ""
	p.flying = false
	n.record(Event{
		Time: item.at, Seq: p.seq, From: p.from, To: p.to, Bytes: len(p.data),
		Status: StatusHopArrived, Link: item.link, Stage: StageLink,
	})
	if Node(p.to) == reached {
		return n.enterReceiver(p)
	}
	hops := n.paths[p.dir]
	nextIndex := -1
	for i := range hops {
		if hops[i].From == reached {
			nextIndex = i
			break
		}
	}
	if nextIndex < 0 {
		n.dropLive(p, ReasonLinkDown, "", "")
		return nil
	}
	next := hops[nextIndex]
	nextLink := n.links[next.Link]
	size := len(p.data)
	switch {
	case !nextLink.up:
		n.dropLive(p, ReasonLinkDown, next.Link, StageLink)
	case size > nextLink.rules.QueueBytes:
		n.dropLive(p, ReasonPacketTooLarge, next.Link, StageLink)
	case nextLink.rules.loss(p.dir) > 0:
		_, dropped := n.random(nextLink, p.dir, p.seq)
		if dropped {
			n.dropLive(p, ReasonRandomLoss, next.Link, StageLink)
		} else {
			n.enqueueLink(nextLink, p)
			n.tickLinks()
		}
	default:
		n.enqueueLink(nextLink, p)
		n.tickLinks()
	}
	return nil
}

// enterReceiver must be called with the engine lock held.
func (n *Network) enterReceiver(p *livePacket) []Delivery {
	r := n.receivers[p.to]
	if r == nil || r.spec.RateBytesPerSecond == 0 {
		delete(n.live, p.id)
		del := Delivery{Packet: Packet{Seq: p.seq, Data: append([]byte(nil), p.data...)}, Time: n.now}
		n.record(Event{
			Time: n.now, Seq: p.seq, From: p.from, To: p.to, Bytes: len(p.data),
			Status: StatusDelivered, Stage: StageReceiver,
		})
		return n.dispatch(p.to, del)
	}
	size := len(p.data)
	if r.used+size > r.spec.QueueBytes {
		n.dropLive(p, ReasonReceiverFull, "", StageReceiver)
		return nil
	}
	r.used += size
	p.stage = StageReceiver
	p.link = ""
	r.q = append(r.q, p)
	n.tryReceiver(p.to)
	return nil
}

func (n *Network) tryReceiver(dst Endpoint) {
	r := n.receivers[dst]
	if r == nil || len(r.q) == 0 || r.busy > n.now {
		return
	}
	p := r.q[0]
	r.q = r.q[1:]
	r.busy = n.now + serializeTime(len(p.data), r.spec.RateBytesPerSecond)
	n.token++
	p.token = n.token
	heap.Push(&n.sched, scheduleItem{
		at: r.busy, kind: "receiver_arrive", id: p.id, seq: p.seq,
		token: p.token, dir: p.dir, order: n.token,
	})
}

func (n *Network) receiverArrive(p *livePacket) []Delivery {
	r := n.receivers[p.to]
	r.used -= len(p.data)
	if r.used < 0 {
		r.used = 0
	}
	delete(n.live, p.id)
	del := Delivery{Packet: Packet{Seq: p.seq, Data: append([]byte(nil), p.data...)}, Time: n.now}
	n.record(Event{
		Time: n.now, Seq: p.seq, From: p.from, To: p.to, Bytes: len(p.data),
		Status: StatusDelivered, Stage: StageReceiver,
	})
	n.tryReceiver(p.to)
	return n.dispatch(p.to, del)
}

func (n *Network) dispatch(dst Endpoint, del Delivery) []Delivery {
	if waiters := n.waiters[dst]; len(waiters) > 0 {
		waiter := waiters[0]
		n.waiters[dst] = waiters[1:]
		waiter.notify <- del
		return nil
	}
	return []Delivery{del}
}

func (n *Network) dropLive(p *livePacket, reason, linkID, stage string) {
	if p.stage == StageLink && p.link != "" && !p.flying {
		l := n.links[p.link]
		l.used -= len(p.data)
		if l.used < 0 {
			l.used = 0
		}
	} else if p.stage == StageReceiver && !p.flying {
		r := n.receivers[p.to]
		if r != nil {
			r.used -= len(p.data)
			if r.used < 0 {
				r.used = 0
			}
		}
	}
	delete(n.live, p.id)
	n.record(Event{
		Time: n.now, Seq: p.seq, From: p.from, To: p.to, Bytes: len(p.data),
		Status: StatusDropped, Reason: reason, Link: linkID, Stage: stage,
	})
}

func (n *Network) record(e Event) {
	n.events = append(n.events, e)
}

func flowDirection(from, to Endpoint) Direction {
	if from == A && to == B {
		return DirectionAtoB
	}
	return DirectionBtoA
}
