package link

import (
	"container/heap"
	"time"
)

// SetClock moves the virtual clock without processing due events. It exists
// for drivers that inject actions at fixed times.
func (n *Network) SetClock(t time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if t > n.now {
		n.now = t
	}
}

// Advance runs every event due by t and delivers packets to virtual readers.
func (n *Network) Advance(t time.Duration) []Delivery {
	n.mu.Lock()
	defer n.mu.Unlock()
	if t < n.now {
		t = n.now
	}
	for n.heap.Len() > 0 {
		item := (*n.heap)[0]
		if item.at > t {
			break
		}
		heap.Pop(n.heap)
		n.now = item.at
		switch item.kind {
		case evDepart:
			n.onDepart(item)
		case evArrive:
			n.onArrive(item)
		case evDrain:
			n.onDrain(item)
		}
	}
	if t > n.now {
		n.now = t
	}
	out := make([]Delivery, 0)
	for _, ep := range []Endpoint{A, B} {
		dels := n.takeDeliveries(ep)
		for _, d := range dels {
			if waiter := n.popWaiter(ep); waiter != nil {
				waiter <- d
			} else {
				out = append(out, d)
			}
		}
	}
	return out
}

// processDue runs every event due by t without touching waiters. It keeps the
// FIFO serializers honest when several sends share one virtual timestamp.
func (n *Network) processDue(t time.Duration) {
	for n.heap.Len() > 0 {
		item := (*n.heap)[0]
		if item.at > t {
			break
		}
		heap.Pop(n.heap)
		n.now = item.at
		switch item.kind {
		case evDepart:
			n.onDepart(item)
		case evArrive:
			n.onArrive(item)
		case evDrain:
			n.onDrain(item)
		}
	}
}

func (n *Network) takeDeliveries(ep Endpoint) []Delivery {
	dels := n.delivered[ep]
	n.delivered[ep] = nil
	sortDeliveries(dels)
	return dels
}

func sortDeliveries(d []Delivery) {
	// insertion order is already deterministic; stable sort by time keeps
	// arrival order explicit.
	for i := 1; i < len(d); i++ {
		for j := i; j > 0 && d[j].Time < d[j-1].Time; j-- {
			d[j], d[j-1] = d[j-1], d[j]
		}
	}
}

func (n *Network) popWaiter(ep Endpoint) chan<- Delivery {
	w := n.waiters[ep]
	if len(w) == 0 {
		return nil
	}
	ch := w[0].notify
	n.waiters[ep] = w[1:]
	return ch
}

// onDepart moves the head packet of a shared segment out of its FIFO queue.
func (n *Network) onDepart(item heapItem) {
	f := item.f
	if f == nil || f.location != LocationQueue || f.segment != item.seg {
		return
	}
	seg := n.segs[item.seg]
	if seg.down {
		n.dropActive(f, ReasonLinkDown, item.seg, LocationQueue, n.now)
		return
	}
	if len(seg.queued) == 0 || seg.queued[0].id != f.id {
		return
	}
	seg.queued = seg.queued[1:]
	seg.used -= len(f.data)
	if seg.used < 0 {
		seg.used = 0
	}
	f.location = LocationInFlight
	seg.inflight[f.id] = f
	heap.Push(n.heap, heapItem{at: f.arriveAt, ord: n.ord(), kind: evArrive, seg: item.seg, f: f})
}

// onArrive finishes propagation on one segment and either enters the next hop
// or admits the packet into the destination receiver.
func (n *Network) onArrive(item heapItem) {
	f := item.f
	if f == nil || f.location != LocationInFlight || f.segment != item.seg {
		return
	}
	seg := n.segs[item.seg]
	delete(seg.inflight, f.id)
	if seg.down {
		n.dropActive(f, ReasonLinkDown, item.seg, LocationInFlight, n.now)
		return
	}
	f.hop++
	if f.hop < len(f.route) {
		n.enterSegment(f, n.now)
		return
	}
	n.acceptAtReceiver(f, n.now)
}

// acceptAtReceiver applies the slow-receiver admission buffer. Unlimited
// receivers deliver immediately; full buffers drop with receiver_full.
func (n *Network) acceptAtReceiver(f *flight, t time.Duration) {
	rc, ok := n.recvs[f.to]
	size := len(f.data)
	if !ok || (rc.cfg.BufferBytes == 0 && rc.cfg.LinkArrivalBytesPerSecond == 0) {
		n.deliver(f, t, t, LocationReceiver, 0)
		return
	}
	if rc.cfg.BufferBytes > 0 && rc.used+size > rc.cfg.BufferBytes {
		n.dropActive(f, ReasonReceiverFull, "", LocationReceiver, t)
		return
	}
	rc.used += size
	f.location = LocationReceiver
	f.segment = ""
	rc.accepted = append(rc.accepted, f)
	rate := rc.cfg.LinkArrivalBytesPerSecond
	var finish time.Duration
	if rate == 0 {
		finish = t
	} else {
		start := rc.busyUntil
		if start < t {
			start = t
		}
		drain := time.Duration(float64(size) / float64(rate) * float64(time.Second))
		finish = start + drain
		rc.busyUntil = finish
	}
	f.finishAt = finish
	n.record(Event{
		Time: t, Seq: f.seq, From: f.from, To: f.to, Bytes: size,
		Status: StatusQueued, Location: LocationReceiver, Occupancy: rc.used,
		Arrive: finish,
	})
	heap.Push(n.heap, heapItem{at: finish, ord: n.ord(), kind: evDrain, dst: f.to, f: f})
}

// onDrain hands one receiver-accepted packet to the application FIFO.
func (n *Network) onDrain(item heapItem) {
	f := item.f
	if f == nil || f.location != LocationReceiver {
		return
	}
	rc := n.recvs[item.dst]
	if rc == nil || len(rc.accepted) == 0 || rc.accepted[0].id != f.id {
		return
	}
	rc.accepted = rc.accepted[1:]
	rc.used -= len(f.data)
	if rc.used < 0 {
		rc.used = 0
	}
	n.deliver(f, item.at, item.at, LocationReceiver, rc.used)
}

func (n *Network) deliver(f *flight, at, finish time.Duration, loc string, occupancy int) {
	n.record(Event{
		Time: finish, Seq: f.seq, From: f.from, To: f.to, Bytes: len(f.data),
		Status: StatusDelivered, Location: loc, Occupancy: occupancy, Arrive: finish,
	})
	delete(n.active, flightKey{f.from, f.seq})
	n.delivered[f.to] = append(n.delivered[f.to], Delivery{
		Packet: Packet{Seq: f.seq, Data: append([]byte(nil), f.data...)},
		Time:   at,
	})
}

func (n *Network) dropActive(f *flight, reason, segName, loc string, t time.Duration) {
	size := len(f.data)
	switch loc {
	case LocationQueue:
		if seg := n.segs[segName]; seg != nil {
			if f.location == LocationQueue {
				keep := seg.queued[:0]
				for _, q := range seg.queued {
					if q.id == f.id {
						seg.used -= size
						if seg.used < 0 {
							seg.used = 0
						}
						continue
					}
					keep = append(keep, q)
				}
				seg.queued = keep
			}
		}
	case LocationInFlight:
		if seg := n.segs[segName]; seg != nil {
			delete(seg.inflight, f.id)
		}
	case LocationReceiver:
		if rc := n.recvs[f.to]; rc != nil {
			keep := rc.accepted[:0]
			for _, q := range rc.accepted {
				if q.id == f.id {
					rc.used -= size
					if rc.used < 0 {
						rc.used = 0
					}
					continue
				}
				keep = append(keep, q)
			}
			rc.accepted = keep
		}
	}
	f.location = StatusDropped
	n.record(Event{
		Time: t, Seq: f.seq, From: f.from, To: f.to, Bytes: size,
		Status: StatusDropped, Reason: reason, Segment: segName, Location: loc,
	})
	delete(n.active, flightKey{f.from, f.seq})
}

func (n *Network) record(ev Event) { n.events = append(n.events, ev) }
