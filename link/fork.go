package link

import (
	"container/heap"
	"math/rand/v2"
	"sort"
)

// RouteDecisions returns a copy of every recorded next-hop choice, in the
// order the router made them.
func (n *Network) RouteDecisions() []RouteDecision {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]RouteDecision, len(n.routes))
	copy(out, n.routes)
	return out
}

// InFlight lists every unfinished packet and where it currently rests,
// sorted by sequence number for stable comparisons.
func (n *Network) InFlight() []Fragment {
	n.mu.Lock()
	defer n.mu.Unlock()
	frags := make([]Fragment, 0, len(n.live))
	for _, p := range n.live {
		stage := p.stage
		if p.flying {
			stage = "in_flight"
		}
		frags = append(frags, Fragment{
			Seq: p.seq, Bytes: len(p.data), At: p.at, Link: p.link, Stage: stage,
		})
	}
	sort.Slice(frags, func(i, j int) bool { return frags[i].Seq < frags[j].Seq })
	return frags
}

// Fork clones the paused network so two independent routing futures can be
// compared from the same pause point. The clone shares nothing with the
// parent: queued and in-flight packets, link and receiver state, the event
// schedule, recorded facts, and the deterministic random streams are all
// deep-copied. Registered read waiters are not carried over.
func (n *Network) Fork() *Network {
	n.mu.Lock()
	defer n.mu.Unlock()
	m := &Network{
		now:       n.now,
		links:     make(map[string]*networkLink, len(n.links)),
		paths:     make(map[Direction][]HopSpec, len(n.paths)),
		receivers: make(map[Endpoint]*receiver, len(n.receivers)),
		rng:       make(map[Direction]*rand.PCG, len(n.rng)),
		waiters:   map[Endpoint][]readWaiter{A: {}, B: {}},
		live:      make(map[int64]*livePacket, len(n.live)),
		nextID:    n.nextID,
		token:     n.token,
		events:    append([]Event(nil), n.events...),
		decisions: append([]Decision(nil), n.decisions...),
		routes:    append([]RouteDecision(nil), n.routes...),
		replay:    append([]Decision(nil), n.replay...),
		replayAt:  n.replayAt,
		closed:    n.closed,
	}
	for dir, pcg := range n.rng {
		clone := *pcg
		m.rng[dir] = &clone
	}
	for dir, path := range n.paths {
		m.paths[dir] = append([]HopSpec(nil), path...)
	}
	for id, l := range n.links {
		m.links[id] = &networkLink{
			spec:  l.spec,
			rules: l.rules,
			up:    l.up,
			used:  l.used,
			busy:  cloneMap(l.busy),
			queues: map[Direction][]*livePacket{
				DirectionAtoB: {},
				DirectionBtoA: {},
			},
		}
	}
	for id, p := range n.live {
		clone := *p
		clone.data = append([]byte(nil), p.data...)
		clone.visited = cloneMap(p.visited)
		m.live[id] = &clone
	}
	for id, l := range n.links {
		for dir, q := range l.queues {
			for _, p := range q {
				mq := m.links[id].queues[dir]
				m.links[id].queues[dir] = append(mq, m.live[p.id])
			}
		}
	}
	for ep, r := range n.receivers {
		mr := &receiver{spec: r.spec, used: r.used, busy: r.busy}
		for _, p := range r.q {
			mr.q = append(mr.q, m.live[p.id])
		}
		m.receivers[ep] = mr
	}
	m.sched = append(schedule(nil), n.sched...)
	heap.Init(&m.sched)
	return m
}

func cloneMap[K comparable, V any](in map[K]V) map[K]V {
	out := make(map[K]V, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
