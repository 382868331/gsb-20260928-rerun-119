package link

import (
	"container/heap"
	"math/rand/v2"
	"time"
)

// Snapshot is a complete, JSON-marshalable copy of a network paused at its
// current virtual time. Restoring it twice creates two independent branches
// that can apply different routing decisions from the same pause point.
type Snapshot struct {
	NowNS          int64                         `json:"now_ns"`
	Links          []SnapshotLink                `json:"links"`
	Paths          map[Direction][]HopSpec       `json:"paths,omitempty"`
	Routes         map[Node]map[Node][]string    `json:"routes,omitempty"`
	Receivers      map[Endpoint]SnapshotReceiver `json:"receivers,omitempty"`
	Live           []SnapshotPacket              `json:"live"`
	Sched          []SnapshotItem                `json:"sched"`
	RNG            map[Direction][]byte          `json:"rng"`
	NextID         int64                         `json:"next_id"`
	Token          int64                         `json:"token"`
	Events         []Event                       `json:"events"`
	Decisions      []Decision                    `json:"decisions,omitempty"`
	RouteDecisions []RouteDecision               `json:"route_decisions,omitempty"`
	Replay         []Decision                    `json:"replay,omitempty"`
	ReplayAt       int                           `json:"replay_at,omitempty"`
	Closed         bool                          `json:"closed"`
}

type SnapshotLink struct {
	Spec   LinkSpec              `json:"spec"`
	Rules  SnapshotRules         `json:"rules"`
	Up     bool                  `json:"up"`
	Used   int                   `json:"used"`
	Busy   map[Direction]int64   `json:"busy"`
	Queues map[Direction][]int64 `json:"queues"`
}

// SnapshotRules mirrors LinkRules with the latency in plain nanoseconds.
type SnapshotRules struct {
	LatencyNS               int64   `json:"latency_ns"`
	QueueBytes              int     `json:"queue_bytes"`
	BandwidthBytesPerSecond uint64  `json:"bandwidth_bps,omitempty"`
	BandwidthAtoB           uint64  `json:"bandwidth_a_to_b,omitempty"`
	BandwidthBtoA           uint64  `json:"bandwidth_b_to_a,omitempty"`
	LossRate                float64 `json:"loss_rate,omitempty"`
	LossAtoB                float64 `json:"loss_a_to_b,omitempty"`
	LossBtoA                float64 `json:"loss_b_to_a,omitempty"`
}

type SnapshotReceiver struct {
	Spec ReceiverSpec `json:"spec"`
	Used int          `json:"used"`
	Busy int64        `json:"busy"`
	Q    []int64      `json:"q"`
}

type SnapshotPacket struct {
	ID      int64     `json:"id"`
	Seq     int       `json:"seq"`
	From    Endpoint  `json:"from"`
	To      Endpoint  `json:"to"`
	Dir     Direction `json:"dir"`
	Data    []byte    `json:"data"`
	At      Node      `json:"at"`
	Stage   string    `json:"stage"`
	Link    string    `json:"link"`
	Cur     HopSpec   `json:"cur"`
	Flying  bool      `json:"flying"`
	Depart  int64     `json:"depart"`
	Arrive  int64     `json:"arrive"`
	Token   int64     `json:"token"`
	Visited []Node    `json:"visited,omitempty"`
}

type SnapshotItem struct {
	At    int64     `json:"at"`
	Kind  string    `json:"kind"`
	Seq   int       `json:"seq"`
	ID    int64     `json:"id"`
	Token int64     `json:"token"`
	Link  string    `json:"link"`
	Dir   Direction `json:"dir"`
	Order int64     `json:"order"`
}

func snapshotRules(r LinkRules) SnapshotRules {
	return SnapshotRules{
		LatencyNS:               int64(r.Latency),
		QueueBytes:              r.QueueBytes,
		BandwidthBytesPerSecond: r.BandwidthBytesPerSecond,
		BandwidthAtoB:           r.BandwidthAtoB,
		BandwidthBtoA:           r.BandwidthBtoA,
		LossRate:                r.LossRate,
		LossAtoB:                r.LossAtoB,
		LossBtoA:                r.LossBtoA,
	}
}

func (s SnapshotRules) rules() LinkRules {
	return LinkRules{
		Latency:                 time.Duration(s.LatencyNS),
		QueueBytes:              s.QueueBytes,
		BandwidthBytesPerSecond: s.BandwidthBytesPerSecond,
		BandwidthAtoB:           s.BandwidthAtoB,
		BandwidthBtoA:           s.BandwidthBtoA,
		LossRate:                s.LossRate,
		LossAtoB:                s.LossAtoB,
		LossBtoA:                s.LossBtoA,
	}
}

// Snapshot captures the full paused state. The network stays usable.
func (n *Network) Snapshot() Snapshot {
	n.mu.Lock()
	defer n.mu.Unlock()
	snap := Snapshot{
		NowNS:          int64(n.now),
		Paths:          map[Direction][]HopSpec{},
		Routes:         map[Node]map[Node][]string{},
		Receivers:      map[Endpoint]SnapshotReceiver{},
		RNG:            map[Direction][]byte{},
		NextID:         n.nextID,
		Token:          n.token,
		Events:         append([]Event(nil), n.events...),
		Decisions:      append([]Decision(nil), n.decisions...),
		RouteDecisions: append([]RouteDecision(nil), n.routeDecisions...),
		Replay:         append([]Decision(nil), n.replay...),
		ReplayAt:       n.replayAt,
		Closed:         n.closed,
	}
	for dir, path := range n.paths {
		snap.Paths[dir] = append([]HopSpec(nil), path...)
	}
	for from, byDest := range n.routes {
		snap.Routes[from] = map[Node][]string{}
		for dest, links := range byDest {
			snap.Routes[from][dest] = append([]string(nil), links...)
		}
	}
	for dir, r := range n.pcg {
		data, err := r.MarshalBinary()
		if err == nil {
			snap.RNG[dir] = data
		}
	}
	ids := make([]string, 0, len(n.links))
	for id := range n.links {
		ids = append(ids, id)
	}
	sortStrings(ids)
	for _, id := range ids {
		l := n.links[id]
		sl := SnapshotLink{
			Spec:   l.spec,
			Rules:  snapshotRules(l.rules),
			Up:     l.up,
			Used:   l.used,
			Busy:   map[Direction]int64{},
			Queues: map[Direction][]int64{},
		}
		for dir, busy := range l.busy {
			sl.Busy[dir] = int64(busy)
		}
		for dir, q := range l.queues {
			for _, p := range q {
				sl.Queues[dir] = append(sl.Queues[dir], p.id)
			}
		}
		snap.Links = append(snap.Links, sl)
	}
	for ep, r := range n.receivers {
		sr := SnapshotReceiver{Spec: r.spec, Used: r.used, Busy: int64(r.busy)}
		for _, p := range r.q {
			sr.Q = append(sr.Q, p.id)
		}
		snap.Receivers[ep] = sr
	}
	for _, p := range n.live {
		snap.Live = append(snap.Live, SnapshotPacket{
			ID: p.id, Seq: p.seq, From: p.from, To: p.to, Dir: p.dir,
			Data: append([]byte(nil), p.data...), At: p.at, Stage: p.stage,
			Link: p.link, Cur: p.cur, Flying: p.flying,
			Depart: int64(p.depart), Arrive: int64(p.arrive), Token: p.token,
			Visited: append([]Node(nil), p.visited...),
		})
	}
	for _, item := range n.sched {
		snap.Sched = append(snap.Sched, SnapshotItem{
			At: int64(item.at), Kind: item.kind, Seq: item.seq, ID: item.id,
			Token: item.token, Link: item.link, Dir: item.dir, Order: item.order,
		})
	}
	return snap
}

// RestoreNetwork rebuilds a network from a snapshot. The restored instance
// continues with identical deliveries, events, and routing decisions as the
// original would have, until the caller diverges its actions.
func RestoreNetwork(snap Snapshot) (*Network, error) {
	n := &Network{
		now:            time.Duration(snap.NowNS),
		links:          map[string]*networkLink{},
		paths:          map[Direction][]HopSpec{},
		routes:         map[Node]map[Node][]string{},
		receivers:      map[Endpoint]*receiver{},
		rng:            map[Direction]*rand.Rand{},
		pcg:            map[Direction]*rand.PCG{},
		waiters:        map[Endpoint][]readWaiter{A: {}, B: {}},
		live:           map[int64]*livePacket{},
		nextID:         snap.NextID,
		token:          snap.Token,
		events:         append([]Event(nil), snap.Events...),
		decisions:      append([]Decision(nil), snap.Decisions...),
		routeDecisions: append([]RouteDecision(nil), snap.RouteDecisions...),
		replay:         append([]Decision(nil), snap.Replay...),
		replayAt:       snap.ReplayAt,
		closed:         snap.Closed,
	}
	for dir, path := range snap.Paths {
		n.paths[dir] = append([]HopSpec(nil), path...)
	}
	for from, byDest := range snap.Routes {
		n.routes[from] = map[Node][]string{}
		for dest, links := range byDest {
			n.routes[from][dest] = append([]string(nil), links...)
		}
	}
	for dir, data := range snap.RNG {
		p := rand.NewPCG(0, 0)
		if err := p.UnmarshalBinary(data); err != nil {
			return nil, err
		}
		n.pcg[dir] = p
		n.rng[dir] = rand.New(p)
	}
	for _, dir := range []Direction{DirectionAtoB, DirectionBtoA} {
		if n.rng[dir] == nil {
			n.pcg[dir] = rand.NewPCG(1, 2)
			n.rng[dir] = rand.New(n.pcg[dir])
		}
	}
	for _, sl := range snap.Links {
		l := &networkLink{
			spec:   sl.Spec,
			rules:  sl.Rules.rules(),
			up:     sl.Up,
			used:   sl.Used,
			busy:   map[Direction]time.Duration{},
			queues: map[Direction][]*livePacket{DirectionAtoB: {}, DirectionBtoA: {}},
		}
		for dir, busy := range sl.Busy {
			l.busy[dir] = time.Duration(busy)
		}
		n.links[sl.Spec.ID] = l
	}
	for ep, sr := range snap.Receivers {
		n.receivers[ep] = &receiver{spec: sr.Spec, used: sr.Used, busy: time.Duration(sr.Busy)}
	}
	for _, sp := range snap.Live {
		p := &livePacket{
			id: sp.ID, seq: sp.Seq, from: sp.From, to: sp.To, dir: sp.Dir,
			data: append([]byte(nil), sp.Data...), at: sp.At, stage: sp.Stage,
			link: sp.Link, cur: sp.Cur, flying: sp.Flying,
			depart: time.Duration(sp.Depart), arrive: time.Duration(sp.Arrive),
			token: sp.Token, visited: append([]Node(nil), sp.Visited...),
		}
		n.live[p.id] = p
	}
	for _, sl := range snap.Links {
		l := n.links[sl.Spec.ID]
		for dir, q := range sl.Queues {
			for _, id := range q {
				if p := n.live[id]; p != nil {
					l.queues[dir] = append(l.queues[dir], p)
				}
			}
		}
	}
	for ep, sr := range snap.Receivers {
		r := n.receivers[ep]
		for _, id := range sr.Q {
			if p := n.live[id]; p != nil {
				r.q = append(r.q, p)
			}
		}
	}
	for _, si := range snap.Sched {
		n.sched = append(n.sched, scheduleItem{
			at: time.Duration(si.At), kind: si.Kind, seq: si.Seq, id: si.ID,
			token: si.Token, link: si.Link, dir: si.Dir, order: si.Order,
		})
	}
	heap.Init(&n.sched)
	return n, nil
}

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}
