package link

import (
	"container/heap"
	"math/rand/v2"
	"sync"
	"time"
)

type flight struct {
	seq       int
	from      Endpoint
	to        Endpoint
	data      []byte
	route     []string
	hop       int
	segment   string
	location  string
	departure time.Duration
	arriveAt  time.Duration
	finishAt  time.Duration
	id        int64
}

type flightKey struct {
	from Endpoint
	seq  int
}

type segState struct {
	cfg      SegmentCfg
	used     int
	busyAB   time.Duration
	busyBA   time.Duration
	down     bool
	queued   []*flight
	inflight map[int64]*flight
}

type recvState struct {
	cfg       ReceiverCfg
	used      int
	busyUntil time.Duration
	accepted  []*flight
}

// Network is the deterministic multi-segment virtual network.
type Network struct {
	mu         sync.Mutex
	now        time.Duration
	topo       Topology
	segs       map[string]*segState
	recvs      map[Endpoint]*recvState
	routes     map[Endpoint][]string
	active     map[flightKey]*flight
	events     []Event
	waiters    map[Endpoint][]readWaiter
	delivered  map[Endpoint][]Delivery
	heap       *eventHeap
	closed     bool
	rng        map[string]rand.Source
	nextID     int64
	nextEvent  int64
	decisionFn func(kind, segment string, from Endpoint, seq int, rate float64) bool
	decisions  []Decision
	replay     []Decision
	replayUsed int
	replayErr  error
}

func segmentRngSeed(seed uint64, name string) uint64 {
	h := uint64(14695981039346656037)
	h ^= seed
	h *= 1099511628211
	for i := 0; i < len(name); i++ {
		h ^= uint64(name[i])
		h *= 1099511628211
	}
	h ^= h >> 31
	return h
}

// NewNetwork builds a virtual network from a validated topology.
func NewNetwork(topo Topology, seed uint64, opts ...NetworkOption) (*Network, error) {
	if err := topo.validate(); err != nil {
		return nil, err
	}
	n := &Network{
		topo:      topo,
		segs:      map[string]*segState{},
		recvs:     map[Endpoint]*recvState{},
		routes:    map[Endpoint][]string{},
		active:    map[flightKey]*flight{},
		waiters:   map[Endpoint][]readWaiter{A: {}, B: {}},
		delivered: map[Endpoint][]Delivery{A: {}, B: {}},
		heap:      &eventHeap{},
		rng:       map[string]rand.Source{},
	}
	for name, cfg := range topo.Segments {
		n.segs[name] = &segState{cfg: cfg, inflight: map[int64]*flight{}}
		s := segmentRngSeed(seed, name)
		n.rng[name] = rand.NewPCG(s^0x9e3779b97f4a7c15, s^0xd1b54a32d192ed03)
	}
	for ep, cfg := range topo.Receivers {
		n.recvs[ep] = &recvState{cfg: cfg}
	}
	for ep, route := range topo.Routes {
		n.routes[ep] = append([]string(nil), route...)
	}
	for _, opt := range opts {
		n.applyOption(opt)
	}
	return n, nil
}

func (n *Network) applyOption(opt NetworkOption) { opt(n) }

// NetworkOption configures a Network.
type NetworkOption func(*Network)

// WithReplay pins random decisions to a previously saved decision log.
func WithReplay(decisions []Decision) NetworkOption {
	return func(n *Network) {
		n.replay = append([]Decision(nil), decisions...)
		n.decisionFn = n.rollReplay
	}
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
	n.processDue(n.now)
	size := len(data)
	reason := ""
	switch {
	case size == 0:
		reason = ReasonEmptyPacket
	case n.routeTooLarge(from, size):
		reason = ReasonPacketTooLarge
	}
	if reason != "" {
		n.record(Event{Time: n.now, Seq: seq, From: from, To: to, Bytes: size, Status: StatusDropped, Reason: reason})
		return SubmitResult{Status: StatusDropped, Reason: reason}, nil
	}
	route := append([]string(nil), n.routes[from]...)
	if len(route) == 0 {
		n.record(Event{Time: n.now, Seq: seq, From: from, To: to, Bytes: size, Status: StatusDropped, Reason: ReasonNoRoute})
		return SubmitResult{Status: StatusDropped, Reason: ReasonNoRoute}, nil
	}
	f := &flight{
		seq: seq, from: from, to: to,
		data:  append([]byte(nil), data...),
		route: route, id: n.nextID,
	}
	n.nextID++
	n.active[flightKey{from, seq}] = f
	status, reason := n.enterSegment(f, n.now)
	return SubmitResult{Status: status, Reason: reason, ArrivalTime: f.arriveAt}, nil
}

func (n *Network) routeTooLarge(from Endpoint, size int) bool {
	for _, name := range n.routes[from] {
		if size > n.segs[name].cfg.QueueBytes {
			return true
		}
	}
	return false
}

// rollLoss performs one deterministic loss draw and stores it in the log.
func (n *Network) rollLoss(segName string, f *flight, rate float64) bool {
	if n.decisionFn != nil {
		return n.decisionFn("loss", segName, f.from, f.seq, rate)
	}
	draw := rand.New(n.rng[segName]).Float64()
	lost := draw < rate
	n.decisions = append(n.decisions, Decision{
		ID: int64(len(n.decisions)), Kind: "loss", Segment: segName,
		From: f.from, Seq: f.seq, Draw: draw, Lost: lost,
	})
	return lost
}

func (n *Network) rollReplay(kind, segName string, from Endpoint, seq int, rate float64) bool {
	idx := n.replayUsed
	if idx >= len(n.replay) {
		n.replayErr = ErrReplayMismatch
		return false
	}
	d := n.replay[idx]
	n.replayUsed++
	if d.Kind != kind || d.Segment != segName || d.From != from || d.Seq != seq {
		n.replayErr = ErrReplayMismatch
	}
	lost := d.Draw < rate
	if lost != d.Lost {
		n.replayErr = ErrReplayMismatch
	}
	return d.Lost
}

// enterSegment enqueues f on segment route[hop] at time t. Packets already on a
// segment keep using it; a reroute changes only hops a packet has not entered.
func (n *Network) enterSegment(f *flight, t time.Duration) (string, string) {
	segName := f.route[f.hop]
	seg := n.segs[segName]
	size := len(f.data)
	toward := f.to
	if seg.down {
		n.dropActive(f, ReasonLinkDown, segName, LocationQueue, t)
		return StatusDropped, ReasonLinkDown
	}
	rate := seg.cfg.loss(toward)
	if n.rollLoss(segName, f, rate) {
		n.dropActive(f, ReasonRandomLoss, segName, LocationQueue, t)
		return StatusDropped, ReasonRandomLoss
	}
	if seg.used+size > seg.cfg.QueueBytes {
		n.record(Event{
			Time: t, Seq: f.seq, From: f.from, To: f.to, Bytes: size,
			Status: StatusDropped, Reason: ReasonQueueFull, Segment: segName,
			Location: LocationQueue, Occupancy: seg.used,
		})
		delete(n.active, flightKey{f.from, f.seq})
		return StatusDropped, ReasonQueueFull
	}
	seg.used += size
	f.segment = segName
	f.location = LocationQueue
	seg.queued = append(seg.queued, f)
	n.scheduleQueue(segName, seg, f, t)
	n.record(Event{
		Time: t, Seq: f.seq, From: f.from, To: f.to, Bytes: size,
		Status: StatusQueued, Segment: segName, Location: LocationQueue,
		Occupancy: seg.used, Arrive: f.arriveAt,
	})
	return StatusQueued, ReasonNone
}

// scheduleQueue serializes queued packets FIFO per direction on the shared
// segment and pushes the departure event.
func (n *Network) scheduleQueue(segName string, seg *segState, f *flight, t time.Duration) {
	busy := &seg.busyAB
	if f.to == A {
		busy = &seg.busyBA
	}
	start := *busy
	if start < t {
		start = t
	}
	ser := time.Duration(float64(len(f.data)) / float64(seg.cfg.bandwidth(f.to)) * float64(time.Second))
	f.departure = start + ser
	*busy = f.departure
	f.arriveAt = f.departure + seg.cfg.Latency
	heap.Push(n.heap, heapItem{at: f.departure, ord: n.ord(), kind: evDepart, seg: segName, f: f})
}

func (n *Network) ord() int64 {
	n.nextEvent++
	return n.nextEvent
}

type eventKind int

const (
	evDepart eventKind = iota
	evArrive
	evDrain
)

type heapItem struct {
	at   time.Duration
	ord  int64
	kind eventKind
	seg  string
	dst  Endpoint
	f    *flight
}

type eventHeap []heapItem

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	if h[i].at != h[j].at {
		return h[i].at < h[j].at
	}
	return h[i].ord < h[j].ord
}
func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x any)   { *h = append(*h, x.(heapItem)) }
func (h *eventHeap) Pop() any {
	old := *h
	v := old[len(old)-1]
	*h = old[:len(old)-1]
	return v
}
