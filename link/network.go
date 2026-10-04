package link

import (
	"container/heap"
	"math/rand/v2"
	"sync"
	"time"
)

// Network is an in-process multi-hop virtual network. No sockets, proxies, or
// host network rules are used.
type Network struct {
	mu sync.Mutex

	now       time.Duration
	links     map[string]*networkLink
	paths     map[Direction][]HopSpec
	receivers map[Endpoint]*receiver
	rng       map[Direction]*rand.PCG
	waiters   map[Endpoint][]readWaiter

	live      map[int64]*livePacket
	sched     schedule
	nextID    int64
	token     int64
	events    []Event
	decisions []Decision
	routes    []RouteDecision
	replay    []Decision
	replayAt  int
	closed    bool
}

type networkLink struct {
	spec   LinkSpec
	rules  LinkRules
	up     bool
	used   int
	busy   map[Direction]time.Duration
	queues map[Direction][]*livePacket
}

type receiver struct {
	spec ReceiverSpec
	used int
	busy time.Duration
	q    []*livePacket
}

type livePacket struct {
	id      int64
	seq     int
	from    Endpoint
	to      Endpoint
	dir     Direction
	data    []byte
	at      Node
	visited map[Node]bool
	stage   string
	link    string
	cur     HopSpec
	flying  bool
	depart  time.Duration
	arrive  time.Duration
	token   int64
}

type scheduleItem struct {
	at    time.Duration
	kind  string
	seq   int
	id    int64
	token int64
	link  string
	dir   Direction
	order int64
}

type schedule []scheduleItem

func (s schedule) Len() int { return len(s) }
func (s schedule) Less(i, j int) bool {
	if s[i].at != s[j].at {
		return s[i].at < s[j].at
	}
	if s[i].seq != s[j].seq {
		return s[i].seq < s[j].seq
	}
	return s[i].order < s[j].order
}
func (s schedule) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s *schedule) Push(x any)   { *s = append(*s, x.(scheduleItem)) }
func (s *schedule) Pop() any {
	old := *s
	n := len(old)
	v := old[n-1]
	*s = old[:n-1]
	return v
}

type NetworkOption func(*Network)

// WithNetworkSeed makes flow directions use distinct deterministic streams.
func WithNetworkSeed(seed uint64) NetworkOption {
	return func(n *Network) {
		n.rng[DirectionAtoB] = rand.NewPCG(seed, seed+1)
		n.rng[DirectionBtoA] = rand.NewPCG(seed+2, seed+3)
	}
}

// WithRecordedDecisions replays saved draws instead of calling the PRNG.
func WithRecordedDecisions(decisions []Decision) NetworkOption {
	return func(n *Network) { n.replay = append([]Decision(nil), decisions...) }
}

// NewNetwork validates and constructs a multi-hop network.
func NewNetwork(spec TopologySpec, opts ...NetworkOption) (*Network, error) {
	n := &Network{
		links:     map[string]*networkLink{},
		paths:     map[Direction][]HopSpec{},
		receivers: map[Endpoint]*receiver{},
		rng:       map[Direction]*rand.PCG{},
		waiters:   map[Endpoint][]readWaiter{A: {}, B: {}},
		live:      map[int64]*livePacket{},
	}
	for _, opt := range opts {
		opt(n)
	}
	if n.rng[DirectionAtoB] == nil {
		n.rng[DirectionAtoB] = rand.NewPCG(1, 2)
		n.rng[DirectionBtoA] = rand.NewPCG(3, 4)
	}

	for i := range spec.Links {
		ls := spec.Links[i]
		if ls.ID == "" || ls.Ends[0] == ls.Ends[1] {
			return nil, ErrInvalid
		}
		if _, exists := n.links[ls.ID]; exists {
			return nil, ErrInvalid
		}
		rules := ls.rules()
		if err := validateRules(rules); err != nil {
			return nil, err
		}
		n.links[ls.ID] = &networkLink{
			spec:   ls,
			rules:  rules,
			up:     true,
			busy:   map[Direction]time.Duration{},
			queues: map[Direction][]*livePacket{DirectionAtoB: {}, DirectionBtoA: {}},
		}
	}
	for dir, path := range spec.Paths {
		if dir != DirectionAtoB && dir != DirectionBtoA {
			return nil, ErrBadEndpoint
		}
		if err := n.checkPath(dir, path); err != nil {
			return nil, err
		}
		n.paths[dir] = append([]HopSpec(nil), path...)
	}
	if len(n.paths) != 2 {
		return nil, ErrInvalid
	}
	for endpoint, rs := range spec.Receivers {
		if endpoint != A && endpoint != B || rs.QueueBytes <= 0 {
			return nil, ErrInvalid
		}
		n.receivers[endpoint] = &receiver{spec: rs}
	}
	heap.Init(&n.sched)
	return n, nil
}

func (n *Network) checkPath(dir Direction, path []HopSpec) error {
	var start, end Node
	if dir == DirectionAtoB {
		start, end = Node(A), Node(B)
	} else {
		start, end = Node(B), Node(A)
	}
	return n.checkHops(path, start, end)
}
