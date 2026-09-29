package link

import (
	"math/rand/v2"
	"sort"
	"sync"
	"time"
)

// Engine is an in-process deterministic virtual link. It never touches the
// operating-system network stack.
type Engine struct {
	mu      sync.Mutex
	now     time.Duration
	cfg     map[Endpoint]Config
	queues  map[Endpoint][]queuedPacket
	used    map[Endpoint]int
	busy    map[Endpoint]time.Duration
	rng     map[Endpoint]*rand.Rand
	waiters map[Endpoint][]readWaiter
	events  []Event
	closed  bool
}

type Option func(*Engine)

// WithSeed makes both directions use distinct deterministic PRNG streams.
func WithSeed(seed uint64) Option {
	return func(e *Engine) {
		e.rng[A] = rand.New(rand.NewPCG(seed, seed+1))
		e.rng[B] = rand.New(rand.NewPCG(seed+2, seed+3))
	}
}

func NewEngine(cfg DirectionConfig, opts ...Option) (*Engine, error) {
	if err := validateConfig(cfg.AtoB); err != nil {
		return nil, err
	}
	if err := validateConfig(cfg.BtoA); err != nil {
		return nil, err
	}
	e := &Engine{
		cfg:     map[Endpoint]Config{A: cfg.AtoB, B: cfg.BtoA},
		queues:  map[Endpoint][]queuedPacket{A: {}, B: {}},
		used:    map[Endpoint]int{A: 0, B: 0},
		busy:    map[Endpoint]time.Duration{A: 0, B: 0},
		rng:     map[Endpoint]*rand.Rand{A: rand.New(rand.NewPCG(1, 2)), B: rand.New(rand.NewPCG(3, 4))},
		waiters: map[Endpoint][]readWaiter{A: {}, B: {}},
	}
	for _, opt := range opts {
		opt(e)
	}
	return e, nil
}

// Update changes rules for traffic leaving from. Already admitted packets keep
// their scheduled serialization and arrival times.
func (e *Engine) Update(from Endpoint, cfg Config) error {
	if from != A && from != B {
		return ErrBadEndpoint
	}
	if err := validateConfig(cfg); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrClosed
	}
	e.cfg[from] = cfg
	return nil
}

func (e *Engine) Clock() time.Duration {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.now
}

func (e *Engine) Events() []Event {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]Event, len(e.events))
	copy(out, e.events)
	return out
}

// Send admits one packet at the current virtual time. Data is copied.
func (e *Engine) Send(from Endpoint, seq int, data []byte) (SubmitResult, error) {
	to, err := peer(from)
	if err != nil {
		return SubmitResult{}, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return SubmitResult{Status: StatusClosed, Reason: ReasonLinkClosed}, ErrClosed
	}

	size := len(data)
	c := e.cfg[from]
	e.releaseQueueBytes(from, e.now)
	reason := ""
	if size == 0 {
		reason = ReasonEmptyPacket
	} else if size > c.QueueBytes {
		reason = ReasonPacketTooLarge
	} else if e.rng[from].Float64() < c.LossRate {
		reason = ReasonRandomLoss
	} else if e.used[from]+size > c.QueueBytes {
		reason = ReasonQueueFull
	}
	if reason != "" {
		e.events = append(e.events, Event{
			Time: e.now, Seq: seq, From: from, To: to, Bytes: size,
			Status: StatusDropped, Reason: reason,
		})
		return SubmitResult{Status: StatusDropped, Reason: reason}, nil
	}

	payload := append([]byte(nil), data...)
	start := e.busy[from]
	if start < e.now {
		start = e.now
	}
	serialization := serializeTime(size, c.BandwidthBytesPerSecond)
	departure := start + serialization
	arrival := departure + c.Latency
	e.busy[from] = departure
	e.used[from] += size
	e.queues[from] = append(e.queues[from], queuedPacket{
		seq: seq, data: payload, departure: departure, arrival: arrival,
	})
	e.events = append(e.events, Event{
		Time: e.now, Seq: seq, From: from, To: to, Bytes: size,
		Status: StatusQueued, Arrive: arrival,
	})
	return SubmitResult{Status: StatusQueued, ArrivalTime: arrival}, nil
}

// SetClock moves the virtual clock without delivering packets. It is used by
// replay drivers before injecting a timed action.
func (e *Engine) SetClock(t time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if t > e.now {
		e.now = t
	}
}

// Advance moves the virtual clock and delivers every packet due by t.
func (e *Engine) Advance(t time.Duration) []Delivery {
	e.mu.Lock()
	defer e.mu.Unlock()
	if t > e.now {
		e.now = t
	}
	for from := range e.queues {
		e.releaseQueueBytes(from, e.now)
	}

	type item struct {
		p  queuedPacket
		to Endpoint
	}
	var due []item
	for from := range e.queues {
		to, _ := peer(from)
		q := e.queues[from]
		for len(q) > 0 {
			if q[0].arrival > e.now {
				break
			}
			if !q[0].released {
				e.used[from] -= len(q[0].data)
				if e.used[from] < 0 {
					e.used[from] = 0
				}
			}
			due = append(due, item{p: q[0], to: to})
			q = q[1:]
		}
		e.queues[from] = q
	}
	sort.SliceStable(due, func(i, j int) bool {
		if due[i].p.arrival != due[j].p.arrival {
			return due[i].p.arrival < due[j].p.arrival
		}
		return due[i].to == B && due[j].to == A
	})

	delivered := make([]Delivery, 0, len(due))
	for _, d := range due {
		packet := Packet{Seq: d.p.seq, Data: append([]byte(nil), d.p.data...)}
		del := Delivery{Packet: packet, Time: d.p.arrival}
		e.events = append(e.events, Event{
			Time: d.p.arrival, Seq: packet.Seq, From: other(d.to), To: d.to,
			Bytes: len(packet.Data), Status: StatusDelivered, Arrive: d.p.arrival,
		})
		if waiters := e.waiters[d.to]; len(waiters) > 0 {
			waiter := waiters[0]
			e.waiters[d.to] = waiters[1:]
			waiter.notify <- del
		} else {
			delivered = append(delivered, del)
		}
	}
	return delivered
}

// Wait registers a virtual blocked reader. Close ends the wait with ErrClosed.
func (e *Engine) Wait(dst Endpoint) <-chan Delivery {
	ch := make(chan Delivery, 1)
	e.mu.Lock()
	defer e.mu.Unlock()
	if dst != A && dst != B {
		ch <- Delivery{Err: ErrBadEndpoint}
		return ch
	}
	if e.closed {
		ch <- Delivery{Time: e.now, Err: ErrClosed}
		return ch
	}
	e.waiters[dst] = append(e.waiters[dst], readWaiter{dst: dst, notify: ch})
	return ch
}

// Close discards queued traffic, records the reason, and ends waits.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	for from, q := range e.queues {
		to, _ := peer(from)
		for _, p := range q {
			e.events = append(e.events, Event{
				Time: e.now, Seq: p.seq, From: from, To: to, Bytes: len(p.data),
				Status: StatusDropped, Reason: ReasonClosedWhileQueue, Arrive: p.arrival,
			})
		}
		e.queues[from] = nil
		e.used[from] = 0
	}
	for dst, waiters := range e.waiters {
		for _, waiter := range waiters {
			waiter.notify <- Delivery{Time: e.now, Err: ErrClosed}
		}
		e.waiters[dst] = nil
	}
	return nil
}

// releaseQueueBytes frees capacity once serialization finishes. Packets that
// have left the queue remain in flight until their arrival time.
func (e *Engine) releaseQueueBytes(from Endpoint, now time.Duration) {
	q := e.queues[from]
	keep := q[:0]
	for _, p := range q {
		if p.departure <= now {
			if p.released {
				continue
			}
			p.released = true
			e.used[from] -= len(p.data)
			if e.used[from] < 0 {
				e.used[from] = 0
			}
		}
		keep = append(keep, p)
	}
	e.queues[from] = keep
}

func peer(from Endpoint) (Endpoint, error) {
	switch from {
	case A:
		return B, nil
	case B:
		return A, nil
	default:
		return "", ErrBadEndpoint
	}
}

func other(v Endpoint) Endpoint {
	if v == A {
		return B
	}
	return A
}
