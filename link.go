// Package netdrill builds an in-process virtual network link for traffic
// quality drills. Two local endpoints exchange data through a configurable
// delay / bandwidth / loss pipeline; the host's real network stack is never
// touched.
package netdrill

import (
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"
)

// Direction identifies one way of the bidirectional link.
type Direction int

const (
	AtoB Direction = iota
	BtoA
)

// Config configures one direction of the virtual link. A zero BandwidthBps or
// QueueBytes means unlimited. Loss is the independent drop probability in
// [0,1], evaluated per packet against the link's deterministic RNG.
type Config struct {
	Latency      time.Duration
	BandwidthBps int64 // bits per second consumed by actual packet bytes
	QueueBytes   int   // total bytes that may wait in the queue
	Loss         float64
}

// DropReason says why a packet never arrived.
type DropReason string

const (
	ReasonRandomLoss DropReason = "random_loss" // selected by the loss die roll
	ReasonQueueFull  DropReason = "queue_full"  // queue capacity exhausted at enqueue
	ReasonClosed     DropReason = "link_closed" // Close ended the wait before delivery
)

// EventType tags a recorded traffic event.
type EventType string

const (
	EventEnqueue EventType = "enqueue"
	EventDeliver EventType = "deliver"
	EventDrop    EventType = "drop"
)

// Event is one recorded traffic fact. Timestamps come from the configured
// Clock, so a SimClock replay produces byte-identical event streams.
type Event struct {
	Seq       int64      `json:"seq"`
	Direction Direction  `json:"direction"`
	Type      EventType  `json:"type"`
	Time      time.Time  `json:"time"`
	Bytes     int        `json:"bytes"`
	ArriveAt  time.Time  `json:"arrive_at,omitempty"`
	Reason    DropReason `json:"reason,omitempty"`
	Payload   string     `json:"payload,omitempty"`
}

// ErrQueueFull is returned (and matched with errors.Is) by Write when a
// packet is rejected because the direction's byte queue is exhausted. It is
// a temporary net.Error.
var ErrQueueFull error = &queueFullError{}

type queueFullError struct{}

func (*queueFullError) Error() string   { return "netdrill: virtual link queue full" }
func (*queueFullError) Timeout() bool   { return false }
func (*queueFullError) Temporary() bool { return true }

type queuedPacket struct {
	event    Event
	arriveAt time.Time
}

type pipe struct {
	mu  sync.Mutex
	cfg Config
	// txFinish is when the egress serializer becomes free. It is computed at
	// enqueue time, freezing the schedule even if the config changes later.
	txFinish time.Time
}

func (p *pipe) validate() error {
	c := p.cfg
	if c.Latency < 0 {
		return errors.New("netdrill: latency must be >= 0")
	}
	if c.BandwidthBps < 0 {
		return errors.New("netdrill: bandwidth must be >= 0")
	}
	if c.QueueBytes < 0 {
		return errors.New("netdrill: queue capacity must be >= 0")
	}
	if c.Loss < 0 || c.Loss > 1 {
		return errors.New("netdrill: loss must be in [0,1]")
	}
	return nil
}

// Sim is a bidirectional virtual link plus its traffic record.
type Sim struct {
	clock Clock
	rng   *rand.Rand

	mu     sync.Mutex
	closed bool
	pipes  [2]pipe
	queued [2]int // bytes currently buffered per direction
	events []Event
	seq    int64
	a, b   *Endpoint
}

// NewSim creates a link with the same initial Config in both directions. The
// fixed seed makes every run replay identically when the same traffic is
// offered at the same virtual offsets.
func NewSim(cfg Config, seed int64, clock Clock) (*Sim, error) {
	if clock == nil {
		clock = WallClock{}
	}
	s := &Sim{
		clock: clock,
		rng:   rand.New(rand.NewSource(seed)),
	}
	s.pipes[AtoB].cfg = cfg
	s.pipes[BtoA].cfg = cfg
	if err := s.pipes[AtoB].validate(); err != nil {
		return nil, err
	}
	s.a = newEndpoint("A", s)
	s.b = newEndpoint("B", s)
	return s, nil
}

// Endpoints returns the two local ends. Data written to A arrives at B and
// vice versa; nothing leaves the process.
func (s *Sim) Endpoints() (*Endpoint, *Endpoint) { return s.a, s.b }

// Config returns the current rule set for one direction.
func (s *Sim) Config(d Direction) Config {
	s.pipes[d].mu.Lock()
	defer s.pipes[d].mu.Unlock()
	return s.pipes[d].cfg
}

// Update changes the rules for one direction. New values apply only to packets
// enqueued afterwards: already queued packets keep their frozen arrival times.
func (s *Sim) Update(d Direction, cfg Config) error {
	p := &s.pipes[d]
	p.mu.Lock()
	old := p.cfg
	p.cfg = cfg
	if err := p.validate(); err != nil {
		p.cfg = old
		p.mu.Unlock()
		return err
	}
	p.mu.Unlock()
	return nil
}

// UpdateBoth applies cfg to both directions.
func (s *Sim) UpdateBoth(cfg Config) error {
	if err := s.Update(AtoB, cfg); err != nil {
		return err
	}
	return s.Update(BtoA, cfg)
}

// Events returns a defensive copy of the recorded traffic log.
func (s *Sim) Events() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Event, len(s.events))
	copy(out, s.events)
	return out
}

// Close ends the drill. Packets still waiting are recorded as drops with
// ReasonClosed and blocked reads/writes return net.ErrClosed immediately.
func (s *Sim) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	s.a.closeNotify()
	s.b.closeNotify()
	return nil
}

func (s *Sim) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *Sim) record(e Event) {
	s.mu.Lock()
	s.events = append(s.events, e)
	s.mu.Unlock()
}

// send enqueues one packet on the writer's outgoing direction. It returns the
// packet's frozen arrival time on success, or ErrQueueFull on rejection.
func (s *Sim) send(writer string, payload []byte) (time.Time, error) {
	if len(payload) == 0 {
		return time.Time{}, errors.New("netdrill: empty packets are not supported")
	}
	d := AtoB
	if writer == "B" {
		d = BtoA
	}
	p := &s.pipes[d]

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return time.Time{}, errClosed
	}
	s.seq++
	seq := s.seq
	s.queued[d] += len(payload)
	queued := s.queued[d]
	s.mu.Unlock()

	e := Event{
		Seq:       seq,
		Direction: d,
		Type:      EventEnqueue,
		Time:      s.clock.Now(),
		Bytes:     len(payload),
		Payload:   string(payload),
	}

	p.mu.Lock()
	cfg := p.cfg
	p.mu.Unlock()

	// Random loss is rolled at enqueue and consumes one RNG draw, so the loss
	// sequence stays reproducible whether or not the queue is contended.
	drop := cfg.Loss > 0 && s.rng.Float64() < cfg.Loss

	if drop {
		s.mu.Lock()
		s.queued[d] -= len(payload)
		s.mu.Unlock()
		e.Type = EventDrop
		e.Reason = ReasonRandomLoss
		s.record(e)
		return time.Time{}, nil
	}

	if cfg.QueueBytes > 0 && queued > cfg.QueueBytes {
		s.mu.Lock()
		s.queued[d] -= len(payload)
		s.mu.Unlock()
		e.Type = EventDrop
		e.Reason = ReasonQueueFull
		s.record(e)
		return time.Time{}, ErrQueueFull
	}

	// Freeze the schedule at enqueue. Bandwidth cost is the actual byte count;
	// later config changes cannot move this packet earlier or later.
	now := s.clock.Now()
	p.mu.Lock()
	txStart := now
	if p.txFinish.After(txStart) {
		txStart = p.txFinish
	}
	var tx time.Duration
	if cfg.BandwidthBps > 0 {
		tx = time.Duration(math.Ceil(float64(len(payload)*8) / float64(cfg.BandwidthBps) * float64(time.Second)))
	}
	arrive := txStart.Add(tx + cfg.Latency)
	p.txFinish = txStart.Add(tx)
	p.mu.Unlock()

	e.ArriveAt = arrive
	s.record(e)

	s.clock.AfterFunc(arrive.Sub(now), func() { s.deliver(queuedPacket{event: e, arriveAt: arrive}) })

	return arrive, nil
}

func (s *Sim) deliver(pk queuedPacket) {
	s.mu.Lock()
	if s.closed {
		s.queued[pk.event.Direction] -= pk.event.Bytes
		s.mu.Unlock()
		e := pk.event
		e.Type = EventDrop
		e.Reason = ReasonClosed
		e.Time = s.clock.Now()
		s.record(e)
		return
	}
	s.queued[pk.event.Direction] -= pk.event.Bytes
	s.mu.Unlock()

	dst := s.a
	if pk.event.Direction == AtoB {
		dst = s.b
	}
	e := pk.event
	e.Type = EventDeliver
	e.Time = pk.arriveAt
	s.record(e)
	dst.deliver(pk.event.Payload, pk.arriveAt)
}
