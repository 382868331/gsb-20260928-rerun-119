package link

import (
	"sync"
	"time"
)

const (
	legacyAB = "__a_to_b__"
	legacyBA = "__b_to_a__"
)

// Engine is the legacy single-queue-per-direction facade. It is implemented on
// top of the multi-segment Network and preserves the first-version API.
type Engine struct {
	mu   sync.Mutex
	net  *Network
	cfg  map[Endpoint]Config
	seed *uint64
}

type Option func(*Engine)

// WithSeed makes both directions use distinct deterministic PRNG streams.
func WithSeed(seed uint64) Option {
	return func(e *Engine) { e.seed = &seed }
}

func NewEngine(cfg DirectionConfig, opts ...Option) (*Engine, error) {
	if err := validateConfig(cfg.AtoB); err != nil {
		return nil, err
	}
	if err := validateConfig(cfg.BtoA); err != nil {
		return nil, err
	}
	e := &Engine{cfg: map[Endpoint]Config{A: cfg.AtoB, B: cfg.BtoA}}
	for _, opt := range opts {
		opt(e)
	}
	var seed uint64 = 1
	if e.seed != nil {
		seed = *e.seed
	}
	net, err := NewNetwork(legacyTopo(cfg), seed)
	if err != nil {
		return nil, err
	}
	e.net = net
	return e, nil
}

func legacyTopo(cfg DirectionConfig) Topology {
	segAB := toSegment(cfg.AtoB)
	segBA := toSegment(cfg.BtoA)
	return Topology{
		Segments: map[string]SegmentCfg{legacyAB: segAB, legacyBA: segBA},
		Routes: map[Endpoint][]string{
			A: {legacyAB},
			B: {legacyBA},
		},
	}
}

func toSegment(c Config) SegmentCfg {
	return SegmentCfg{
		Latency:      c.Latency,
		BandwidthBPS: c.BandwidthBytesPerSecond,
		LossRate:     c.LossRate,
		QueueBytes:   c.QueueBytes,
	}
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
	e.cfg[from] = cfg
	name := legacyAB
	if from == B {
		name = legacyBA
	}
	return e.net.UpdateSegment(name, toSegment(cfg))
}

func (e *Engine) Clock() time.Duration { return e.net.Clock() }

func (e *Engine) Events() []Event { return e.net.Events() }

// Send admits one packet at the current virtual time. Data is copied.
func (e *Engine) Send(from Endpoint, seq int, data []byte) (SubmitResult, error) {
	return e.net.Send(from, seq, data)
}

// SetClock moves the virtual clock without delivering packets.
func (e *Engine) SetClock(t time.Duration) { e.net.SetClock(t) }

// Advance moves the virtual clock and delivers every packet due by t.
func (e *Engine) Advance(t time.Duration) []Delivery { return e.net.Advance(t) }

// Wait registers a virtual blocked reader. Close ends the wait with ErrClosed.
func (e *Engine) Wait(dst Endpoint) <-chan Delivery { return e.net.Wait(dst) }

// Close discards queued traffic, records the reason, and ends waits.
func (e *Engine) Close() error { return e.net.Close() }
