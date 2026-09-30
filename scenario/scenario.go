package scenario

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"network-drill/link"
)

type Config struct {
	LatencyMS    int     `json:"latency_ms"`
	BandwidthBPS uint64  `json:"bandwidth_bps"`
	LossRate     float64 `json:"loss_rate"`
	QueueBytes   int     `json:"queue_bytes"`
}

// SegmentJSON is one shared link segment. Shared fields apply to both
// directions; the reverse fields override bandwidth and loss toward A.
type SegmentJSON struct {
	LatencyMS           int     `json:"latency_ms"`
	BandwidthBPS        uint64  `json:"bandwidth_bps"`
	LossRate            float64 `json:"loss_rate"`
	QueueBytes          int     `json:"queue_bytes"`
	BandwidthBPSReverse uint64  `json:"bandwidth_bps_reverse,omitempty"`
	LossRateReverse     float64 `json:"loss_rate_reverse,omitempty"`
}

type ReceiverJSON struct {
	BufferBytes  int    `json:"buffer_bytes"`
	DrainRateBPS uint64 `json:"drain_rate_bps"`
}

type Scenario struct {
	Seed       uint64                  `json:"seed"`
	DurationMS int                     `json:"duration_ms"`
	AToB       Config                  `json:"a_to_b"`
	BToA       Config                  `json:"b_to_a"`
	Segments   map[string]SegmentJSON  `json:"segments,omitempty"`
	Routes     map[string][]string     `json:"routes,omitempty"`
	Receivers  map[string]ReceiverJSON `json:"receivers,omitempty"`
	Replay     []link.Decision         `json:"replay,omitempty"`
	Actions    []Action                `json:"actions"`
}

type Action struct {
	TimeMS     int         `json:"time_ms"`
	Op         string      `json:"op"`
	From       string      `json:"from,omitempty"`
	Seq        int         `json:"seq,omitempty"`
	Size       int         `json:"size,omitempty"`
	Data       string      `json:"data,omitempty"`
	Segment    string      `json:"segment,omitempty"`
	Route      []string    `json:"route,omitempty"`
	Legacy     Config      `json:"-"`
	SegmentCfg SegmentJSON `json:"segment_cfg,omitempty"`
}

func (a *Action) UnmarshalJSON(raw []byte) error {
	type flat Action
	var f flat
	if err := json.Unmarshal(raw, &f); err != nil {
		return err
	}
	*a = Action(f)
	if err := json.Unmarshal(raw, &a.Legacy); err != nil {
		return err
	}
	var seg SegmentJSON
	if err := json.Unmarshal(raw, &seg); err == nil && seg.BandwidthBPS != 0 {
		a.SegmentCfg = seg
	}
	return nil
}

type Result struct {
	Seed      uint64          `json:"seed"`
	Replayed  bool            `json:"replayed"`
	Events    []link.Event    `json:"events"`
	Decisions []link.Decision `json:"decisions,omitempty"`
}

func Load(path string) (*Scenario, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s Scenario
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, err
	}
	if s.DurationMS < 0 {
		return nil, fmt.Errorf("duration_ms must be non-negative")
	}
	return &s, nil
}

func (s *Scenario) topology() (link.Topology, bool, error) {
	if len(s.Segments) == 0 {
		return link.Topology{}, false, nil
	}
	topo := link.Topology{
		Segments:  map[string]link.SegmentCfg{},
		Routes:    map[link.Endpoint][]string{},
		Receivers: map[link.Endpoint]link.ReceiverCfg{},
	}
	for name, seg := range s.Segments {
		topo.Segments[name] = link.SegmentCfg{
			Latency:             time.Duration(seg.LatencyMS) * time.Millisecond,
			BandwidthBPS:        seg.BandwidthBPS,
			LossRate:            seg.LossRate,
			QueueBytes:          seg.QueueBytes,
			BandwidthBPSReverse: seg.BandwidthBPSReverse,
			LossRateReverse:     seg.LossRateReverse,
		}
	}
	for ep, names := range s.Routes {
		e, err := endpoint(ep)
		if err != nil {
			return topo, true, err
		}
		topo.Routes[e] = names
	}
	for ep, rc := range s.Receivers {
		e, err := endpoint(ep)
		if err != nil {
			return topo, true, err
		}
		topo.Receivers[e] = link.ReceiverCfg{
			BufferBytes:               rc.BufferBytes,
			LinkArrivalBytesPerSecond: rc.DrainRateBPS,
		}
	}
	return topo, true, nil
}

// runner abstracts the legacy engine and the multi-segment network.
type runner interface {
	Advance(time.Duration) []link.Delivery
	Close() error
	Clock() time.Duration
	Events() []link.Event
}

func (s *Scenario) Run() (*Result, error) {
	actions := append([]Action(nil), s.Actions...)
	sort.SliceStable(actions, func(i, j int) bool { return actions[i].TimeMS < actions[j].TimeMS })

	topo, multi, err := s.topology()
	if err != nil {
		return nil, err
	}

	result := &Result{Seed: s.Seed, Replayed: len(s.Replay) > 0}
	if multi {
		opts := []link.NetworkOption{}
		if len(s.Replay) > 0 {
			opts = append(opts, link.WithReplay(s.Replay))
		}
		net, err := link.NewNetwork(topo, s.Seed, opts...)
		if err != nil {
			return nil, err
		}
		defer net.Close()
		for _, action := range actions {
			if err := advanceTo(net, action.TimeMS); err != nil {
				return nil, err
			}
			if err := runNetworkAction(net, action); err != nil {
				return nil, err
			}
		}
		endAt := time.Duration(s.DurationMS) * time.Millisecond
		if endAt > net.Clock() {
			net.Advance(endAt)
		}
		if err := net.ReplayError(); err != nil {
			return nil, err
		}
		result.Events = net.Events()
		result.Decisions = net.Decisions()
		return result, nil
	}

	engine, err := link.NewEngine(link.DirectionConfig{
		AtoB: toLinkConfig(s.AToB),
		BtoA: toLinkConfig(s.BToA),
	}, link.WithSeed(s.Seed))
	if err != nil {
		return nil, err
	}
	defer engine.Close()
	for _, action := range actions {
		if err := advanceTo(engine, action.TimeMS); err != nil {
			return nil, err
		}
		if err := runAction(engine, action); err != nil {
			return nil, err
		}
	}
	endAt := time.Duration(s.DurationMS) * time.Millisecond
	if endAt > engine.Clock() {
		engine.Advance(endAt)
	}
	result.Events = engine.Events()
	return result, nil
}

func advanceTo(r runner, ms int) error {
	if ms < 0 {
		return fmt.Errorf("action time_ms must be non-negative")
	}
	r.Advance(time.Duration(ms) * time.Millisecond)
	return nil
}

func runNetworkAction(net *link.Network, action Action) error {
	switch action.Op {
	case "send":
		from, err := endpoint(action.From)
		if err != nil {
			return err
		}
		payload, err := payload(action)
		if err != nil {
			return err
		}
		_, err = net.Send(from, action.Seq, payload)
		return err
	case "update":
		if action.Segment != "" {
			return net.UpdateSegment(action.Segment, toSegmentCfg(action.SegmentCfg))
		}
		from, err := endpoint(action.From)
		if err != nil {
			return err
		}
		_ = from
		return fmt.Errorf("update in topology mode requires segment")
	case "reroute":
		from, err := endpoint(action.From)
		if err != nil {
			return err
		}
		return net.Reroute(from, action.Route)
	case "break":
		return net.Break(action.Segment)
	case "recover":
		return net.Recover(action.Segment)
	case "close":
		return net.Close()
	default:
		return fmt.Errorf("unknown op %q", action.Op)
	}
}

func runAction(engine *link.Engine, action Action) error {
	switch action.Op {
	case "send":
		from, err := endpoint(action.From)
		if err != nil {
			return err
		}
		payload, err := payload(action)
		if err != nil {
			return err
		}
		_, err = engine.Send(from, action.Seq, payload)
		return err
	case "update":
		from, err := endpoint(action.From)
		if err != nil {
			return err
		}
		return engine.Update(from, toLinkConfig(action.Legacy))
	case "close":
		return engine.Close()
	default:
		return fmt.Errorf("unknown op %q", action.Op)
	}
}

func toSegmentCfg(s SegmentJSON) link.SegmentCfg {
	return link.SegmentCfg{
		Latency:             time.Duration(s.LatencyMS) * time.Millisecond,
		BandwidthBPS:        s.BandwidthBPS,
		LossRate:            s.LossRate,
		QueueBytes:          s.QueueBytes,
		BandwidthBPSReverse: s.BandwidthBPSReverse,
		LossRateReverse:     s.LossRateReverse,
	}
}

func toLinkConfig(c Config) link.Config {
	return link.Config{
		Latency:                 time.Duration(c.LatencyMS) * time.Millisecond,
		BandwidthBytesPerSecond: c.BandwidthBPS,
		LossRate:                c.LossRate,
		QueueBytes:              c.QueueBytes,
	}
}

func endpoint(value string) (link.Endpoint, error) {
	switch link.Endpoint(value) {
	case link.A:
		return link.A, nil
	case link.B:
		return link.B, nil
	default:
		return "", link.ErrBadEndpoint
	}
}

func payload(action Action) ([]byte, error) {
	if action.Data != "" && action.Size != 0 {
		return nil, fmt.Errorf("action seq=%d cannot set both data and size", action.Seq)
	}
	if action.Data != "" {
		return []byte(action.Data), nil
	}
	if action.Size <= 0 {
		return nil, fmt.Errorf("action seq=%d requires positive size or non-empty data", action.Seq)
	}
	return make([]byte, action.Size), nil
}
