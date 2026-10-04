package scenario

import (
	"fmt"
	"sort"
	"time"

	"network-drill/link"
)

type NetworkConfig struct {
	ID            string   `json:"id"`
	Ends          []string `json:"ends"`
	LatencyMS     int      `json:"latency_ms"`
	QueueBytes    int      `json:"queue_bytes"`
	BandwidthBPS  uint64   `json:"bandwidth_bps,omitempty"`
	BandwidthAtoB uint64   `json:"bandwidth_a_to_b,omitempty"`
	BandwidthBtoA uint64   `json:"bandwidth_b_to_a,omitempty"`
	LossRate      float64  `json:"loss_rate,omitempty"`
	LossAtoB      float64  `json:"loss_a_to_b,omitempty"`
	LossBtoA      float64  `json:"loss_b_to_a,omitempty"`
}

type Hop struct {
	Link string `json:"link"`
	From string `json:"from"`
	To   string `json:"to"`
}

type ReceiverConfig struct {
	RateBPS    uint64 `json:"rate_bps"`
	QueueBytes int    `json:"queue_bytes"`
}

type NetworkScenario struct {
	Version    int                       `json:"version"`
	Seed       uint64                    `json:"seed"`
	DurationMS int                       `json:"duration_ms"`
	Links      []NetworkConfig           `json:"links"`
	Paths      map[string][]Hop          `json:"paths"`
	Receivers  map[string]ReceiverConfig `json:"receivers"`
	Actions    []NetworkAction           `json:"actions"`
	Decisions  []link.Decision           `json:"decisions,omitempty"`
}

type NetworkAction struct {
	TimeMS int `json:"time_ms"`
	ActionCommon
	Path string `json:"path,omitempty"`
	Hops []Hop  `json:"hops,omitempty"`
	NetworkConfig
}

type ActionCommon struct {
	Op   string `json:"op"`
	From string `json:"from,omitempty"`
	Seq  int    `json:"seq,omitempty"`
	Size int    `json:"size,omitempty"`
	Data string `json:"data,omitempty"`
	Link string `json:"link,omitempty"`
}

func (s *Scenario) runV2() (*Result, error) {
	spec := link.TopologySpec{
		Links:     make([]link.LinkSpec, 0, len(s.Links)),
		Paths:     map[link.Direction][]link.HopSpec{},
		Receivers: map[link.Endpoint]link.ReceiverSpec{},
	}
	for _, l := range s.Links {
		if len(l.Ends) != 2 || l.LatencyMS < 0 {
			return nil, fmt.Errorf("invalid link %q", l.ID)
		}
		spec.Links = append(spec.Links, link.LinkSpec{
			ID:                      l.ID,
			Ends:                    [2]link.Node{link.Node(l.Ends[0]), link.Node(l.Ends[1])},
			Latency:                 time.Duration(l.LatencyMS) * time.Millisecond,
			QueueBytes:              l.QueueBytes,
			BandwidthBytesPerSecond: l.BandwidthBPS,
			BandwidthAtoB:           l.BandwidthAtoB,
			BandwidthBtoA:           l.BandwidthBtoA,
			LossRate:                l.LossRate,
			LossAtoB:                l.LossAtoB,
			LossBtoA:                l.LossBtoA,
		})
	}
	for name, hops := range s.Paths {
		dir, err := direction(name)
		if err != nil {
			return nil, err
		}
		for _, h := range hops {
			spec.Paths[dir] = append(spec.Paths[dir], link.HopSpec{
				Link: h.Link, From: link.Node(h.From), To: link.Node(h.To),
			})
		}
	}
	for name, rc := range s.Receivers {
		ep, err := endpoint(name)
		if err != nil {
			return nil, err
		}
		spec.Receivers[ep] = link.ReceiverSpec{
			RateBytesPerSecond: rc.RateBPS,
			QueueBytes:         rc.QueueBytes,
		}
	}
	engine, err := link.NewNetwork(spec, link.WithNetworkSeed(s.Seed), link.WithRecordedDecisions(s.Decisions))
	if err != nil {
		return nil, err
	}
	defer engine.Close()

	actions := append([]NetworkAction(nil), s.NetActions...)
	sort.SliceStable(actions, func(i, j int) bool { return actions[i].TimeMS < actions[j].TimeMS })
	for _, action := range actions {
		if action.TimeMS < 0 {
			return nil, fmt.Errorf("action time_ms must be non-negative")
		}
		engine.Advance(time.Duration(action.TimeMS) * time.Millisecond)
		if err := runNetworkAction(engine, action); err != nil {
			return nil, err
		}
	}
	endAt := time.Duration(s.DurationMS) * time.Millisecond
	if endAt > engine.Clock() {
		engine.Advance(endAt)
	}
	return &Result{
		Seed: s.Seed, Events: engine.Events(),
		Decisions: engine.Decisions(), Routes: engine.RouteDecisions(),
	}, nil
}

func runNetworkAction(engine *link.Network, action NetworkAction) error {
	switch action.Op {
	case "send":
		from, err := endpoint(action.From)
		if err != nil {
			return err
		}
		payload, err := payload(Action{
			Seq: action.Seq, Size: action.Size, Data: action.Data,
		})
		if err != nil {
			return err
		}
		_, err = engine.Send(from, action.Seq, payload)
		return err
	case "update_link":
		return engine.UpdateLink(action.Link, link.LinkRules{
			Latency:                 time.Duration(action.LatencyMS) * time.Millisecond,
			QueueBytes:              action.QueueBytes,
			BandwidthBytesPerSecond: action.BandwidthBPS,
			BandwidthAtoB:           action.BandwidthAtoB,
			BandwidthBtoA:           action.BandwidthBtoA,
			LossRate:                action.LossRate,
			LossAtoB:                action.LossAtoB,
			LossBtoA:                action.LossBtoA,
		})
	case "link_down":
		return engine.SetLinkDown(action.Link)
	case "link_up":
		return engine.SetLinkUp(action.Link)
	case "update_path":
		dir, err := direction(action.Path)
		if err != nil {
			return err
		}
		hops := make([]link.HopSpec, 0, len(action.Hops))
		for _, h := range action.Hops {
			hops = append(hops, link.HopSpec{Link: h.Link, From: link.Node(h.From), To: link.Node(h.To)})
		}
		return engine.UpdatePath(dir, hops)
	case "close":
		return engine.Close()
	default:
		return fmt.Errorf("unknown op %q", action.Op)
	}
}

func direction(value string) (link.Direction, error) {
	switch link.Direction(value) {
	case link.DirectionAtoB:
		return link.DirectionAtoB, nil
	case link.DirectionBtoA:
		return link.DirectionBtoA, nil
	default:
		return "", link.ErrBadEndpoint
	}
}
