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

type Scenario struct {
	Seed       uint64   `json:"seed"`
	DurationMS int      `json:"duration_ms"`
	AToB       Config   `json:"a_to_b"`
	BToA       Config   `json:"b_to_a"`
	Actions    []Action `json:"actions"`
}

type Action struct {
	TimeMS int    `json:"time_ms"`
	Op     string `json:"op"`
	From   string `json:"from,omitempty"`
	Seq    int    `json:"seq,omitempty"`
	Size   int    `json:"size,omitempty"`
	Data   string `json:"data,omitempty"`
	Config
}

type Result struct {
	Seed   uint64       `json:"seed"`
	Events []link.Event `json:"events"`
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

func (s *Scenario) Run() (*Result, error) {
	engine, err := link.NewEngine(link.DirectionConfig{
		AtoB: toLinkConfig(s.AToB),
		BtoA: toLinkConfig(s.BToA),
	}, link.WithSeed(s.Seed))
	if err != nil {
		return nil, err
	}
	defer engine.Close()

	actions := append([]Action(nil), s.Actions...)
	sort.SliceStable(actions, func(i, j int) bool { return actions[i].TimeMS < actions[j].TimeMS })
	for _, action := range actions {
		if action.TimeMS < 0 {
			return nil, fmt.Errorf("action time_ms must be non-negative")
		}
		at := time.Duration(action.TimeMS) * time.Millisecond
		engine.Advance(at)
		if err := runAction(engine, action); err != nil {
			return nil, err
		}
	}
	endAt := time.Duration(s.DurationMS) * time.Millisecond
	if endAt > engine.Clock() {
		engine.Advance(endAt)
	}
	return &Result{Seed: s.Seed, Events: engine.Events()}, nil
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
		return engine.Update(from, toLinkConfig(action.Config))
	case "close":
		return engine.Close()
	default:
		return fmt.Errorf("unknown op %q", action.Op)
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
