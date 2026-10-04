package link

import "time"

// Direction names a flow orientation independently of link endpoint order.
type Direction string

const (
	DirectionAtoB Direction = "a_to_b"
	DirectionBtoA Direction = "b_to_a"
)

const (
	StageLink     = "link"
	StageReceiver = "receiver"
)

// HopSpec identifies one directed traversal of a named link.
type HopSpec struct {
	Link string `json:"link"`
	From Node   `json:"from"`
	To   Node   `json:"to"`
}

// Node is a switch or endpoint inside the in-process topology.
type Node string

// LinkSpec describes one physical-style segment.
//
// BandwidthBytesPerSecond, when positive, is one serializer shared by both
// directions. BandwidthAtoB/BandwidthBtoA instead give each direction its own
// serializer while QueueBytes stays a shared byte budget. Loss rates follow
// the same shared-or-per-direction rule.
type LinkSpec struct {
	ID                      string        `json:"id"`
	Ends                    [2]Node       `json:"ends"`
	Latency                 time.Duration `json:"-"`
	QueueBytes              int           `json:"queue_bytes"`
	BandwidthBytesPerSecond uint64        `json:"bandwidth_bps,omitempty"`
	BandwidthAtoB           uint64        `json:"bandwidth_a_to_b,omitempty"`
	BandwidthBtoA           uint64        `json:"bandwidth_b_to_a,omitempty"`
	LossRate                float64       `json:"loss_rate,omitempty"`
	LossAtoB                float64       `json:"loss_a_to_b,omitempty"`
	LossBtoA                float64       `json:"loss_b_to_a,omitempty"`
}

// ReceiverSpec models a slow application reader at one endpoint.
// RateBytesPerSecond zero means packets are delivered immediately on arrival.
type ReceiverSpec struct {
	RateBytesPerSecond uint64 `json:"rate_bps"`
	QueueBytes         int    `json:"queue_bytes"`
}

// TopologySpec is the deterministic multi-hop network configuration.
type TopologySpec struct {
	Links     []LinkSpec
	Paths     map[Direction][]HopSpec
	Receivers map[Endpoint]ReceiverSpec
}

// Decision is one recorded random draw. Replaying the same draws reproduces
// every event one by one without consulting a PRNG.
type Decision struct {
	TimeNS   int64   `json:"time_ns"`
	Seq      int     `json:"seq"`
	Link     string  `json:"link"`
	Draw     float64 `json:"draw"`
	LossRate float64 `json:"loss_rate"`
	Dropped  bool    `json:"dropped"`
}

// RouteDecision is one recorded next-hop choice. It keeps the facts the
// router knew at decision time: which links were down, which link and next
// node were chosen, and the stop reason when no usable next hop existed.
type RouteDecision struct {
	TimeNS int64    `json:"time_ns"`
	Seq    int      `json:"seq"`
	At     Node     `json:"at"`
	Link   string   `json:"link,omitempty"`
	To     Node     `json:"to,omitempty"`
	Down   []string `json:"down,omitempty"`
	Reason string   `json:"reason,omitempty"`
}

// Fragment locates one unfinished packet inside the topology.
type Fragment struct {
	Seq   int    `json:"seq"`
	Bytes int    `json:"bytes"`
	At    Node   `json:"at"`
	Link  string `json:"link,omitempty"`
	Stage string `json:"stage"`
}

// LinkRules carries the mutable rules of one link.
type LinkRules struct {
	Latency                 time.Duration
	QueueBytes              int
	BandwidthBytesPerSecond uint64
	BandwidthAtoB           uint64
	BandwidthBtoA           uint64
	LossRate                float64
	LossAtoB                float64
	LossBtoA                float64
}

func (s LinkSpec) rules() LinkRules {
	return LinkRules{
		Latency:                 s.Latency,
		QueueBytes:              s.QueueBytes,
		BandwidthBytesPerSecond: s.BandwidthBytesPerSecond,
		BandwidthAtoB:           s.BandwidthAtoB,
		BandwidthBtoA:           s.BandwidthBtoA,
		LossRate:                s.LossRate,
		LossAtoB:                s.LossAtoB,
		LossBtoA:                s.LossBtoA,
	}
}

func (r LinkRules) bandwidth(dir Direction) uint64 {
	if r.BandwidthBytesPerSecond > 0 {
		return r.BandwidthBytesPerSecond
	}
	if dir == DirectionAtoB {
		return r.BandwidthAtoB
	}
	return r.BandwidthBtoA
}

func (r LinkRules) loss(dir Direction) float64 {
	if dir == DirectionAtoB && r.LossAtoB > 0 {
		return r.LossAtoB
	}
	if dir == DirectionBtoA && r.LossBtoA > 0 {
		return r.LossBtoA
	}
	return r.LossRate
}

func validateRules(r LinkRules) error {
	if r.Latency < 0 || r.QueueBytes <= 0 {
		return ErrInvalid
	}
	shared := r.BandwidthBytesPerSecond > 0
	directed := r.BandwidthAtoB > 0 && r.BandwidthBtoA > 0
	if !shared && !directed {
		return ErrInvalid
	}
	for _, loss := range []float64{r.LossRate, r.LossAtoB, r.LossBtoA} {
		if loss < 0 || loss > 1 {
			return ErrInvalid
		}
	}
	return nil
}
