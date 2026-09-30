package link

import "time"

// SegmentCfg describes one shared link segment. Capacity and the queue are
// shared by both directions; bandwidth and loss can be overridden per
// direction. Zero-valued direction overrides fall back to the shared values.
type SegmentCfg struct {
	Latency             time.Duration
	BandwidthBPS        uint64
	LossRate            float64
	QueueBytes          int
	BandwidthBPSReverse uint64
	LossRateReverse     float64
}

// ReceiverCfg models a slow receiver. LinkArrivalBytesPerSecond drains the
// network-admission buffer into the application; BufferBytes caps it.
// Zero values mean unlimited, instant delivery.
type ReceiverCfg struct {
	BufferBytes               int
	LinkArrivalBytesPerSecond uint64
}

func (c SegmentCfg) bandwidth(toward Endpoint) uint64 {
	if toward == A && c.BandwidthBPSReverse != 0 {
		return c.BandwidthBPSReverse
	}
	return c.BandwidthBPS
}

func (c SegmentCfg) loss(toward Endpoint) float64 {
	if toward == A && c.LossRateReverse != 0 {
		return c.LossRateReverse
	}
	return c.LossRate
}

func validateSegment(c SegmentCfg) error {
	if c.Latency < 0 || c.QueueBytes <= 0 {
		return ErrInvalid
	}
	if c.BandwidthBPS == 0 {
		return ErrInvalid
	}
	if c.LossRate < 0 || c.LossRate > 1 {
		return ErrInvalid
	}
	if c.LossRateReverse < 0 || c.LossRateReverse > 1 {
		return ErrInvalid
	}
	return nil
}

// Topology is a named set of segments plus the initial routes and receiver
// behavior for each endpoint.
type Topology struct {
	Segments  map[string]SegmentCfg
	Routes    map[Endpoint][]string
	Receivers map[Endpoint]ReceiverCfg
}

func (t Topology) validate() error {
	if len(t.Segments) == 0 {
		return ErrInvalid
	}
	for name, seg := range t.Segments {
		if err := validateSegment(seg); err != nil {
			return err
		}
		if name == "" {
			return ErrInvalid
		}
	}
	for ep, route := range t.Routes {
		if ep != A && ep != B {
			return ErrBadEndpoint
		}
		if len(route) == 0 {
			return ErrNoRoute
		}
		for _, seg := range route {
			if _, ok := t.Segments[seg]; !ok {
				return ErrNoRoute
			}
		}
	}
	for ep, rc := range t.Receivers {
		if ep != A && ep != B {
			return ErrBadEndpoint
		}
		if rc.BufferBytes < 0 {
			return ErrInvalid
		}
	}
	return nil
}
