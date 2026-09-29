package link

import (
	"errors"
	"time"
)

// Endpoint identifies one of the two in-process endpoints.
type Endpoint string

const (
	A Endpoint = "a"
	B Endpoint = "b"
)

const (
	StatusQueued    = "queued"
	StatusDelivered = "delivered"
	StatusDropped   = "dropped"
	StatusClosed    = "closed"
)

const (
	ReasonNone             = ""
	ReasonRandomLoss       = "random_loss"
	ReasonQueueFull        = "queue_full"
	ReasonPacketTooLarge   = "packet_too_large"
	ReasonEmptyPacket      = "empty_packet"
	ReasonLinkClosed       = "link_closed"
	ReasonClosedWhileQueue = "closed_while_waiting"
)

var (
	ErrClosed      = errors.New("link closed")
	ErrInvalid     = errors.New("invalid link configuration")
	ErrBadEndpoint = errors.New("unknown endpoint")
)

// Packet is one independently queued network-layer payload.
type Packet struct {
	Seq  int
	Data []byte
}

// Config applies to one traffic direction. A zero value is not valid because
// bandwidth must be positive.
type Config struct {
	Latency                 time.Duration
	BandwidthBytesPerSecond uint64
	LossRate                float64
	QueueBytes              int
}

// DirectionConfig configures A->B and B->A independently.
type DirectionConfig struct {
	AtoB Config
	BtoA Config
}

// Event is a durable traffic-record entry. Times use the simulator clock.
type Event struct {
	Time   time.Duration `json:"time_ns"`
	Seq    int           `json:"seq"`
	From   Endpoint      `json:"from"`
	To     Endpoint      `json:"to"`
	Bytes  int           `json:"bytes"`
	Status string        `json:"status"`
	Reason string        `json:"reason,omitempty"`
	Arrive time.Duration `json:"arrival_time_ns,omitempty"`
}

// SubmitResult reports the unambiguous outcome of one send.
type SubmitResult struct {
	Status      string
	Reason      string
	ArrivalTime time.Duration
}

type queuedPacket struct {
	seq       int
	data      []byte
	departure time.Duration
	arrival   time.Duration
	released  bool
}

type readWaiter struct {
	dst    Endpoint
	notify chan<- Delivery
}

// Delivery is sent to a virtual blocked reader.
type Delivery struct {
	Packet Packet
	Time   time.Duration
	Err    error
}
