package netdrill

import (
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var errClosed = net.ErrClosed

type chunk struct {
	data []byte
	at   time.Time
}

// Endpoint is one local end of the virtual link. It satisfies net.Conn so
// ordinary transport code can be exercised without any real network.
type Endpoint struct {
	name string
	sim  *Sim

	mu     sync.Mutex
	closed atomic.Bool
	inbox  []chunk
	wake   chan struct{} // closed/replaced on inbox or lifecycle changes
	rdDeadline
	wrDeadline
}

type rdDeadline = deadline
type wrDeadline = deadline

type deadline struct {
	mu sync.Mutex
	t  time.Time
}

func (d *deadline) set(t time.Time) {
	d.mu.Lock()
	d.t = t
	d.mu.Unlock()
}

func newEndpoint(name string, sim *Sim) *Endpoint {
	return &Endpoint{name: name, sim: sim, wake: make(chan struct{})}
}

// Read waits for and copies one delivered packet (datagram semantics; the
// excess bytes of an oversize packet are discarded), or up to len(b).
func (e *Endpoint) Read(b []byte) (int, error) {
	for {
		if e.closed.Load() {
			return 0, errClosed
		}
		e.mu.Lock()
		if len(e.inbox) > 0 {
			c := e.inbox[0]
			n := copy(b, c.data)
			e.inbox = e.inbox[1:]
			e.mu.Unlock()
			return n, nil
		}
		wake := e.wake
		e.mu.Unlock()

		var timeout <-chan time.Time
		e.rdDeadline.mu.Lock()
		if !e.rdDeadline.t.IsZero() {
			d := e.rdDeadline.t.Sub(e.sim.clock.Now())
			if d <= 0 {
				e.rdDeadline.mu.Unlock()
				return 0, timeoutError()
			}
			t := e.sim.clock.After(d)
			timeout = t
		}
		e.rdDeadline.mu.Unlock()

		select {
		case <-wake:
		case <-timeout:
			return 0, timeoutError()
		}
	}
}

// Write sends b as one packet through the virtual link. Queue exhaustion is
// reported as ErrQueueFull (a temporary error) and also recorded as a drop.
func (e *Endpoint) Write(b []byte) (int, error) {
	if e.closed.Load() {
		return 0, errClosed
	}
	wakeLen := len(b)
	_, err := e.sim.send(e.name, b)
	if err != nil {
		return 0, err
	}
	return wakeLen, nil
}

// Close shuts this endpoint down. Packets still in flight are finalized by
// Sim.Close; this only stops local blocked reads/writes.
func (e *Endpoint) Close() error {
	if e.closed.Swap(true) {
		return nil
	}
	e.closeNotify()
	return nil
}

func (e *Endpoint) closeNotify() {
	e.mu.Lock()
	e.closed.Store(true)
	close(e.wake)
	e.wake = make(chan struct{})
	e.mu.Unlock()
}

func (e *Endpoint) deliver(payload string, at time.Time) {
	if e.closed.Load() {
		return
	}
	e.mu.Lock()
	e.inbox = append(e.inbox, chunk{data: []byte(payload), at: at})
	close(e.wake)
	e.wake = make(chan struct{})
	e.mu.Unlock()
}

// LocalAddr / RemoteAddr return fixed dummy addresses; no real interface is
// used.
func (e *Endpoint) LocalAddr() net.Addr { return dummyAddr(e.name) }
func (e *Endpoint) RemoteAddr() net.Addr {
	other := "B"
	if e.name == "B" {
		other = "A"
	}
	return dummyAddr(other)
}

// SetDeadline sets both read and write deadlines.
func (e *Endpoint) SetDeadline(t time.Time) error {
	e.SetReadDeadline(t)
	return e.SetWriteDeadline(t)
}

// SetReadDeadline sets the deadline for future Read calls.
func (e *Endpoint) SetReadDeadline(t time.Time) error {
	e.rdDeadline.set(t)
	return nil
}

// SetWriteDeadline sets the deadline for future Write calls. Writes do not
// block under backpressure (a full queue returns ErrQueueFull immediately), so
// it is accepted for net.Conn compatibility but has no waiting effect.
func (e *Endpoint) SetWriteDeadline(t time.Time) error {
	e.wrDeadline.set(t)
	return nil
}

type dummyAddr string

func (d dummyAddr) Network() string { return "netdrill" }
func (d dummyAddr) String() string  { return "virtual-" + string(d) }

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "netdrill: i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func timeoutError() error { return timeoutErr{} }
