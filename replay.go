package netdrill

import (
	"bufio"
	"encoding/json"
	"io"
	"sort"
	"time"
)

// Step is one offered write in a replay script: wait Offset after the replay
// starts, then send Payload from the named endpoint ("A" or "B").
type Step struct {
	Offset  time.Duration
	From    string // "A" or "B"
	Payload []byte
}

// Replay offers a fixed traffic script to the link using the simulation clock.
// Combined with a fixed seed, it produces deterministic arrival order,
// timing, and drop-reason records. All offers are registered up front against
// the virtual clock and fired synchronously while the clock runs, so no real
// goroutine scheduling can perturb the result.
func Replay(s *Sim, clk *SimClock, steps []Step) []Event {
	schedule(s, clk, steps)
	clk.Run()
	return s.Events()
}

// ReplayAsync drives the same script in fixed ticks while caller goroutines
// Read from the endpoints, so deliveries land in endpoint inboxes during the
// run. For record-only checks prefer Replay.
func ReplayAsync(s *Sim, clk *SimClock, steps []Step, tick time.Duration) {
	schedule(s, clk, steps)
	for clk.Pending() {
		clk.Advance(tick)
	}
}

func schedule(s *Sim, clk *SimClock, steps []Step) {
	a, b := s.Endpoints()
	offers := make([]Step, len(steps))
	copy(offers, steps)
	sort.SliceStable(offers, func(i, j int) bool { return offers[i].Offset < offers[j].Offset })
	for _, st := range offers {
		ep := a
		if st.From == "B" {
			ep = b
		}
		payload := st.Payload
		clk.AfterFunc(st.Offset, func() { _, _ = ep.Write(payload) })
	}
}

// WriteEvents writes the traffic record as JSON Lines, one Event per line.
func WriteEvents(w io.Writer, events []Event) error {
	bw := bufio.NewWriter(w)
	enc := json.NewEncoder(bw)
	for _, e := range events {
		if err := enc.Encode(e); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// ReadEvents loads a JSON Lines traffic record produced by WriteEvents.
func ReadEvents(r io.Reader) ([]Event, error) {
	dec := json.NewDecoder(r)
	var events []Event
	for {
		var e Event
		if err := dec.Decode(&e); err != nil {
			if err == io.EOF {
				return events, nil
			}
			return nil, err
		}
		events = append(events, e)
	}
}
