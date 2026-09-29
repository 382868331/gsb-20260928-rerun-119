package netdrill

import (
	"bytes"
	"encoding/json"
	"errors"
	"net"
	"reflect"
	"testing"
	"time"
)

func script() []Step {
	return []Step{
		{Offset: 0, From: "A", Payload: []byte("hello-100-bytes-" + string(make([]byte, 84)))},
		{Offset: 50 * time.Millisecond, From: "A", Payload: bytes.Repeat([]byte("x"), 100)},
		{Offset: 60 * time.Millisecond, From: "B", Payload: []byte("reply")},
		{Offset: 70 * time.Millisecond, From: "A", Payload: []byte("third")},
		{Offset: 80 * time.Millisecond, From: "A", Payload: []byte("fourth")},
	}
}

func runOnce(t *testing.T, seed int64) []Event {
	t.Helper()
	clk := NewSimClock()
	cfg := Config{
		Latency:      100 * time.Millisecond,
		BandwidthBps: 800, // 100 bytes/s
		QueueBytes:   250,
		Loss:         0.3,
	}
	s, err := NewSim(cfg, seed, clk)
	if err != nil {
		t.Fatal(err)
	}
	events := Replay(s, clk, script())
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	return events
}

func TestFixedSeedReplayIsIdentical(t *testing.T) {
	first := runOnce(t, 42)
	second := runOnce(t, 42)
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("same seed produced different records\n%v\n%v", first, second)
	}
	other := runOnce(t, 43)
	if reflect.DeepEqual(first, other) {
		t.Fatal("different seeds unexpectedly produced identical records")
	}
}

func TestBandwidthSerializesByActualBytes(t *testing.T) {
	clk := NewSimClock()
	cfg := Config{Latency: 100 * time.Millisecond, BandwidthBps: 800}
	s, err := NewSim(cfg, 1, clk)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := s.Endpoints()

	clk.AfterFunc(0, func() {
		if _, err := a.Write(bytes.Repeat([]byte("a"), 100)); err != nil {
			t.Error(err)
		}
	})
	clk.AdvanceNext() // t=10ms? no: t=0 enqueue, arrival scheduled at 900ms

	clk.AfterFunc(50*time.Millisecond, func() {
		if _, err := a.Write(bytes.Repeat([]byte("b"), 100)); err != nil {
			t.Error(err)
		}
	})
	clk.AdvanceNext() // t=50ms enqueue

	clk.Run()
	events := s.Events()
	var delivers []Event
	for _, e := range events {
		if e.Type == EventDeliver {
			delivers = append(delivers, e)
		}
	}
	if len(delivers) != 2 {
		t.Fatalf("want 2 deliveries, got %d", len(delivers))
	}
	if got := delivers[0].Time.Sub(time.Unix(0, 0)); got != 1100*time.Millisecond {
		t.Fatalf("first arrival = %v, want 1.1s", got)
	}
	// second packet waits for the serializer (free at 1s), sends 1s, +100ms
	if got := delivers[1].Time.Sub(time.Unix(0, 0)); got != 2100*time.Millisecond {
		t.Fatalf("second arrival = %v, want 2.1s", got)
	}
}

func TestQueueExhaustionIsExplicit(t *testing.T) {
	clk := NewSimClock()
	cfg := Config{Latency: 10 * time.Second, QueueBytes: 100}
	s, err := NewSim(cfg, 1, clk)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := s.Endpoints()
	pk := bytes.Repeat([]byte("q"), 100)
	if _, err := a.Write(pk); err != nil {
		t.Fatalf("first packet should fit: %v", err)
	}
	if _, err := a.Write(pk); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("second packet err = %v, want ErrQueueFull", err)
	} else if ne, ok := err.(net.Error); !ok || !ne.Temporary() {
		t.Fatal("ErrQueueFull should be a temporary net.Error")
	}
	drops := 0
	for _, e := range s.Events() {
		if e.Type == EventDrop && e.Reason == ReasonQueueFull {
			drops++
			if e.Bytes != 100 {
				t.Fatalf("drop accounted %d bytes, want 100", e.Bytes)
			}
		}
	}
	if drops != 1 {
		t.Fatalf("want 1 queue-full record, got %d", drops)
	}
	// After the rejected packet the queue is unchanged: a third 100B still fails
	// because the first occupies the queue.
	if _, err := a.Write(pk); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("third packet err = %v, want ErrQueueFull", err)
	}
}

func TestRuleChangeDoesNotMoveQueuedPacket(t *testing.T) {
	clk := NewSimClock()
	cfg := Config{Latency: 100 * time.Millisecond, BandwidthBps: 800}
	s, err := NewSim(cfg, 1, clk)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := s.Endpoints()
	clk.AfterFunc(0, func() { _, _ = a.Write(bytes.Repeat([]byte("z"), 100)) })
	clk.AdvanceNext() // enqueue at t=0, arrival frozen at 900ms

	if err := s.Update(AtoB, Config{Latency: 10 * time.Second, BandwidthBps: 10}); err != nil {
		t.Fatal(err)
	}
	clk.Run()
	for _, e := range s.Events() {
		if e.Type == EventDeliver && e.Time.Sub(time.Unix(0, 0)) != 1100*time.Millisecond {
			t.Fatalf("queued packet moved to %v, want 1.1s", e.Time)
		}
	}
}

func TestCloseEndsWaitingPacketsAndReads(t *testing.T) {
	clk := NewSimClock()
	cfg := Config{Latency: 10 * time.Second}
	s, err := NewSim(cfg, 1, clk)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := s.Endpoints()
	clk.AfterFunc(0, func() { _, _ = a.Write([]byte("late")) })
	clk.AdvanceNext() // enqueue, delivery waiting far in the future
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	clk.Run()
	found := false
	for _, e := range s.Events() {
		if e.Type == EventDrop && e.Reason == ReasonClosed {
			found = true
		}
	}
	if !found {
		t.Fatal("waiting packet was not recorded as link_closed drop")
	}

	// A blocked Read on a closed endpoint returns net.ErrClosed immediately.
	errCh := make(chan error, 1)
	go func() {
		_, err := a.Read(make([]byte, 64))
		errCh <- err
	}()
	select {
	case err := <-errCh:
		if err != net.ErrClosed {
			t.Fatalf("read after close err = %v, want net.ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked Read was not released by Close")
	}
}

func TestEventsJSONRoundTrip(t *testing.T) {
	events := runOnce(t, 7)
	var buf bytes.Buffer
	if err := WriteEvents(&buf, events); err != nil {
		t.Fatal(err)
	}
	// One JSON object per line.
	if lines := bytes.Count(buf.Bytes(), []byte{'\n'}); lines != len(events) {
		t.Fatalf("wrote %d lines for %d events", lines, len(events))
	}
	got, err := ReadEvents(&buf)
	if err != nil {
		t.Fatal(err)
	}
	for i := range got {
		got[i].Payload = ""
	}
	if len(got) != len(events) {
		t.Fatalf("round trip count %d != %d", len(got), len(events))
	}
	var probe Event
	if err := json.Unmarshal([]byte(`{"type":"drop","reason":"random_loss"}`), &probe); err != nil {
		t.Fatal(err)
	}
}

func TestInvalidConfigRejected(t *testing.T) {
	clk := NewSimClock()
	if _, err := NewSim(Config{Loss: 1.5}, 1, clk); err == nil {
		t.Fatal("loss > 1 should be rejected")
	}
	s, err := NewSim(Config{}, 1, clk)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateBoth(Config{Latency: -1}); err == nil {
		t.Fatal("negative latency should be rejected")
	}
}
