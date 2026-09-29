package link_test

import (
	"testing"
	"time"

	"network-drill/link"
)

func testConfig() link.DirectionConfig {
	c := link.Config{
		Latency:                 100 * time.Millisecond,
		BandwidthBytesPerSecond: 100,
		LossRate:                0,
		QueueBytes:              100,
	}
	return link.DirectionConfig{AtoB: c, BtoA: c}
}

func TestBandwidthLatencyAndQueueCapacity(t *testing.T) {
	engine, err := link.NewEngine(testConfig(), link.WithSeed(1))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	first, err := engine.Send(link.A, 1, make([]byte, 100))
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != link.StatusQueued || first.ArrivalTime != 1100*time.Millisecond {
		t.Fatalf("unexpected first result: %+v", first)
	}
	second, err := engine.Send(link.A, 2, make([]byte, 100))
	if err != nil {
		t.Fatal(err)
	}
	if second.Status != link.StatusDropped || second.Reason != link.ReasonQueueFull {
		t.Fatalf("unexpected second result: %+v", second)
	}

	delivered := engine.Advance(1100 * time.Millisecond)
	if len(delivered) != 1 || delivered[0].Packet.Seq != 1 || delivered[0].Time != 1100*time.Millisecond {
		t.Fatalf("unexpected deliveries: %+v", delivered)
	}

	third, err := engine.Send(link.A, 3, make([]byte, 100))
	if err != nil {
		t.Fatal(err)
	}
	if third.Status != link.StatusQueued || third.ArrivalTime != 2200*time.Millisecond {
		t.Fatalf("unexpected third result: %+v", third)
	}
}

func TestUpdateDoesNotMoveQueuedPacketIntoPast(t *testing.T) {
	engine, err := link.NewEngine(testConfig(), link.WithSeed(1))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	before, err := engine.Send(link.A, 1, make([]byte, 100))
	if err != nil {
		t.Fatal(err)
	}
	engine.Advance(500 * time.Millisecond)
	updated := link.Config{
		Latency:                 time.Millisecond,
		BandwidthBytesPerSecond: 100000,
		LossRate:                0,
		QueueBytes:              1000,
	}
	if err := engine.Update(link.A, updated); err != nil {
		t.Fatal(err)
	}
	if got := engine.Advance(501 * time.Millisecond); len(got) != 0 {
		t.Fatalf("old packet arrived early: %+v", got)
	}
	if got := engine.Advance(before.ArrivalTime); len(got) != 1 {
		t.Fatalf("old packet did not keep arrival time: %+v", got)
	}
}

func TestFixedSeedReplayIsDeterministic(t *testing.T) {
	run := func() []link.Event {
		cfg := testConfig()
		cfg.AtoB.LossRate = 0.5
		engine, err := link.NewEngine(cfg, link.WithSeed(99))
		if err != nil {
			t.Fatal(err)
		}
		for seq := 1; seq <= 12; seq++ {
			if _, err := engine.Send(link.A, seq, make([]byte, 50)); err != nil {
				t.Fatal(err)
			}
		}
		engine.Advance(time.Hour)
		events := engine.Events()
		if err := engine.Close(); err != nil {
			t.Fatal(err)
		}
		return events
	}

	first := run()
	second := run()
	if len(first) != len(second) {
		t.Fatalf("event count mismatch: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("event %d differs: %+v vs %+v", i, first[i], second[i])
		}
	}
}

func TestCloseEndsWaitAndRecordsQueuedLoss(t *testing.T) {
	engine, err := link.NewEngine(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Send(link.A, 1, make([]byte, 10)); err != nil {
		t.Fatal(err)
	}
	wait := engine.Wait(link.B)
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	delivery := <-wait
	if delivery.Err != link.ErrClosed {
		t.Fatalf("expected closed wait, got %+v", delivery)
	}
	events := engine.Events()
	last := events[len(events)-1]
	if last.Status != link.StatusDropped || last.Reason != link.ReasonClosedWhileQueue {
		t.Fatalf("unexpected close event: %+v", last)
	}
}
