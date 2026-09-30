package scenario_test

import (
	"path/filepath"
	"testing"
	"time"

	"network-drill/link"
	"network-drill/scenario"
)

func loadResult(t *testing.T, name string, replay []link.Decision) *scenario.Result {
	t.Helper()
	s, err := scenario.Load(filepath.Join("..", "examples", name))
	if err != nil {
		t.Fatal(err)
	}
	if replay != nil {
		s.Replay = replay
	}
	result, err := s.Run()
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestLegacyExampleKeepsQueuedScheduleAndFixesLostDelivery(t *testing.T) {
	result := loadResult(t, "queue.json", nil)
	var seq1 *link.Event
	var seq3 *link.Event
	for i := range result.Events {
		e := result.Events[i]
		if e.Seq == 1 && e.Status == link.StatusDelivered {
			seq1 = &e
		}
		if e.Seq == 3 && e.Status == link.StatusDelivered {
			seq3 = &e
		}
	}
	// seq1 keeps its old serialization+propagation schedule (100ms
	// serialization at 1000 B/s + 100ms propagation = 200ms); the update at
	// 150ms cannot pull it forward into the past.
	if seq1 == nil || seq1.Time != 200*time.Millisecond {
		t.Fatalf("seq1 delivery not preserved: %+v", seq1)
	}
	if seq3 == nil || seq3.Time != 1010*time.Millisecond {
		t.Fatalf("seq3 should use the fast updated link: %+v", seq3)
	}
}

func TestMultipathTrafficCheck(t *testing.T) {
	result := loadResult(t, "multipath.json", nil)

	delivered := map[int]bool{}
	dropped := map[int]string{}
	maxQueueOccupancy := map[string]int{}
	maxReceiver := 0
	for _, e := range result.Events {
		if e.Status == link.StatusQueued {
			if e.Location == link.LocationQueue && e.Occupancy > maxQueueOccupancy[e.Segment] {
				maxQueueOccupancy[e.Segment] = e.Occupancy
			}
			if e.Location == link.LocationReceiver && e.Occupancy > maxReceiver {
				maxReceiver = e.Occupancy
			}
		}
		if e.Status == link.StatusDelivered {
			delivered[e.Seq] = true
		}
		if e.Status == link.StatusDropped {
			dropped[e.Seq] = e.Reason
		}
	}

	// Reverse direction with its own (faster, loss-free) rules delivers.
	if !delivered[10] {
		t.Fatal("reverse packet 10 must deliver over b->a route")
	}
	// seq2 was admitted into the receiver buffer and is still draining at close.
	if dropped[2] != link.ReasonClosedWhileQueue {
		t.Fatalf("seq2 close disposition = %q", dropped[2])
	}
	// Short outage kills packets in flight on backup.
	for _, seq := range []int{3, 4, 5} {
		if dropped[seq] != link.ReasonLinkDown {
			t.Fatalf("seq%d link-down loss = %q", seq, dropped[seq])
		}
	}
	// A packet entering backup while it is broken is dropped at admission.
	if dropped[11] != link.ReasonLinkDown {
		t.Fatalf("seq11 admission during outage = %q", dropped[11])
	}
	// Shared narrow bottleneck rejects bytes beyond its 150B queue.
	if dropped[8] != link.ReasonQueueFull {
		t.Fatalf("shared bottleneck queue_full = %q", dropped[8])
	}
	if max := maxQueueOccupancy["narrow"]; max > 150 {
		t.Fatalf("narrow occupancy exceeded capacity: %d", max)
	}
	// Slow receiver caps its admission buffer and drops the overflow.
	if dropped[1] != link.ReasonReceiverFull || dropped[6] != link.ReasonReceiverFull {
		t.Fatalf("slow receiver drops missing: %v", dropped)
	}
	if maxReceiver > 150 {
		t.Fatalf("receiver occupancy exceeded buffer: %d", maxReceiver)
	}
	// Packets that entered primary before the reroute never get pulled back onto
	// backup: seq1 stays on primary, while later packets take backup.
	var seq1Backup bool
	for _, e := range result.Events {
		if e.Seq == 1 && e.Segment == "backup" {
			seq1Backup = true
		}
	}
	if seq1Backup {
		t.Fatal("reroute moved an in-flight packet back onto a different segment")
	}
}

func TestSavedDecisionsReplayEventByEvent(t *testing.T) {
	first := loadResult(t, "multipath.json", nil)
	if len(first.Decisions) == 0 {
		t.Fatal("expected recorded random decisions")
	}
	second := loadResult(t, "multipath.json", first.Decisions)
	if !second.Replayed {
		t.Fatal("result must flag replay mode")
	}
	if len(second.Events) != len(first.Events) {
		t.Fatal("event count mismatch")
	}
	for i := range first.Events {
		if first.Events[i] != second.Events[i] {
			t.Fatalf("event %d differs:\n%+v\n%+v", i, first.Events[i], second.Events[i])
		}
	}
}

func TestTamperedReplayIsRejected(t *testing.T) {
	first := loadResult(t, "multipath.json", nil)
	bad := first.Decisions
	bad[0].Lost = !bad[0].Lost
	s, err := scenario.Load(filepath.Join("..", "examples", "multipath.json"))
	if err != nil {
		t.Fatal(err)
	}
	s.Replay = bad
	if _, err := s.Run(); err == nil {
		t.Fatal("expected replay mismatch error")
	}
}
