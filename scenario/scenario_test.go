package scenario_test

import (
	"path/filepath"
	"testing"
	"time"

	"network-drill/link"
	"network-drill/scenario"
)

func TestExampleReplayRecordsOrderTimeAndReason(t *testing.T) {
	s, err := scenario.Load(filepath.Join("..", "examples", "queue.json"))
	if err != nil {
		t.Fatal(err)
	}
	result, err := s.Run()
	if err != nil {
		t.Fatal(err)
	}
	if result.Seed != 42 || len(result.Events) != 4 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if result.Events[1].Status != link.StatusDropped || result.Events[1].Reason != link.ReasonQueueFull {
		t.Fatalf("missing queue-full reason: %+v", result.Events[1])
	}
	if result.Events[3].Status != link.StatusDelivered || result.Events[3].Time != 1010*time.Millisecond {
		t.Fatalf("unexpected delivery: %+v", result.Events[3])
	}
}

func TestRerouteExampleRecordsDecisionsAndStops(t *testing.T) {
	s, err := scenario.Load(filepath.Join("..", "examples", "reroute.json"))
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.Run()
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Events) != len(second.Events) || len(first.RouteDecisions) != len(second.RouteDecisions) {
		t.Fatalf("same scenario must replay identically")
	}
	for i := range first.Events {
		if first.Events[i] != second.Events[i] {
			t.Fatalf("event %d not deterministic: %+v vs %+v", i, first.Events[i], second.Events[i])
		}
	}
	if len(first.RouteDecisions) == 0 {
		t.Fatalf("dynamic routing must record decisions")
	}
	for _, d := range first.RouteDecisions {
		if len(d.Facts) == 0 {
			t.Fatalf("decision without recorded facts: %+v", d)
		}
	}
	summary := link.Summarize(first.Events, link.B)
	if got := summary.Dropped[link.ReasonLinkDown]; len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("frag-1 and frag-2 were on l2 when it broke: %+v", summary)
	}
	if len(summary.Duplicates) != 0 {
		t.Fatalf("recovered links must not duplicate fragments: %+v", summary)
	}
	if len(summary.Delivered) != 2 || summary.Delivered[0] != 3 || summary.Delivered[1] != 4 {
		t.Fatalf("only frag-3 and frag-4 reach b: %+v", summary)
	}
}
