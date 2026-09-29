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
