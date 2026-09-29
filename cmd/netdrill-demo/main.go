// netdrill-demo replays a fixed script over the virtual link twice with the
// same seed and prints the traffic record, demonstrating that arrival order,
// virtual timestamps and loss reasons are reproducible. It never touches the
// host network.
package main

import (
	"fmt"
	"os"
	"time"

	"netdrill"
)

func main() {
	steps := []netdrill.Step{
		{Offset: 0, From: "A", Payload: []byte("packet-one----100-bytes-padding-padding")},
		{Offset: 20 * time.Millisecond, From: "A", Payload: []byte("two")},
		{Offset: 30 * time.Millisecond, From: "B", Payload: []byte("ack")},
		{Offset: 40 * time.Millisecond, From: "A", Payload: []byte("three")},
	}

	run := func(seed int64) []netdrill.Event {
		clk := netdrill.NewSimClock()
		cfg := netdrill.Config{
			Latency:      80 * time.Millisecond,
			BandwidthBps: 400, // 50 bytes/s: every byte costs simulated time
			QueueBytes:   120,
			Loss:         0.25,
		}
		sim, err := netdrill.NewSim(cfg, seed, clk)
		if err != nil {
			panic(err)
		}
		events := netdrill.Replay(sim, clk, steps)
		_ = sim.Close()
		return events
	}

	first := run(20260929)
	second := run(20260929)

	same := true
	for i := range first {
		if i >= len(second) || fmt.Sprint(first[i]) != fmt.Sprint(second[i]) {
			same = false
		}
	}
	fmt.Printf("seed 20260929 replayed twice, identical records: %v\n\n", same)
	if err := netdrill.WriteEvents(os.Stdout, first); err != nil {
		panic(err)
	}
}
