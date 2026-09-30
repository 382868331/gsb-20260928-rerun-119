package link_test

import (
	"testing"
	"time"

	"network-drill/link"
)

func multiTopo(loss float64) link.Topology {
	return link.Topology{
		Segments: map[string]link.SegmentCfg{
			"s1": {Latency: 10 * time.Millisecond, BandwidthBPS: 1000, LossRate: loss, QueueBytes: 100000},
			"s2": {Latency: 10 * time.Millisecond, BandwidthBPS: 1000, LossRate: 0, QueueBytes: 100000},
		},
		Routes: map[link.Endpoint][]string{
			link.A: {"s1", "s2"},
			link.B: {"s2", "s1"},
		},
	}
}

func TestMultiHopFlowAndDirectionOverrides(t *testing.T) {
	topo := multiTopo(0)
	topo.Segments["s1"] = link.SegmentCfg{
		Latency: 10 * time.Millisecond, BandwidthBPS: 100,
		LossRate: 0, QueueBytes: 100000,
		BandwidthBPSReverse: 10000,
	}
	net, err := link.NewNetwork(topo, 123)
	if err != nil {
		t.Fatal(err)
	}
	defer net.Close()
	if _, err := net.Send(link.A, 1, make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	del := net.Advance(2 * time.Second)
	// 100ms serialization on slow s1, then fast s2, plus propagation.
	if len(del) != 1 || del[0].Packet.Seq != 1 {
		t.Fatalf("unexpected delivery: %+v", del)
	}
	if got := del[0].Time; got < 1120*time.Millisecond || got > 1300*time.Millisecond {
		t.Fatalf("multi-hop timing off: %v", got)
	}
}

func TestRandomLossDecisionsSaveAndReplayIdentically(t *testing.T) {
	run := func(replay []link.Decision) (*link.Network, []link.Event, []link.Decision, error) {
		var opt link.NetworkOption
		if replay != nil {
			opt = link.WithReplay(replay)
		}
		var net *link.Network
		var err error
		if opt != nil {
			net, err = link.NewNetwork(multiTopo(0.5), 99, opt)
		} else {
			net, err = link.NewNetwork(multiTopo(0.5), 99)
		}
		if err != nil {
			return nil, nil, nil, err
		}
		for seq := 1; seq <= 30; seq++ {
			if _, err := net.Send(link.A, seq, make([]byte, 40)); err != nil {
				t.Fatal(err)
			}
		}
		net.Advance(time.Hour)
		ev := net.Events()
		dm := net.Decisions()
		return net, ev, dm, nil
	}

	_, first, decisions, err := run(nil)
	if err != nil {
		t.Fatal(err)
	}
	hasLoss := false
	for _, e := range first {
		if e.Reason == link.ReasonRandomLoss {
			hasLoss = true
		}
	}
	if !hasLoss {
		t.Fatal("loss rate 0.5 should drop at least one of 30 packets")
	}
	if len(decisions) < 30 {
		t.Fatalf("expected at least one decision per packet, got %d", len(decisions))
	}
	_, second, _, err := run(decisions)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != len(second) {
		t.Fatalf("event count mismatch %d vs %d", len(first), len(second))
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("event %d differs: %+v vs %+v", i, first[i], second[i])
		}
	}
}

func TestBreakRecoverAndRerouteBoundaries(t *testing.T) {
	net, err := link.NewNetwork(multiTopo(0), 5)
	if err != nil {
		t.Fatal(err)
	}
	defer net.Close()
	net.Send(link.A, 1, make([]byte, 100))
	net.SetClock(5 * time.Millisecond)
	if err := net.Break("s1"); err != nil {
		t.Fatal(err)
	}
	net.Send(link.A, 2, make([]byte, 100))
	if err := net.Recover("s1"); err != nil {
		t.Fatal(err)
	}
	net.Send(link.A, 3, make([]byte, 100))
	net.Advance(time.Hour)
	reasons := map[int]string{}
	for _, e := range net.Events() {
		if e.Status == link.StatusDropped {
			reasons[e.Seq] = e.Reason
		}
	}
	if reasons[1] != link.ReasonLinkDown || reasons[2] != link.ReasonLinkDown {
		t.Fatalf("outage losses wrong: %v", reasons)
	}
	if _, ok := reasons[3]; ok {
		t.Fatalf("post-recovery packet must deliver: %v", reasons)
	}
}

func TestCloseGivesEveryUnfinishedPacketDisposition(t *testing.T) {
	topo := multiTopo(0)
	topo.Receivers = map[link.Endpoint]link.ReceiverCfg{
		link.B: {BufferBytes: 100000, LinkArrivalBytesPerSecond: 1},
	}
	net, err := link.NewNetwork(topo, 1)
	if err != nil {
		t.Fatal(err)
	}
	net.Send(link.A, 1, make([]byte, 10))
	net.Send(link.A, 2, make([]byte, 10))
	net.Advance(50 * time.Millisecond)
	wait := net.Wait(link.B)
	if err := net.Close(); err != nil {
		t.Fatal(err)
	}
	d := <-wait
	if d.Err != link.ErrClosed {
		t.Fatalf("wait should end closed: %+v", d)
	}
	closed := 0
	for _, e := range net.Events() {
		if e.Reason == link.ReasonClosedWhileQueue {
			closed++
		}
	}
	if closed != 2 {
		t.Fatalf("expected 2 explicit close dispositions, got %d", closed)
	}
}
