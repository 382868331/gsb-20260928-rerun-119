package link_test

import (
	"testing"
	"time"

	"network-drill/link"
)

func twoHopSpec() link.TopologySpec {
	return link.TopologySpec{
		Links: []link.LinkSpec{
			{ID: "l1", Ends: [2]link.Node{"a", "r1"}, Latency: time.Second, QueueBytes: 1 << 20, BandwidthBytesPerSecond: 1 << 20},
			{ID: "l2", Ends: [2]link.Node{"r1", "b"}, Latency: time.Second, QueueBytes: 1 << 20, BandwidthBytesPerSecond: 1 << 20},
		},
		Paths: map[link.Direction][]link.HopSpec{
			link.DirectionAtoB: {
				{Link: "l1", From: "a", To: "r1"},
				{Link: "l2", From: "r1", To: "b"},
			},
			link.DirectionBtoA: {
				{Link: "l2", From: "b", To: "r1"},
				{Link: "l1", From: "r1", To: "a"},
			},
		},
		Receivers: map[link.Endpoint]link.ReceiverSpec{
			link.A: {QueueBytes: 1 << 20},
			link.B: {QueueBytes: 1 << 20},
		},
	}
}

func runTwoHop(t *testing.T, step time.Duration) ([]link.Event, []link.Delivery) {
	t.Helper()
	n, err := link.NewNetwork(twoHopSpec(), link.WithNetworkSeed(7))
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	if _, err := n.Send(link.A, 1, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	var delivered []link.Delivery
	for now := step; now <= 5*time.Second; now += step {
		delivered = append(delivered, n.Advance(now)...)
	}
	return n.Events(), delivered
}

func TestMultiHopChainsFromRealEventTimes(t *testing.T) {
	bigEvents, bigDelivered := runTwoHop(t, 5*time.Second)
	if len(bigDelivered) != 1 {
		t.Fatalf("expected one delivery, got %+v", bigDelivered)
	}
	if got := bigDelivered[0].Time; got >= 3*time.Second {
		t.Fatalf("second hop started from final clock: delivered at %v", got)
	}
	if string(bigDelivered[0].Packet.Data) != "hello" {
		t.Fatalf("payload corrupted: %q", bigDelivered[0].Packet.Data)
	}

	fineEvents, fineDelivered := runTwoHop(t, 100*time.Millisecond)
	if len(fineDelivered) != 1 || fineDelivered[0].Time != bigDelivered[0].Time {
		t.Fatalf("step size changed delivery: big=%+v fine=%+v", bigDelivered, fineDelivered)
	}
	if len(bigEvents) != len(fineEvents) {
		t.Fatalf("step size changed event log: %d vs %d", len(bigEvents), len(fineEvents))
	}
	for i := range bigEvents {
		if bigEvents[i] != fineEvents[i] {
			t.Fatalf("event %d differs by step size: %+v vs %+v", i, bigEvents[i], fineEvents[i])
		}
	}
}

func asymmetricSpec() link.TopologySpec {
	return link.TopologySpec{
		Links: []link.LinkSpec{
			{ID: "l1", Ends: [2]link.Node{"a", "b"}, Latency: time.Second, QueueBytes: 1 << 20,
				BandwidthAtoB: 100, BandwidthBtoA: 10000},
		},
		Paths: map[link.Direction][]link.HopSpec{
			link.DirectionAtoB: {{Link: "l1", From: "a", To: "b"}},
			link.DirectionBtoA: {{Link: "l1", From: "b", To: "a"}},
		},
		Receivers: map[link.Endpoint]link.ReceiverSpec{
			link.A: {QueueBytes: 1 << 20},
			link.B: {QueueBytes: 1 << 20},
		},
	}
}

func TestUpdateLinkKeepsInFlightAndRespectsDirection(t *testing.T) {
	n, err := link.NewNetwork(asymmetricSpec(), link.WithNetworkSeed(3))
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()

	// 100 bytes at 100 B/s: serializes for 1s, then 1s latency -> arrival 2s.
	if _, err := n.Send(link.A, 1, make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	n.Advance(500 * time.Millisecond)
	fast := link.LinkRules{
		Latency:                 time.Millisecond,
		QueueBytes:              1 << 20,
		BandwidthBytesPerSecond: 1 << 20,
	}
	if err := n.UpdateLink("l1", fast); err != nil {
		t.Fatal(err)
	}
	if got := n.Advance(1500 * time.Millisecond); len(got) != 0 {
		t.Fatalf("in-flight packet adopted new rules: %+v", got)
	}
	got := n.Advance(2 * time.Second)
	if len(got) != 1 || got[0].Time != 2*time.Second {
		t.Fatalf("in-flight packet lost old schedule: %+v", got)
	}

	// New traffic uses the updated rules in both directions.
	if _, err := n.Send(link.A, 2, make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Send(link.B, 3, make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	got = n.Advance(3 * time.Second)
	if len(got) != 2 {
		t.Fatalf("expected two fast deliveries, got %+v", got)
	}
	for _, d := range got {
		if d.Time > 2*time.Second+5*time.Millisecond {
			t.Fatalf("post-update packet ignored new rules: %+v", d)
		}
	}

	// Direction asymmetry: B->A serializer is 100x faster than A->B.
	n2, err := link.NewNetwork(asymmetricSpec(), link.WithNetworkSeed(3))
	if err != nil {
		t.Fatal(err)
	}
	defer n2.Close()
	if _, err := n2.Send(link.B, 1, make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	got = n2.Advance(1100 * time.Millisecond)
	if len(got) != 1 || got[0].Time != 1010*time.Millisecond {
		t.Fatalf("b_to_a bandwidth not applied: %+v", got)
	}
}

func lossySpec() link.TopologySpec {
	spec := twoHopSpec()
	spec.Links[0].LossRate = 0.5
	return spec
}

func driveLossy(t *testing.T, opts ...link.NetworkOption) ([]link.Event, []link.Decision, []link.Delivery) {
	t.Helper()
	n, err := link.NewNetwork(lossySpec(), opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	for seq := 1; seq <= 8; seq++ {
		n.Advance(time.Duration(seq-1) * 300 * time.Millisecond)
		if _, err := n.Send(link.A, seq, []byte{byte(seq)}); err != nil {
			t.Fatal(err)
		}
	}
	delivered := n.Advance(10 * time.Second)
	return n.Events(), n.Decisions(), delivered
}

func TestSaveAndRestoreReplaysIdentically(t *testing.T) {
	events, decisions, delivered := driveLossy(t, link.WithNetworkSeed(11))
	if len(decisions) == 0 {
		t.Fatal("expected recorded loss decisions")
	}

	// Restore: replay the recorded draws into a fresh network with another seed.
	replayEvents, _, replayDelivered := driveLossy(t,
		link.WithNetworkSeed(999), link.WithRecordedDecisions(decisions))
	if len(events) != len(replayEvents) {
		t.Fatalf("restore changed event log: %d vs %d", len(events), len(replayEvents))
	}
	for i := range events {
		if events[i] != replayEvents[i] {
			t.Fatalf("event %d differs after restore: %+v vs %+v", i, events[i], replayEvents[i])
		}
	}
	if len(delivered) != len(replayDelivered) {
		t.Fatalf("restore changed deliveries: %+v vs %+v", delivered, replayDelivered)
	}
	for i := range delivered {
		if delivered[i].Time != replayDelivered[i].Time ||
			delivered[i].Packet.Seq != replayDelivered[i].Packet.Seq {
			t.Fatalf("delivery %d differs after restore: %+v vs %+v", i, delivered[i], replayDelivered[i])
		}
	}
}

func TestCombinedFatesAreExplained(t *testing.T) {
	spec := link.TopologySpec{
		Links: []link.LinkSpec{
			{ID: "l1", Ends: [2]link.Node{"a", "r1"}, Latency: 500 * time.Millisecond, QueueBytes: 1 << 20, BandwidthBytesPerSecond: 1 << 20},
			{ID: "l2", Ends: [2]link.Node{"r1", "b"}, Latency: 500 * time.Millisecond, QueueBytes: 1 << 20, BandwidthBytesPerSecond: 1 << 20},
			{ID: "l3", Ends: [2]link.Node{"a", "b"}, Latency: 200 * time.Millisecond, QueueBytes: 1 << 20, BandwidthBytesPerSecond: 1 << 20},
		},
		Paths: map[link.Direction][]link.HopSpec{
			link.DirectionAtoB: {
				{Link: "l1", From: "a", To: "r1"},
				{Link: "l2", From: "r1", To: "b"},
			},
			link.DirectionBtoA: {{Link: "l3", From: "b", To: "a"}},
		},
		Receivers: map[link.Endpoint]link.ReceiverSpec{
			link.A: {QueueBytes: 1 << 20},
			link.B: {QueueBytes: 1 << 20},
		},
	}
	n, err := link.NewNetwork(spec, link.WithNetworkSeed(5))
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()

	send := func(seq int) {
		t.Helper()
		if _, err := n.Send(link.A, seq, []byte{byte(seq)}); err != nil {
			t.Fatal(err)
		}
	}

	send(1) // in flight on l1 when the path switches
	n.Advance(100 * time.Millisecond)
	if err := n.UpdatePath(link.DirectionAtoB, []link.HopSpec{{Link: "l3", From: "a", To: "b"}}); err != nil {
		t.Fatal(err)
	}
	send(2) // takes l3, then the link breaks under it
	n.Advance(150 * time.Millisecond)
	if err := n.SetLinkDown("l3"); err != nil {
		t.Fatal(err)
	}
	send(3) // rejected immediately: first hop is down
	if err := n.SetLinkUp("l3"); err != nil {
		t.Fatal(err)
	}
	if err := n.UpdateLink("l3", link.LinkRules{
		Latency: 200 * time.Millisecond, QueueBytes: 1 << 20,
		BandwidthBytesPerSecond: 1 << 20, LossRate: 1,
	}); err != nil {
		t.Fatal(err)
	}
	send(4) // admitted then lost by the updated loss rule
	n.Advance(5 * time.Second)

	want := map[int]string{
		1: link.ReasonLinkDown,   // old path suffix vanished at the switch node
		2: link.ReasonLinkDown,   // in flight when l3 broke
		3: link.ReasonLinkDown,   // first hop down at send time
		4: link.ReasonRandomLoss, // new loss rule applies to later traffic
	}
	events := n.Events()
	for seq, reason := range want {
		found := false
		for _, e := range events {
			if e.Seq == seq && e.Status == link.StatusDropped && e.Reason == reason {
				found = true
			}
		}
		if !found {
			t.Fatalf("seq %d missing explained fate %q in %+v", seq, reason, events)
		}
	}
	for _, status := range []string{link.StatusRerouted, link.StatusLinkDown, link.StatusLinkUp, link.StatusRuleUpdate} {
		found := false
		for _, e := range events {
			if e.Status == status {
				found = true
			}
		}
		if !found {
			t.Fatalf("missing %q record in %+v", status, events)
		}
	}
}
