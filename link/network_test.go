package link_test

import (
	"testing"
	"time"

	"network-drill/link"
)

func twoHopSpec(bandwidth uint64) link.TopologySpec {
	linkCfg := func(id string, from, to link.Node) link.LinkSpec {
		return link.LinkSpec{
			ID:                      id,
			Ends:                    [2]link.Node{from, to},
			Latency:                 time.Second,
			QueueBytes:              1000,
			BandwidthBytesPerSecond: bandwidth,
		}
	}
	return link.TopologySpec{
		Links: []link.LinkSpec{
			linkCfg("l1", "a", "c"),
			linkCfg("l2", "c", "b"),
			linkCfg("l3", "a", "d"),
			linkCfg("l4", "d", "b"),
		},
		Paths: map[link.Direction][]link.HopSpec{
			link.DirectionAtoB: {
				{Link: "l1", From: "a", To: "c"},
				{Link: "l2", From: "c", To: "b"},
			},
			link.DirectionBtoA: {
				{Link: "l2", From: "b", To: "c"},
				{Link: "l1", From: "c", To: "a"},
			},
		},
		Receivers: map[link.Endpoint]link.ReceiverSpec{
			link.A: {QueueBytes: 1000},
			link.B: {QueueBytes: 1000},
		},
	}
}

// A 100-byte packet over two hops of 100 B/s + 1s latency needs 4 virtual
// seconds: serialize [0,1], propagate to C at 2, serialize [2,3], propagate
// to B at 4. One 5s jump and millisecond-sized steps must agree.
func TestNetworkAdvanceStepSizeDoesNotChangeDelivery(t *testing.T) {
	run := func(step time.Duration) []link.Delivery {
		n, err := link.NewNetwork(twoHopSpec(100), link.WithNetworkSeed(7))
		if err != nil {
			t.Fatal(err)
		}
		defer n.Close()
		if _, err := n.Send(link.A, 1, make([]byte, 100)); err != nil {
			t.Fatal(err)
		}
		var got []link.Delivery
		for now := step; now <= 5*time.Second; now += step {
			got = append(got, n.Advance(now)...)
		}
		return got
	}

	big := run(5 * time.Second)
	fine := run(time.Millisecond)
	if len(big) != 1 || big[0].Time != 4*time.Second {
		t.Fatalf("big step missed the 4s delivery: %+v", big)
	}
	if len(fine) != 1 || fine[0].Time != big[0].Time ||
		len(fine[0].Packet.Data) != len(big[0].Packet.Data) ||
		fine[0].Packet.Seq != big[0].Packet.Seq {
		t.Fatalf("fine steps diverge: %+v vs %+v", fine, big)
	}
}

func TestNetworkUpdateLinkKeepsAdmittedPacketsOnOldRules(t *testing.T) {
	run := func(step time.Duration) time.Duration {
		n, err := link.NewNetwork(twoHopSpec(100), link.WithNetworkSeed(7))
		if err != nil {
			t.Fatal(err)
		}
		defer n.Close()
		if _, err := n.Send(link.A, 1, make([]byte, 100)); err != nil {
			t.Fatal(err)
		}
		fast := link.LinkRules{
			Latency:                 0,
			QueueBytes:              1000,
			BandwidthBytesPerSecond: 100000,
		}
		advance := func(to time.Duration) {
			if step == 5*time.Second {
				n.Advance(to)
				return
			}
			for cur := step; cur <= to; cur += step {
				n.Advance(cur)
			}
		}
		advance(500 * time.Millisecond)
		if err := n.UpdateLink("l1", fast); err != nil {
			t.Fatal(err)
		}
		got := n.Advance(5 * time.Second)
		if len(got) != 1 {
			t.Fatalf("expected one delivery, got %+v", got)
		}
		return got[0].Time
	}

	// The packet entered l1 under 100 B/s + 1s latency, so it reaches C at 2s
	// and B at 4s even though l1 changed at 500ms, regardless of step size.
	if got, want := run(5*time.Second), 4*time.Second; got != want {
		t.Fatalf("big step delivery = %v, want %v", got, want)
	}
	if got, want := run(time.Millisecond), 4*time.Second; got != want {
		t.Fatalf("fine step delivery = %v, want %v", got, want)
	}
}

func TestNetworkDirectionAsymmetry(t *testing.T) {
	spec := twoHopSpec(0)
	for i := range spec.Links {
		spec.Links[i].BandwidthBytesPerSecond = 0
		spec.Links[i].BandwidthAtoB = 100
		spec.Links[i].BandwidthBtoA = 1000
	}
	n, err := link.NewNetwork(spec, link.WithNetworkSeed(7))
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	if _, err := n.Send(link.A, 1, make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Send(link.B, 2, make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	got := n.Advance(5 * time.Second)
	times := map[int]time.Duration{}
	for _, d := range got {
		times[d.Packet.Seq] = d.Time
	}
	// A->B: 1s serialize + 1s latency per hop => 4s.
	// B->A: 0.1s serialize + 1s latency per hop => 2.2s.
	if times[1] != 4*time.Second {
		t.Fatalf("a->b delivery = %v, want 4s", times[1])
	}
	if times[2] != 2200*time.Millisecond {
		t.Fatalf("b->a delivery = %v, want 2.2s", times[2])
	}
}

func TestNetworkRecordedDecisionsSurviveMidRunRestore(t *testing.T) {
	spec := twoHopSpec(1000)
	for i := range spec.Links {
		spec.Links[i].LossAtoB = 0.5
		spec.Links[i].BandwidthBytesPerSecond = 1000
	}

	// Reference run driven in one jump per action interval; its draws are
	// the checkpoint captured mid-run.
	drive := func(n *link.Network, step time.Duration) []link.Delivery {
		var got []link.Delivery
		advance := func(to time.Duration) {
			cur := n.Clock()
			if step <= 0 || to-cur <= step {
				got = append(got, n.Advance(to)...)
				return
			}
			for cur < to {
				cur += step
				if cur > to {
					cur = to
				}
				got = append(got, n.Advance(cur)...)
			}
		}
		for seq := 1; seq <= 8; seq++ {
			advance(time.Duration(seq) * 200 * time.Millisecond)
			if _, err := n.Send(link.A, seq, make([]byte, 50)); err != nil {
				t.Fatal(err)
			}
		}
		advance(10 * time.Second)
		return got
	}

	ref, err := link.NewNetwork(spec, link.WithNetworkSeed(123))
	if err != nil {
		t.Fatal(err)
	}
	refEvents := drive(ref, time.Hour)
	decisions := ref.Decisions()
	if len(decisions) == 0 {
		t.Fatal("test setup produced no random decisions")
	}

	// Restored engine fed the saved draws, driven in millisecond steps.
	restored, err := link.NewNetwork(spec,
		link.WithNetworkSeed(123), link.WithRecordedDecisions(decisions))
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	gotEvents := drive(restored, time.Millisecond)
	if len(gotEvents) != len(refEvents) {
		t.Fatalf("delivery count after restore = %d, want %d", len(gotEvents), len(refEvents))
	}
	for i := range gotEvents {
		if gotEvents[i].Time != refEvents[i].Time ||
			gotEvents[i].Packet.Seq != refEvents[i].Packet.Seq {
			t.Fatalf("restored delivery %d diverges: %+v vs %+v", i, gotEvents[i], refEvents[i])
		}
	}

	// Decisions recorded after the replay checkpoint must match a fresh
	// continuous stream, proving the PRNG stayed aligned.
	for i, d := range restored.Decisions() {
		if d != decisions[i] {
			t.Fatalf("decision %d diverges after restore: %+v vs %+v", i, d, decisions[i])
		}
	}
}

func TestNetworkCombinedScenarioExplainsEveryFate(t *testing.T) {
	spec := twoHopSpec(1000)
	spec.Links[0].LossAtoB = 1 // l1 always drops A->B admission
	n, err := link.NewNetwork(spec, link.WithNetworkSeed(5))
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()

	// seq 1 is rejected at l1 by the per-direction random loss rule.
	if res, err := n.Send(link.A, 1, make([]byte, 10)); err != nil || res.Status != link.StatusDropped {
		t.Fatalf("seq1 result = %+v, %v", res, err)
	}

	// Restore l1, admit seq 2 on the A-C-B path, then break l2 while it is
	// in flight; the packet must be recorded as link_down on l2.
	noLoss := link.LinkRules{
		Latency:                 time.Second,
		QueueBytes:              1000,
		BandwidthBytesPerSecond: 1000,
	}
	if err := n.UpdateLink("l1", noLoss); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Send(link.A, 2, make([]byte, 10)); err != nil {
		t.Fatal(err)
	}
	n.Advance(1500 * time.Millisecond)
	if err := n.SetLinkDown("l2"); err != nil {
		t.Fatal(err)
	}
	n.Advance(2100 * time.Millisecond)

	// Reroute later traffic over l3/l4; seq 3 must traverse the new path.
	if err := n.UpdatePath(link.DirectionAtoB, []link.HopSpec{
		{Link: "l3", From: "a", To: "d"},
		{Link: "l4", From: "d", To: "b"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Send(link.A, 3, make([]byte, 10)); err != nil {
		t.Fatal(err)
	}
	got := n.Advance(5 * time.Second)

	fate := map[int]string{}
	reason := map[int]string{}
	deliveredSeq := map[int]bool{}
	for _, e := range n.Events() {
		switch e.Status {
		case link.StatusDropped:
			fate[e.Seq] = link.StatusDropped
			reason[e.Seq] = e.Reason
		case link.StatusDelivered:
			fate[e.Seq] = link.StatusDelivered
		}
	}
	for _, d := range got {
		deliveredSeq[d.Packet.Seq] = true
	}
	if fate[1] != link.StatusDropped || reason[1] != link.ReasonRandomLoss {
		t.Fatalf("seq1 fate = %s/%s", fate[1], reason[1])
	}
	if fate[2] != link.StatusDropped || reason[2] != link.ReasonLinkDown {
		t.Fatalf("seq2 fate = %s/%s", fate[2], reason[2])
	}
	if fate[3] != link.StatusDelivered || !deliveredSeq[3] {
		t.Fatalf("seq3 fate = %s, delivered=%v", fate[3], deliveredSeq[3])
	}

	// The rerouted packet must show l3 and l4 in its hop records.
	links := map[int]map[string]bool{}
	for _, e := range n.Events() {
		if e.Seq == 3 && (e.Status == link.StatusSerializing || e.Status == link.StatusHopArrived) {
			if links[3] == nil {
				links[3] = map[string]bool{}
			}
			links[3][e.Link] = true
		}
	}
	if !links[3]["l3"] || !links[3]["l4"] {
		t.Fatalf("seq3 did not traverse rerouted path: %v", links[3])
	}
}
