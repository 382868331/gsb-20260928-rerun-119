package link_test

import (
	"testing"
	"time"

	"network-drill/link"
)

// diamondSpec is a diamond topology: a reaches b via r1 (l1, l2) or r2 (l3, l4).
func diamondSpec() link.TopologySpec {
	fast := uint64(1 << 20)
	return link.TopologySpec{
		Links: []link.LinkSpec{
			{ID: "l1", Ends: [2]link.Node{"a", "r1"}, Latency: time.Second, QueueBytes: 1 << 20, BandwidthBytesPerSecond: fast},
			{ID: "l2", Ends: [2]link.Node{"r1", "b"}, Latency: time.Second, QueueBytes: 1 << 20, BandwidthBytesPerSecond: fast},
			{ID: "l3", Ends: [2]link.Node{"a", "r2"}, Latency: time.Second, QueueBytes: 1 << 20, BandwidthBytesPerSecond: fast},
			{ID: "l4", Ends: [2]link.Node{"r2", "b"}, Latency: time.Second, QueueBytes: 1 << 20, BandwidthBytesPerSecond: fast},
		},
		Routes: map[link.Node]map[link.Node][]string{
			"a":  {"b": {"l1", "l3"}},
			"r1": {"b": {"l2"}, "a": {"l1"}},
			"r2": {"b": {"l4"}, "a": {"l3"}},
			"b":  {"a": {"l2", "l4"}},
		},
		Receivers: map[link.Endpoint]link.ReceiverSpec{
			link.A: {QueueBytes: 1 << 20},
			link.B: {QueueBytes: 1 << 20},
		},
	}
}

func linksOfSeq(events []link.Event, seq int) []string {
	var out []string
	for _, e := range events {
		if e.Seq == seq && e.Status == link.StatusSerializing {
			out = append(out, e.Link)
		}
	}
	return out
}

func TestDynamicRoutingRecordsFactsAndSkipsDownLink(t *testing.T) {
	n, err := link.NewNetwork(diamondSpec(), link.WithNetworkSeed(9))
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	if err := n.SetLinkDown("l1"); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Send(link.A, 1, []byte("via-r2")); err != nil {
		t.Fatal(err)
	}
	got := n.Advance(5 * time.Second)
	if len(got) != 1 || string(got[0].Packet.Data) != "via-r2" {
		t.Fatalf("expected delivery through r2, got %+v", got)
	}
	events := n.Events()
	links := linksOfSeq(events, 1)
	if len(links) != 2 || links[0] != "l3" || links[1] != "l4" {
		t.Fatalf("packet used wrong links: %v", links)
	}
	decisions := n.RouteDecisions()
	if len(decisions) != 2 {
		t.Fatalf("expected two hop decisions, got %+v", decisions)
	}
	first := decisions[0]
	if first.At != "a" || first.Dest != "b" || first.Chosen != "l3" || first.Stop != "" {
		t.Fatalf("unexpected first decision: %+v", first)
	}
	if len(first.Facts) != 2 || first.Facts[0].Link != "l1" || first.Facts[0].Up ||
		first.Facts[1].Link != "l3" || !first.Facts[1].Up {
		t.Fatalf("decision lost the facts known at choice time: %+v", first.Facts)
	}
	if first.Facts[0].BandwidthBps == 0 {
		t.Fatalf("facts must include the bandwidth known at decision time: %+v", first.Facts)
	}
}

func TestMidHopBreakDoesNotMoveOrDuplicateInflight(t *testing.T) {
	spec := diamondSpec()
	for i := range spec.Links {
		if spec.Links[i].ID == "l2" {
			spec.Links[i].Latency = 10 * time.Second
		}
	}
	n, err := link.NewNetwork(spec, link.WithNetworkSeed(5))
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	if _, err := n.Send(link.A, 1, []byte("frag-1")); err != nil {
		t.Fatal(err)
	}
	// At 2s the packet is propagating on l2 (arrived at r1 at 1s).
	n.Advance(2 * time.Second)
	if err := n.SetLinkDown("l2"); err != nil {
		t.Fatal(err)
	}
	if err := n.SetLinkUp("l2"); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Send(link.A, 2, []byte("frag-2")); err != nil {
		t.Fatal(err)
	}
	n.Advance(30 * time.Second)

	summary := link.Summarize(n.Events(), link.B)
	if len(summary.Delivered) != 1 || summary.Delivered[0] != 2 {
		t.Fatalf("only frag-2 may be delivered: %+v", summary)
	}
	if len(summary.Duplicates) != 0 {
		t.Fatalf("recovery must not resurrect in-flight data: %+v", summary)
	}
	if got := summary.Dropped[link.ReasonLinkDown]; len(got) != 1 || got[0] != 1 {
		t.Fatalf("frag-1 must stop with link_down: %+v", summary)
	}
	// The in-flight fragment never appears on the alternate route.
	if links := linksOfSeq(n.Events(), 1); len(links) != 2 || links[0] != "l1" || links[1] != "l2" {
		t.Fatalf("in-flight fragment moved to another route: %v", links)
	}
}

func TestRoutingLoopStopsWithReason(t *testing.T) {
	spec := diamondSpec()
	// r1 points back to a: a -> r1 -> a would loop forever without detection.
	spec.Routes["r1"]["b"] = []string{"l1"}
	n, err := link.NewNetwork(spec, link.WithNetworkSeed(1))
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	if _, err := n.Send(link.A, 1, []byte("loop")); err != nil {
		t.Fatal(err)
	}
	n.Advance(time.Hour)
	summary := link.Summarize(n.Events(), link.B)
	if got := summary.Dropped[link.ReasonRoutingLoop]; len(got) != 1 || got[0] != 1 {
		t.Fatalf("loop must stop with routing_loop: %+v", summary)
	}
	if len(summary.Delivered) != 0 || len(summary.InFlight) != 0 {
		t.Fatalf("looped packet must terminate, not linger: %+v", summary)
	}
	decisions := n.RouteDecisions()
	last := decisions[len(decisions)-1]
	if last.Stop != link.ReasonRoutingLoop {
		t.Fatalf("final decision must record the loop stop: %+v", last)
	}
	events := n.Events()
	last2 := events[len(events)-1]
	if last2.Status != link.StatusDropped || last2.Reason != link.ReasonRoutingLoop {
		t.Fatalf("events must end at the loop stop, got %+v", last2)
	}
}

func TestNoRouteStopsWithReason(t *testing.T) {
	n, err := link.NewNetwork(diamondSpec(), link.WithNetworkSeed(1))
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	n.SetLinkDown("l1")
	n.SetLinkDown("l3")
	result, err := n.Send(link.A, 1, []byte("stuck"))
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != link.StatusDropped || result.Reason != link.ReasonNoRoute {
		t.Fatalf("expected no_route, got %+v", result)
	}
	n.Advance(time.Hour)
	decisions := n.RouteDecisions()
	if len(decisions) != 1 || decisions[0].Stop != link.ReasonNoRoute {
		t.Fatalf("expected one no_route decision, got %+v", decisions)
	}
	for _, f := range decisions[0].Facts {
		if f.Up {
			t.Fatalf("facts must show every candidate down: %+v", decisions[0].Facts)
		}
	}
	summary := link.Summarize(n.Events(), link.B)
	if len(summary.InFlight) != 0 {
		t.Fatalf("unreachable packet must not stay in flight: %+v", summary)
	}
}

func TestRerouteAffectsOnlyUnenteredFragments(t *testing.T) {
	spec := diamondSpec()
	for i := range spec.Links {
		if spec.Links[i].ID == "l1" {
			spec.Links[i].BandwidthBytesPerSecond = 0
			spec.Links[i].BandwidthAtoB = 100
			spec.Links[i].BandwidthBtoA = 10000
		}
	}
	n, err := link.NewNetwork(spec, link.WithNetworkSeed(2))
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	// seq 1 enters l1 at t=0 and serializes for 1s (100 bytes at 100 B/s).
	if _, err := n.Send(link.A, 1, make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	n.Advance(100 * time.Millisecond)
	// Reroute while seq 1 is serializing on l1: it keeps l1, later sends use l3.
	if err := n.UpdateRoute("a", "b", []string{"l3"}); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Send(link.A, 2, make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	n.Advance(10 * time.Second)
	events := n.Events()
	if links := linksOfSeq(events, 1); len(links) != 2 || links[0] != "l1" || links[1] != "l2" {
		t.Fatalf("already-entered fragment changed route: %v", links)
	}
	if links := linksOfSeq(events, 2); len(links) != 2 || links[0] != "l3" || links[1] != "l4" {
		t.Fatalf("unentered fragment ignored the reroute: %v", links)
	}
	summary := link.Summarize(events, link.B)
	if len(summary.Delivered) != 2 || len(summary.Duplicates) != 0 {
		t.Fatalf("both fragments must be delivered exactly once: %+v", summary)
	}
}

func TestAsymmetricDirectionAndBandwidthUpdate(t *testing.T) {
	spec := link.TopologySpec{
		Links: []link.LinkSpec{
			{ID: "l1", Ends: [2]link.Node{"a", "b"}, Latency: time.Second, QueueBytes: 1 << 20,
				BandwidthAtoB: 100, BandwidthBtoA: 10000},
		},
		Routes: map[link.Node]map[link.Node][]string{
			"a": {"b": {"l1"}},
			"b": {"a": {"l1"}},
		},
		Receivers: map[link.Endpoint]link.ReceiverSpec{
			link.A: {QueueBytes: 1 << 20},
			link.B: {QueueBytes: 1 << 20},
		},
	}
	n, err := link.NewNetwork(spec, link.WithNetworkSeed(4))
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
	gotA := n.Advance(1500 * time.Millisecond)
	if len(gotA) != 1 || gotA[0].Packet.Seq != 2 {
		t.Fatalf("fast B->A direction should deliver first: %+v", gotA)
	}
	gotB := n.Advance(3 * time.Second)
	if len(gotB) != 1 || gotB[0].Packet.Seq != 1 {
		t.Fatalf("slow A->B direction should arrive at 2s: %+v", gotB)
	}
	// Bandwidth change only touches traffic entering the link afterwards.
	if err := n.UpdateLink("l1", link.LinkRules{
		Latency: time.Second, QueueBytes: 1 << 20, BandwidthAtoB: 10000, BandwidthBtoA: 10000,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Send(link.A, 3, make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	got := n.Advance(5 * time.Second)
	if len(got) != 1 || got[0].Time != 3*time.Second+1010*time.Millisecond {
		t.Fatalf("post-update traffic must use the new bandwidth: %+v", got)
	}
}
