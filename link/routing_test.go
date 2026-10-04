package link_test

import (
	"reflect"
	"testing"
	"time"

	"network-drill/link"
)

// drillSpec is the shared artificial topology: directed asymmetric
// bandwidths, a main path a->r1->b, a direct link, and a backup r1->b link.
func drillSpec() link.TopologySpec {
	dir := func(id string, ends [2]link.Node, latency time.Duration, a2b, b2a uint64) link.LinkSpec {
		return link.LinkSpec{
			ID: id, Ends: ends, Latency: latency, QueueBytes: 1 << 20,
			BandwidthAtoB: a2b, BandwidthBtoA: b2a,
		}
	}
	return link.TopologySpec{
		Links: []link.LinkSpec{
			dir("l1", [2]link.Node{"a", "r1"}, 100*time.Millisecond, 1000, 10000),
			dir("l2", [2]link.Node{"r1", "b"}, 100*time.Millisecond, 2000, 2000),
			dir("l3", [2]link.Node{"a", "b"}, 500*time.Millisecond, 500, 5000),
			dir("l4", [2]link.Node{"r1", "b"}, 50*time.Millisecond, 1000, 1000),
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
}

// dirLink builds one asymmetric-bandwidth link for decision-snapshot tests.
func dirLink(id string, ends [2]link.Node, latency time.Duration, a2b, b2a uint64) link.LinkSpec {
	return link.LinkSpec{
		ID: id, Ends: ends, Latency: latency, QueueBytes: 1 << 20,
		BandwidthAtoB: a2b, BandwidthBtoA: b2a,
	}
}

// TestRouteDecisionKeepsDecisionTimeBandwidth freezes the bandwidth and up
// state of every candidate link at decision time. The preferred route gets
// slower while an old fragment is still travelling: that fragment keeps its
// original serialization schedule, a freshly sent packet re-decides against
// the new facts, and a later down/up cycle neither redelivers data nor
// rewrites the records already made.
func TestRouteDecisionKeepsDecisionTimeBandwidth(t *testing.T) {
	spec := link.TopologySpec{
		Links: []link.LinkSpec{
			dirLink("l1", [2]link.Node{"a", "r1"}, 100*time.Millisecond, 4000, 4000),
			dirLink("l2", [2]link.Node{"r1", "b"}, 100*time.Millisecond, 4000, 4000),
			dirLink("l4", [2]link.Node{"r1", "b"}, 100*time.Millisecond, 1000, 1000),
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
	n, err := link.NewNetwork(spec, link.WithNetworkSeed(11))
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()

	// seq 1 chooses l2 at r1 while l2 is the fast link: 4 bytes on
	// 4000 B/s serialize in 1ms, and by 102ms it is in flight on l2.
	if _, err := n.Send(link.A, 1, []byte("four")); err != nil {
		t.Fatal(err)
	}
	n.Advance(102 * time.Millisecond) // seq 1 is already in flight on l2
	if flying := n.InFlight(); len(flying) != 1 || flying[0].Link != "l2" || flying[0].Stage != "in_flight" {
		t.Fatalf("seq 1 should be in flight on l2 before the slowdown: %+v", flying)
	}

	// l2 gets slower (4000 -> 500 B/s) while seq 1 is still on the wire.
	slow := link.LinkRules{
		Latency: 100 * time.Millisecond, QueueBytes: 1 << 20,
		BandwidthAtoB: 500, BandwidthBtoA: 500,
	}
	if err := n.UpdateLink("l2", slow); err != nil {
		t.Fatal(err)
	}

	// The old fragment keeps its original plan: 1ms serialization plus
	// 100ms latency from l1's 101ms arrival, landing at 202ms.
	oldDone := n.Advance(202 * time.Millisecond)
	if len(oldDone) != 1 || oldDone[0].Packet.Seq != 1 || oldDone[0].Time != 202*time.Millisecond {
		t.Fatalf("old fragment must finish on its original fast schedule: %+v", oldDone)
	}

	// A new packet re-decides with the new facts: l2 is up but slow, and the
	// recorded snapshot must show its current 500 B/s bandwidth.
	if _, err := n.Send(link.A, 2, []byte("five")); err != nil {
		t.Fatal(err)
	}
	n.Advance(303 * time.Millisecond) // seq 2 reaches r1 and chooses l2

	candidate := func(seq int, id string) (link.CandidateLink, bool) {
		for _, rd := range n.RouteDecisions() {
			if rd.Seq != seq || rd.At != "r1" {
				continue
			}
			for _, c := range rd.Candidates {
				if c.Link == id {
					return c, true
				}
			}
		}
		return link.CandidateLink{}, false
	}
	old, ok := candidate(1, "l2")
	if !ok || !old.Up || old.BandwidthBytesPerSecond != 4000 {
		t.Fatalf("seq 1 decision lost its decision-time l2 facts: %+v ok=%v", old, ok)
	}
	new, ok := candidate(2, "l2")
	if !ok || !new.Up || new.BandwidthBytesPerSecond != 500 {
		t.Fatalf("seq 2 decision must see the slowed l2: %+v ok=%v", new, ok)
	}
	if backup, ok := candidate(1, "l4"); !ok || !backup.Up || backup.BandwidthBytesPerSecond != 1000 {
		t.Fatalf("l4 candidate facts missing from the old decision: %+v ok=%v", backup, ok)
	}

	// Snapshot the records, then cycle l2 down and up after everything
	// delivered: recovery must not redeliver a packet or rewrite history.
	before := n.RouteDecisions()
	n.Advance(2 * time.Second)
	if err := n.SetLinkDown("l2"); err != nil {
		t.Fatal(err)
	}
	if err := n.SetLinkUp("l2"); err != nil {
		t.Fatal(err)
	}
	afterCycle := n.Advance(3 * time.Second)
	if len(afterCycle) != 0 {
		t.Fatalf("link recovery redelivered packets: %+v", afterCycle)
	}
	after := n.RouteDecisions()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("recovery rewrote route decisions:\nbefore=%+v\nafter =%+v", before, after)
	}

	// Exported records are detached snapshots: mutating a returned copy and
	// changing live bandwidth again cannot alter what old decisions report.
	tampered := n.RouteDecisions()
	tampered[0].Candidates[0].BandwidthBytesPerSecond = 999999
	again := n.UpdateLink("l2", link.LinkRules{
		Latency: 100 * time.Millisecond, QueueBytes: 1 << 20,
		BandwidthAtoB: 8000, BandwidthBtoA: 8000,
	})
	if again != nil {
		t.Fatal(again)
	}
	stable, ok := candidate(1, "l2")
	if !ok || stable.BandwidthBytesPerSecond != 4000 {
		t.Fatalf("old decision read later bandwidth instead of its own: %+v", stable)
	}
}

type drillOutcome struct {
	events    []link.Event
	decisions []link.Decision
	routes    []link.RouteDecision
	delivered []link.Delivery
}

// runDrill drives the same artificial traffic: asymmetric directions, a link
// outage with recovery, and a bandwidth change. step controls the advance
// granularity so big-step and fine-step runs can be compared.
func runDrill(t *testing.T, step time.Duration, opts ...link.NetworkOption) drillOutcome {
	t.Helper()
	n, err := link.NewNetwork(drillSpec(), opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	send := func(seq int, from link.Endpoint, data string) {
		t.Helper()
		if _, err := n.Send(from, seq, []byte(data)); err != nil {
			t.Fatal(err)
		}
	}
	advance := func(to time.Duration) []link.Delivery {
		var out []link.Delivery
		for now := n.Clock() + step; now <= to; now += step {
			out = append(out, n.Advance(now)...)
		}
		if n.Clock() < to {
			out = append(out, n.Advance(to)...)
		}
		return out
	}
	var delivered []link.Delivery
	send(1, link.A, "alpha")
	send(2, link.B, "bravo")
	delivered = append(delivered, advance(10*time.Millisecond)...)
	send(3, link.A, "charlie")
	delivered = append(delivered, advance(50*time.Millisecond)...)
	if err := n.SetLinkDown("l2"); err != nil {
		t.Fatal(err)
	}
	delivered = append(delivered, advance(60*time.Millisecond)...)
	send(4, link.A, "delta")
	delivered = append(delivered, advance(400*time.Millisecond)...)
	if err := n.SetLinkUp("l2"); err != nil {
		t.Fatal(err)
	}
	fast := link.LinkRules{
		Latency: 100 * time.Millisecond, QueueBytes: 1 << 20,
		BandwidthAtoB: 4000, BandwidthBtoA: 2000,
	}
	if err := n.UpdateLink("l2", fast); err != nil {
		t.Fatal(err)
	}
	delivered = append(delivered, advance(450*time.Millisecond)...)
	send(5, link.A, "echo")
	delivered = append(delivered, advance(3*time.Second)...)
	return drillOutcome{
		events: n.Events(), decisions: n.Decisions(),
		routes: n.RouteDecisions(), delivered: delivered,
	}
}

func TestRerouteExplainsAffectedFragments(t *testing.T) {
	out := runDrill(t, 3*time.Second, link.WithNetworkSeed(7))

	// Every fragment is delivered exactly once: nothing in flight was moved
	// or replayed after the recovery.
	seen := map[int]int{}
	for _, d := range out.delivered {
		seen[d.Packet.Seq]++
	}
	for seq := 1; seq <= 5; seq++ {
		if seen[seq] != 1 {
			t.Fatalf("seq %d delivered %d times: %+v", seq, seen[seq], out.delivered)
		}
	}

	// seq 1, 3, 4 reached r1 while l2 was down: each has a route decision
	// recording the known-down fact and the l4 detour.
	rerouted := map[int]link.RouteDecision{}
	for _, rd := range out.routes {
		if rd.At == "r1" {
			rerouted[rd.Seq] = rd
		}
	}
	for _, seq := range []int{1, 3, 4} {
		rd, ok := rerouted[seq]
		if !ok {
			t.Fatalf("seq %d missing r1 route decision in %+v", seq, out.routes)
		}
		if rd.Link != "l4" || len(rd.Down) != 1 || rd.Down[0] != "l2" {
			t.Fatalf("seq %d reroute lost its facts: %+v", seq, rd)
		}
	}
	// seq 5 arrived after recovery and uses the restored l2.
	if rd := rerouted[5]; rd.Link != "l2" || len(rd.Down) != 0 {
		t.Fatalf("seq 5 should use recovered l2: %+v", rd)
	}

	// The bandwidth change only touches seq 5: 4 bytes at 4000 B/s serialize
	// in 1ms after the 554ms r1 arrival, plus 100ms latency.
	for _, d := range out.delivered {
		if d.Packet.Seq == 5 && d.Time != 655*time.Millisecond {
			t.Fatalf("seq 5 ignored the bandwidth update: %v", d.Time)
		}
	}
	if len(out.events) == 0 || len(out.routes) == 0 {
		t.Fatal("expected recorded events and route decisions")
	}
}

func TestStepSizeAndReopenKeepIdenticalRecords(t *testing.T) {
	big := runDrill(t, 3*time.Second, link.WithNetworkSeed(7))
	fine := runDrill(t, time.Millisecond, link.WithNetworkSeed(7))
	assertSameOutcome(t, "fine-step", big, fine)

	// Save and reopen: replaying the recorded decisions into a fresh network
	// with a different seed must reproduce every record.
	reopened := runDrill(t, 3*time.Second,
		link.WithNetworkSeed(999), link.WithRecordedDecisions(big.decisions))
	assertSameOutcome(t, "reopened", big, reopened)
}

func assertSameOutcome(t *testing.T, what string, want, got drillOutcome) {
	t.Helper()
	if len(want.events) != len(got.events) {
		t.Fatalf("%s changed event count: %d vs %d", what, len(want.events), len(got.events))
	}
	for i := range want.events {
		if want.events[i] != got.events[i] {
			t.Fatalf("%s changed event %d: %+v vs %+v", what, i, want.events[i], got.events[i])
		}
	}
	if len(want.routes) != len(got.routes) {
		t.Fatalf("%s changed route records: %d vs %d", what, len(want.routes), len(got.routes))
	}
	for i := range want.routes {
		if !reflect.DeepEqual(want.routes[i], got.routes[i]) {
			t.Fatalf("%s changed route %d: %+v vs %+v", what, i, want.routes[i], got.routes[i])
		}
	}
	if len(want.delivered) != len(got.delivered) {
		t.Fatalf("%s changed deliveries: %+v vs %+v", what, want.delivered, got.delivered)
	}
	for i := range want.delivered {
		if want.delivered[i].Time != got.delivered[i].Time ||
			want.delivered[i].Packet.Seq != got.delivered[i].Packet.Seq {
			t.Fatalf("%s changed delivery %d: %+v vs %+v", what, i, want.delivered[i], got.delivered[i])
		}
	}
}

func TestForkComparesTwoRoutingFutures(t *testing.T) {
	spec := drillSpec()
	n, err := link.NewNetwork(spec, link.WithNetworkSeed(3))
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	for i, data := range []string{"one", "two"} {
		if _, err := n.Send(link.A, i+1, []byte(data)); err != nil {
			t.Fatal(err)
		}
	}
	// Pause with seq 1 serializing on l1 and seq 2 queued behind it.
	n.Advance(2 * time.Millisecond)
	paused := n.InFlight()
	if len(paused) != 2 {
		t.Fatalf("expected two fragments at the pause point: %+v", paused)
	}

	stay := n.Fork()
	sw := n.Fork()
	defer stay.Close()
	defer sw.Close()
	if got := sw.InFlight(); len(got) != len(paused) {
		t.Fatalf("fork changed the in-flight range: %+v vs %+v", paused, got)
	}
	// Two independent routing decisions from the same pause point.
	direct := []link.HopSpec{{Link: "l3", From: "a", To: "b"}}
	if err := sw.UpdatePath(link.DirectionAtoB, direct); err != nil {
		t.Fatal(err)
	}
	if _, err := stay.Send(link.A, 3, []byte("three")); err != nil {
		t.Fatal(err)
	}
	if _, err := sw.Send(link.A, 3, []byte("three")); err != nil {
		t.Fatal(err)
	}
	stayDone := stay.Advance(5 * time.Second)
	swDone := sw.Advance(5 * time.Second)

	seqs := func(ds []link.Delivery) map[int]int {
		out := map[int]int{}
		for _, d := range ds {
			out[d.Packet.Seq]++
		}
		return out
	}
	staySeqs, swSeqs := seqs(stayDone), seqs(swDone)
	for seq := 1; seq <= 3; seq++ {
		if staySeqs[seq] != 1 || swSeqs[seq] != 1 {
			t.Fatalf("duplicate or missing fragment seq %d: stay=%v sw=%v", seq, staySeqs, swSeqs)
		}
	}
	// The futures diverge only for traffic that had not entered a link:
	// seq 3 takes l3 in the switched fork and l1 in the staying fork.
	linkOf := func(n *link.Network, seq int) string {
		for _, rd := range n.RouteDecisions() {
			if rd.Seq == seq && rd.At == "a" {
				return rd.Link
			}
		}
		return ""
	}
	if got := linkOf(stay, 3); got != "l1" {
		t.Fatalf("staying fork rerouted seq 3: %q", got)
	}
	if got := linkOf(sw, 3); got != "l3" {
		t.Fatalf("switched fork kept seq 3 on the old path: %q", got)
	}
	// seq 1 was already on l1 in both futures and must not be moved.
	if got := linkOf(sw, 1); got != "l1" {
		t.Fatalf("switched fork moved in-flight seq 1: %q", got)
	}
}

func TestLoopAndUnreachablePathsStopClearly(t *testing.T) {
	spec := link.TopologySpec{
		Links: []link.LinkSpec{
			{ID: "l1", Ends: [2]link.Node{"a", "r1"}, Latency: 100 * time.Millisecond, QueueBytes: 1 << 20, BandwidthBytesPerSecond: 1000},
			{ID: "l2", Ends: [2]link.Node{"r1", "r2"}, Latency: 100 * time.Millisecond, QueueBytes: 1 << 20, BandwidthBytesPerSecond: 1000},
			{ID: "l3", Ends: [2]link.Node{"a", "r2"}, Latency: 100 * time.Millisecond, QueueBytes: 1 << 20, BandwidthBytesPerSecond: 1000},
			{ID: "l4", Ends: [2]link.Node{"r2", "b"}, Latency: 100 * time.Millisecond, QueueBytes: 1 << 20, BandwidthBytesPerSecond: 1000},
			{ID: "l5", Ends: [2]link.Node{"r1", "b"}, Latency: 100 * time.Millisecond, QueueBytes: 1 << 20, BandwidthBytesPerSecond: 1000},
		},
		Paths: map[link.Direction][]link.HopSpec{
			link.DirectionAtoB: {
				{Link: "l1", From: "a", To: "r1"},
				{Link: "l2", From: "r1", To: "r2"},
				{Link: "l4", From: "r2", To: "b"},
			},
			link.DirectionBtoA: {
				{Link: "l4", From: "b", To: "r2"},
				{Link: "l3", From: "r2", To: "a"},
			},
		},
		Receivers: map[link.Endpoint]link.ReceiverSpec{
			link.A: {QueueBytes: 1 << 20},
			link.B: {QueueBytes: 1 << 20},
		},
	}
	n, err := link.NewNetwork(spec, link.WithNetworkSeed(1))
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()

	loop := []link.HopSpec{
		{Link: "l1", From: "a", To: "r1"},
		{Link: "l1", From: "r1", To: "a"},
	}
	if err := n.UpdatePath(link.DirectionAtoB, loop); err != link.ErrRoutingLoop {
		t.Fatalf("loop not rejected: %v", err)
	}
	dead := []link.HopSpec{{Link: "l1", From: "a", To: "r1"}}
	if err := n.UpdatePath(link.DirectionAtoB, dead); err != link.ErrNoRoute {
		t.Fatalf("unreachable path not rejected: %v", err)
	}

	// A loop-free path can still conflict with a packet's travelled history:
	// seq 1 visited r1, and the new suffix would send it back there.
	if _, err := n.Send(link.A, 1, []byte("loop")); err != nil {
		t.Fatal(err)
	}
	n.Advance(150 * time.Millisecond) // seq 1 is flying on l2 toward r2
	conflicting := []link.HopSpec{
		{Link: "l3", From: "a", To: "r2"},
		{Link: "l2", From: "r2", To: "r1"},
		{Link: "l5", From: "r1", To: "b"},
	}
	if err := n.UpdatePath(link.DirectionAtoB, conflicting); err != nil {
		t.Fatal(err)
	}
	n.Advance(5 * time.Second) // must terminate, not spin on events
	foundsLoop := false
	for _, e := range n.Events() {
		if e.Seq == 1 && e.Status == link.StatusDropped && e.Reason == link.ReasonRoutingLoop {
			foundsLoop = true
		}
	}
	if !foundsLoop {
		t.Fatalf("seq 1 missing routing_loop stop in %+v", n.Events())
	}

	// With every link down, a new send stops with no_route immediately.
	for _, id := range []string{"l1", "l2", "l3", "l4", "l5"} {
		if err := n.SetLinkDown(id); err != nil {
			t.Fatal(err)
		}
	}
	res, err := n.Send(link.A, 2, []byte("lost"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != link.StatusDropped || res.Reason != link.ReasonNoRoute {
		t.Fatalf("expected no_route stop, got %+v", res)
	}
	var last link.RouteDecision
	for _, rd := range n.RouteDecisions() {
		if rd.Seq == 2 {
			last = rd
		}
	}
	if last.Reason != link.ReasonNoRoute || len(last.Down) != 5 {
		t.Fatalf("no_route decision lost its facts: %+v", last)
	}
}
