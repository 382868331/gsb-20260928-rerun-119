package link_test

import (
	"encoding/json"
	"testing"
	"time"

	"network-drill/link"
)

type drillAction struct {
	at    time.Duration
	apply func(n *link.Network) error
}

// drillScript sends five fragments, breaks l2 mid-flight, recovers it, and
// changes both a route and a bandwidth, so every mechanism is exercised.
func drillScript() []drillAction {
	return []drillAction{
		{0, func(n *link.Network) error { _, err := n.Send(link.A, 1, []byte("frag-1")); return err }},
		{100 * time.Millisecond, func(n *link.Network) error { _, err := n.Send(link.A, 2, []byte("frag-2")); return err }},
		{1500 * time.Millisecond, func(n *link.Network) error { return n.SetLinkDown("l2") }},
		{1600 * time.Millisecond, func(n *link.Network) error { return n.UpdateRoute("a", "b", []string{"l3"}) }},
		{1700 * time.Millisecond, func(n *link.Network) error { _, err := n.Send(link.A, 3, []byte("frag-3")); return err }},
		{2 * time.Second, func(n *link.Network) error { return n.SetLinkUp("l2") }},
		{2100 * time.Millisecond, func(n *link.Network) error {
			return n.UpdateLink("l4", link.LinkRules{
				Latency: time.Second, QueueBytes: 1 << 20, BandwidthBytesPerSecond: 100,
			})
		}},
		{2200 * time.Millisecond, func(n *link.Network) error { _, err := n.Send(link.A, 4, []byte("frag-4")); return err }},
		{2300 * time.Millisecond, func(n *link.Network) error { _, err := n.Send(link.B, 5, []byte("frag-5")); return err }},
	}
}

func runDrill(t *testing.T, step time.Duration, until time.Duration) (*link.Network, []link.Event, []link.RouteDecision) {
	t.Helper()
	n, err := link.NewNetwork(diamondSpec(), link.WithNetworkSeed(11))
	if err != nil {
		t.Fatal(err)
	}
	for _, action := range drillScript() {
		if step <= 0 {
			n.Advance(action.at)
		} else {
			for now := n.Clock() + step; now <= action.at; now += step {
				n.Advance(now)
			}
			n.Advance(action.at)
		}
		if err := action.apply(n); err != nil {
			t.Fatal(err)
		}
	}
	if step <= 0 {
		n.Advance(until)
	} else {
		for now := n.Clock() + step; now <= until; now += step {
			n.Advance(now)
		}
		n.Advance(until)
	}
	return n, n.Events(), n.RouteDecisions()
}

func TestCoarseAndFineStepsAgree(t *testing.T) {
	nBig, bigEvents, bigDecisions := runDrill(t, 0, 20*time.Second)
	defer nBig.Close()
	nFine, fineEvents, fineDecisions := runDrill(t, 10*time.Millisecond, 20*time.Second)
	defer nFine.Close()
	if len(bigEvents) != len(fineEvents) {
		t.Fatalf("step size changed event count: %d vs %d", len(bigEvents), len(fineEvents))
	}
	for i := range bigEvents {
		if bigEvents[i] != fineEvents[i] {
			t.Fatalf("event %d differs by step size: %+v vs %+v", i, bigEvents[i], fineEvents[i])
		}
	}
	if len(bigDecisions) != len(fineDecisions) {
		t.Fatalf("step size changed routing decisions: %d vs %d", len(bigDecisions), len(fineDecisions))
	}
	for i := range bigDecisions {
		a, _ := json.Marshal(bigDecisions[i])
		b, _ := json.Marshal(fineDecisions[i])
		if string(a) != string(b) {
			t.Fatalf("decision %d differs by step size: %s vs %s", i, a, b)
		}
	}
}

func TestSnapshotSaveReopenReproducesRun(t *testing.T) {
	full, fullEvents, fullDecisions := runDrill(t, 0, 20*time.Second)
	defer full.Close()

	paused, err := link.NewNetwork(diamondSpec(), link.WithNetworkSeed(11))
	if err != nil {
		t.Fatal(err)
	}
	actions := drillScript()
	pauseAt := 1600 * time.Millisecond
	index := 0
	for i, action := range actions {
		if action.at >= pauseAt {
			index = i
			break
		}
		paused.Advance(action.at)
		if err := action.apply(paused); err != nil {
			t.Fatal(err)
		}
	}
	paused.Advance(pauseAt)
	raw, err := json.Marshal(paused.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	paused.Close()

	var snap link.Snapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		t.Fatal(err)
	}
	reopened, err := link.RestoreNetwork(snap)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for _, action := range actions[index:] {
		reopened.Advance(action.at)
		if err := action.apply(reopened); err != nil {
			t.Fatal(err)
		}
	}
	reopened.Advance(20 * time.Second)

	reopenedEvents := reopened.Events()
	if len(reopenedEvents) != len(fullEvents) {
		t.Fatalf("reopened run diverged: %d vs %d events", len(reopenedEvents), len(fullEvents))
	}
	for i := range fullEvents {
		if reopenedEvents[i] != fullEvents[i] {
			t.Fatalf("event %d diverged after reopen: %+v vs %+v", i, reopenedEvents[i], fullEvents[i])
		}
	}
	reopenedDecisions := reopened.RouteDecisions()
	if len(reopenedDecisions) != len(fullDecisions) {
		t.Fatalf("reopened run changed routing decisions: %d vs %d", len(reopenedDecisions), len(fullDecisions))
	}
	for i := range fullDecisions {
		a, _ := json.Marshal(fullDecisions[i])
		b, _ := json.Marshal(reopenedDecisions[i])
		if string(a) != string(b) {
			t.Fatalf("decision %d diverged after reopen: %s vs %s", i, a, b)
		}
	}
}

func TestForkComparesTwoRoutingDecisions(t *testing.T) {
	base, err := link.NewNetwork(diamondSpec(), link.WithNetworkSeed(11))
	if err != nil {
		t.Fatal(err)
	}
	actions := []drillAction{
		{0, func(n *link.Network) error { _, err := n.Send(link.A, 1, []byte("frag-1")); return err }},
		{100 * time.Millisecond, func(n *link.Network) error { _, err := n.Send(link.A, 2, []byte("frag-2")); return err }},
		{1500 * time.Millisecond, func(n *link.Network) error { return n.SetLinkDown("l2") }},
		{1700 * time.Millisecond, func(n *link.Network) error { _, err := n.Send(link.A, 3, []byte("frag-3")); return err }},
		{2 * time.Second, func(n *link.Network) error { return n.SetLinkUp("l2") }},
	}
	pauseAt := 1200 * time.Millisecond
	for _, action := range actions {
		if action.at >= pauseAt {
			break
		}
		base.Advance(action.at)
		if err := action.apply(base); err != nil {
			t.Fatal(err)
		}
	}
	base.Advance(pauseAt)
	snap := base.Snapshot()
	base.Close()

	fork := func(mutate func(n *link.Network)) *link.Network {
		n, err := link.RestoreNetwork(snap)
		if err != nil {
			t.Fatal(err)
		}
		if mutate != nil {
			mutate(n)
		}
		for _, action := range actions {
			if action.at < pauseAt {
				continue
			}
			n.Advance(action.at)
			if err := action.apply(n); err != nil {
				t.Fatal(err)
			}
		}
		n.Advance(20 * time.Second)
		return n
	}

	stay := fork(nil)
	defer stay.Close()
	rerouted := fork(func(n *link.Network) {
		if err := n.UpdateRoute("a", "b", []string{"l3"}); err != nil {
			t.Fatal(err)
		}
	})
	defer rerouted.Close()

	staySummary := link.Summarize(stay.Events(), link.B)
	reroutedSummary := link.Summarize(rerouted.Events(), link.B)
	diff := link.CompareSummaries(staySummary, reroutedSummary)

	// frag-1 and frag-2 were in flight on l2 when it broke: both branches
	// lose exactly them; the reroute cannot move already-transmitted data.
	for name, summary := range map[string]link.FlowSummary{"stay": staySummary, "rerouted": reroutedSummary} {
		got := summary.Dropped[link.ReasonLinkDown]
		if len(got) != 2 || got[0] != 1 || got[1] != 2 {
			t.Fatalf("%s branch must drop frag-1 and frag-2 on the broken link: %+v", name, summary)
		}
	}
	if len(staySummary.Duplicates) != 0 || len(reroutedSummary.Duplicates) != 0 {
		t.Fatalf("no branch may duplicate fragments: %+v vs %+v", staySummary, reroutedSummary)
	}
	// frag-3 had not entered any link at the pause: the reroute moves it.
	if links := linksOfSeq(stay.Events(), 3); len(links) != 2 || links[0] != "l1" || links[1] != "l2" {
		t.Fatalf("stay branch must keep frag-3 on l1/l2: %v", links)
	}
	if links := linksOfSeq(rerouted.Events(), 3); len(links) != 2 || links[0] != "l3" || links[1] != "l4" {
		t.Fatalf("rerouted branch must move frag-3 to l3/l4: %v", links)
	}
	// Both branches deliver frag-3 and nothing is left in flight.
	if len(diff.DeliveredOnlyA) != 0 || len(diff.DeliveredOnlyB) != 0 {
		t.Fatalf("both branches deliver the same fragments: %+v", diff)
	}
	if len(diff.InFlightOnlyA) != 0 || len(diff.InFlightOnlyB) != 0 {
		t.Fatalf("no fragment may be left in flight: %+v", diff)
	}
	if len(diff.DroppedOnlyA) != 0 || len(diff.DroppedOnlyB) != 0 {
		t.Fatalf("both branches lose the same in-flight fragments: %+v", diff)
	}
}
