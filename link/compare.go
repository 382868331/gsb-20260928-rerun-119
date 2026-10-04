package link

import "sort"

// FlowSummary condenses one run's event log for one destination endpoint:
// what was delivered, what repeated, what stopped with which reason, and
// what was still in transit when the log ended.
type FlowSummary struct {
	Delivered  []int            `json:"delivered"`
	Duplicates []int            `json:"duplicates,omitempty"`
	Dropped    map[string][]int `json:"dropped,omitempty"`
	InFlight   []int            `json:"in_flight,omitempty"`
}

// Summarize builds the summary for packets addressed to dst.
func Summarize(events []Event, dst Endpoint) FlowSummary {
	s := FlowSummary{Dropped: map[string][]int{}}
	terminal := map[int]bool{}
	seen := map[int]bool{}
	inFlight := map[int]bool{}
	for _, e := range events {
		if e.To != dst {
			continue
		}
		switch e.Status {
		case StatusDelivered:
			if seen[e.Seq] {
				s.Duplicates = append(s.Duplicates, e.Seq)
			} else {
				seen[e.Seq] = true
				s.Delivered = append(s.Delivered, e.Seq)
			}
			terminal[e.Seq] = true
		case StatusDropped, StatusClosed:
			s.Dropped[e.Reason] = append(s.Dropped[e.Reason], e.Seq)
			terminal[e.Seq] = true
		case StatusQueued, StatusSerializing, StatusHopArrived:
			inFlight[e.Seq] = true
		}
	}
	for seq := range inFlight {
		if !terminal[seq] {
			s.InFlight = append(s.InFlight, seq)
		}
	}
	sort.Ints(s.Delivered)
	sort.Ints(s.Duplicates)
	sort.Ints(s.InFlight)
	for reason := range s.Dropped {
		sort.Ints(s.Dropped[reason])
	}
	return s
}

// SummaryDiff reports how two runs from the same pause point diverged.
type SummaryDiff struct {
	DeliveredOnlyA []int            `json:"delivered_only_a,omitempty"`
	DeliveredOnlyB []int            `json:"delivered_only_b,omitempty"`
	InFlightOnlyA  []int            `json:"in_flight_only_a,omitempty"`
	InFlightOnlyB  []int            `json:"in_flight_only_b,omitempty"`
	DuplicatesA    []int            `json:"duplicates_a,omitempty"`
	DuplicatesB    []int            `json:"duplicates_b,omitempty"`
	DroppedOnlyA   map[string][]int `json:"dropped_only_a,omitempty"`
	DroppedOnlyB   map[string][]int `json:"dropped_only_b,omitempty"`
}

// CompareSummaries diffs two summaries, usually one per branch of a fork.
func CompareSummaries(a, b FlowSummary) SummaryDiff {
	return SummaryDiff{
		DeliveredOnlyA: diffInts(a.Delivered, b.Delivered),
		DeliveredOnlyB: diffInts(b.Delivered, a.Delivered),
		InFlightOnlyA:  diffInts(a.InFlight, b.InFlight),
		InFlightOnlyB:  diffInts(b.InFlight, a.InFlight),
		DuplicatesA:    a.Duplicates,
		DuplicatesB:    b.Duplicates,
		DroppedOnlyA:   diffDropped(a.Dropped, b.Dropped),
		DroppedOnlyB:   diffDropped(b.Dropped, a.Dropped),
	}
}

func diffInts(x, y []int) []int {
	inY := map[int]bool{}
	for _, v := range y {
		inY[v] = true
	}
	var out []int
	for _, v := range x {
		if !inY[v] {
			out = append(out, v)
		}
	}
	return out
}

func diffDropped(x, y map[string][]int) map[string][]int {
	var out map[string][]int
	for reason, seqs := range x {
		other := map[int]bool{}
		for _, v := range y[reason] {
			other[v] = true
		}
		for _, v := range seqs {
			if !other[v] {
				if out == nil {
					out = map[string][]int{}
				}
				out[reason] = append(out[reason], v)
			}
		}
	}
	return out
}
