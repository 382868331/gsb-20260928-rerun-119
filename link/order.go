package link

import "sort"

func sortFlights(fs []*flight) {
	sort.SliceStable(fs, func(i, j int) bool {
		if fs[i].from != fs[j].from {
			return fs[i].from == A
		}
		if fs[i].to != fs[j].to {
			return fs[i].to == B
		}
		return fs[i].seq < fs[j].seq
	})
}

func sortStrings(s []string) { sort.Strings(s) }
