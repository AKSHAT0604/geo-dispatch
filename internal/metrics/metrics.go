// Package metrics defines the Prometheus collectors the dispatch pipeline
// reports against. Collectors are package-level so any caller can record
// against them without threading a registry through every function
// signature - the same pattern client_golang itself is designed around.
package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	// MatchLatency is the end-to-end time a single dispatch attempt takes,
	// from candidate search through the offer loop's return - whichever
	// way it resolves.
	MatchLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "dispatch_match_latency_seconds",
		Help:    "End-to-end latency of a single dispatch attempt (candidate search plus the offer loop).",
		Buckets: prometheus.DefBuckets,
	})

	// MatchesTotal counts trips that successfully matched a driver.
	MatchesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dispatch_matches_total",
		Help: "Total number of trips successfully matched to a driver.",
	})

	// UnfulfilledTotal counts trips that exhausted every candidate without
	// matching. Failing to match is a valid, observable outcome, not an
	// error - this metric is how "valid" stays distinguishable from
	// "silent" without digging through logs.
	UnfulfilledTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dispatch_unfulfilled_total",
		Help: "Total number of trips that exhausted every candidate without matching.",
	})

	// RingExpansion is how many k-ring steps candidate search needed
	// before finding enough AVAILABLE drivers (or giving up at MaxK).
	RingExpansion = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "dispatch_ring_expansion",
		Help:    "k-ring steps candidate search needed before finding enough AVAILABLE drivers.",
		Buckets: []float64{0, 1, 2, 3, 4},
	})

	// OffersPerMatch is how many offer rounds a trip went through before
	// resolving, whether matched or unfulfilled.
	OffersPerMatch = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "dispatch_offers_per_match",
		Help:    "Offer rounds a trip went through before matching or being marked unfulfilled.",
		Buckets: []float64{1, 2, 3, 4, 5},
	})
)

func init() {
	prometheus.MustRegister(MatchLatency, MatchesTotal, UnfulfilledTotal, RingExpansion, OffersPerMatch)
}
