// Command loadgen drives synthetic driver and rider load directly against
// the matching and offer pipeline over a live Redis, and reports measured
// throughput and latency. It talks to the internal packages rather than
// over HTTP, since what's being benchmarked is the dispatch engine itself.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/AKSHAT0604/geo-dispatch/internal/h3index"
	"github.com/AKSHAT0604/geo-dispatch/internal/matching"
	"github.com/AKSHAT0604/geo-dispatch/internal/offers"
	"github.com/AKSHAT0604/geo-dispatch/internal/store"
)

// Hyderabad bounding box - a real city's bounds so the demo looks credible.
const (
	minLat, maxLat = 17.20, 17.55
	minLng, maxLng = 78.30, 78.65
	walkStepDeg    = 0.003 // roughly a few hundred meters per driver ping
)

func main() {
	numDrivers := flag.Int("drivers", 1000, "number of simulated drivers")
	riderRate := flag.Float64("rider-rate", 20, "ride request arrival rate, requests/sec (Poisson)")
	duration := flag.Duration("duration", 30*time.Second, "how long to generate ride requests for")
	acceptProb := flag.Float64("accept-prob", 0.85, "probability a driver accepts an offer")
	acceptDelay := flag.Duration("accept-delay", 2*time.Second, "simulated driver response delay")
	pingInterval := flag.Duration("ping-interval", 3*time.Second, "how often each driver pings its location")
	redisAddr := flag.String("redis-addr", envOr("REDIS_ADDR", "127.0.0.1:6379"), "redis address")
	flag.Parse()

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		pingCancel()
		log.Fatalf("connect to redis at %s: %v", *redisAddr, err)
	}
	pingCancel()

	drivers := store.NewDriverStore(rdb, h3index.DefaultResolution)
	trips := store.NewTripStore(rdb)
	offerStore := store.NewOfferStore(rdb)
	hub := offers.NewResponseHub()
	dispatcher := offers.NewDispatcher(offerStore, trips, drivers, hub, nil, offers.DefaultConfig)
	estimator := matching.NewCongestionAwareEstimator(matching.NewInMemoryCongestionProvider(), h3index.DefaultResolution)

	// driverCtx and riderCtx are deliberately separate. Driver ping loops
	// run indefinitely and only need to stop once every rider is done;
	// tying them to the same cancellation as in-flight dispatches would
	// cut those dispatches off mid-resolution the moment request
	// generation stops, well before their natural offer-window-bound
	// timeout. riderCtx's timeout is only a safety net.
	driverCtx, driverCancel := context.WithCancel(context.Background())
	defer driverCancel()
	trailingBuffer := offers.DefaultConfig.OfferWindow*time.Duration(offers.DefaultConfig.MaxRounds) + 30*time.Second
	riderCtx, riderCancel := context.WithTimeout(context.Background(), *duration+trailingBuffer)
	defer riderCancel()

	// Prefixed with a per-run ID so driver/rider IDs - and therefore trip
	// idempotency keys, which loadgen derives from the rider ID - never
	// collide with a previous run against the same Redis instance. Without
	// this, rerunning against persistent state resurrects already-resolved
	// trips instead of creating new ones.
	runID := rand.Int64()
	log.Printf("loadgen: run=%d %d drivers, %.1f req/s for %s, redis=%s", runID, *numDrivers, *riderRate, *duration, *redisAddr)

	var driverWG sync.WaitGroup
	for i := 0; i < *numDrivers; i++ {
		id := fmt.Sprintf("loadgen-%d-driver-%d", runID, i)
		driverWG.Add(1)
		go func(id string) {
			defer driverWG.Done()
			runDriver(driverCtx, drivers, offerStore, hub, id, *pingInterval, *acceptProb, *acceptDelay)
		}(id)
	}

	var matched, unfulfilled, requestErrors int64
	var latMu sync.Mutex
	var latencies []time.Duration
	var riderWG sync.WaitGroup

	stop := time.After(*duration)
	riderSeq := 0
riderLoop:
	for {
		select {
		case <-stop:
			break riderLoop
		case <-riderCtx.Done():
			break riderLoop
		default:
		}

		interArrival := time.Duration(-math.Log(1-rand.Float64()) / *riderRate * float64(time.Second))
		select {
		case <-time.After(interArrival):
		case <-riderCtx.Done():
			break riderLoop
		}

		riderSeq++
		riderWG.Add(1)
		go func(seq int) {
			defer riderWG.Done()
			lat, lng := randPoint()
			start := time.Now()
			m, err := runRider(riderCtx, trips, drivers, dispatcher, estimator, fmt.Sprintf("loadgen-%d-rider-%d", runID, seq), lat, lng)
			if err != nil {
				atomic.AddInt64(&requestErrors, 1)
				log.Printf("rider %d: %v", seq, err)
				return
			}
			elapsed := time.Since(start)
			latMu.Lock()
			latencies = append(latencies, elapsed)
			latMu.Unlock()
			if m {
				atomic.AddInt64(&matched, 1)
			} else {
				atomic.AddInt64(&unfulfilled, 1)
			}
		}(riderSeq)
	}

	log.Printf("loadgen: request generation done, waiting for in-flight dispatches to resolve")
	riderWG.Wait()
	driverCancel()
	driverWG.Wait()

	report(*duration, latencies, matched, unfulfilled, requestErrors)
}

// runDriver pings a random-walking location on pingInterval and, whenever
// it's currently offered a trip (polled via CurrentOfferFor, since it only
// knows its own driver ID), waits acceptDelay and then accepts or declines
// according to acceptProb.
func runDriver(ctx context.Context, drivers *store.DriverStore, offerStore *store.OfferStore, hub *offers.ResponseHub, id string, pingInterval time.Duration, acceptProb float64, acceptDelay time.Duration) {
	lat, lng := randPoint()
	pingTicker := time.NewTicker(pingInterval)
	defer pingTicker.Stop()
	pollTicker := time.NewTicker(150 * time.Millisecond)
	defer pollTicker.Stop()

	var responding atomic.Bool

	for {
		select {
		case <-ctx.Done():
			return
		case <-pingTicker.C:
			lat, lng = randomWalk(lat, lng)
			_ = drivers.UpdateLocation(ctx, id, lat, lng)
		case <-pollTicker.C:
			if !responding.CompareAndSwap(false, true) {
				continue // already responding to an offer
			}
			tripID, err := offerStore.CurrentOfferFor(ctx, id)
			if err != nil {
				responding.Store(false)
				continue
			}
			go func(tripID string) {
				defer responding.Store(false)
				select {
				case <-time.After(acceptDelay):
				case <-ctx.Done():
					return
				}
				resp := offers.Declined
				if rand.Float64() < acceptProb {
					resp = offers.Accepted
				}
				hub.Respond(tripID, resp)
			}(tripID)
		}
	}
}

// runRider creates a trip and drives one full dispatch attempt for it,
// mirroring exactly what a real demand-service + disco request path does:
// idempotent trip creation, candidate search, ranking, then the offer loop.
func runRider(ctx context.Context, trips *store.TripStore, drivers *store.DriverStore, dispatcher *offers.Dispatcher, estimator matching.ETAEstimator, riderID string, lat, lng float64) (matched bool, err error) {
	trip, err := trips.CreateTrip(ctx, riderID, lat, lng, riderID)
	if err != nil {
		return false, fmt.Errorf("create trip: %w", err)
	}

	origin, err := h3index.CellFor(lat, lng, h3index.DefaultResolution)
	if err != nil {
		return false, fmt.Errorf("cell for origin: %w", err)
	}

	candidates, err := matching.FindCandidates(ctx, drivers, origin, matching.DefaultSearchConfig)
	if err != nil {
		return false, fmt.Errorf("find candidates: %w", err)
	}
	ranked, err := matching.Rank(estimator, matching.Location{Lat: lat, Lng: lng}, candidates, matching.DefaultRankConfig, time.Now())
	if err != nil {
		return false, fmt.Errorf("rank: %w", err)
	}

	matched, _, err = dispatcher.Run(ctx, trip.TripID, origin, ranked)
	return matched, err
}

func randPoint() (lat, lng float64) {
	lat = minLat + rand.Float64()*(maxLat-minLat)
	lng = minLng + rand.Float64()*(maxLng-minLng)
	return
}

func randomWalk(lat, lng float64) (float64, float64) {
	lat += (rand.Float64() - 0.5) * walkStepDeg
	lng += (rand.Float64() - 0.5) * walkStepDeg
	return clamp(lat, minLat, maxLat), clamp(lng, minLng, maxLng)
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func report(duration time.Duration, latencies []time.Duration, matched, unfulfilled, requestErrors int64) {
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	total := matched + unfulfilled

	fmt.Println()
	fmt.Println("=== loadgen results ===")
	fmt.Printf("request generation window: %s\n", duration)
	fmt.Printf("resolved requests:         %d\n", total)
	fmt.Printf("matched:                   %d (%.1f%%)\n", matched, pct(matched, total))
	fmt.Printf("unfulfilled:               %d (%.1f%%)\n", unfulfilled, pct(unfulfilled, total))
	fmt.Printf("request errors:            %d\n", requestErrors)
	fmt.Printf("throughput:                %.1f matches/sec\n", float64(matched)/duration.Seconds())
	if len(latencies) > 0 {
		fmt.Printf("p50 latency:               %s\n", percentile(latencies, 0.50))
		fmt.Printf("p95 latency:               %s\n", percentile(latencies, 0.95))
		fmt.Printf("p99 latency:               %s\n", percentile(latencies, 0.99))
	}
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func pct(n, total int64) float64 {
	if total == 0 {
		return 0
	}
	return float64(n) / float64(total) * 100
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
