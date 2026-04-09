package metrics

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// EventKind identifies the type of a metrics event.
type EventKind int

const (
	KindPlaylist EventKind = iota
	KindSegment
	KindPart
	KindError
)

// Event carries per-request telemetry from a virtual client.
type Event struct {
	Kind       EventKind
	ClientID   int
	Latency    time.Duration // total request duration
	TTFB       time.Duration // time to first byte (segments/parts only)
	Bytes      int64
	HTTPStatus int
	Err        error
	Timestamp  time.Time
}

// latencyBuckets defines log-scale boundaries in milliseconds (up to ~30 s).
var latencyBuckets = []float64{
	1, 2, 5, 10, 20, 50, 100, 200, 500,
	1000, 2000, 5000, 10000, 20000, 30000,
}

const numBuckets = 16 // 15 boundaries + overflow

// histogram is protected by its own mutex so it can be updated from any
// goroutine. Snapshot reads are infrequent (every render interval) so
// write-contention on the histogram is the only real cost here.
type histogram struct {
	mu     sync.Mutex
	counts [numBuckets]int64
	total  int64
}

func (h *histogram) record(ms float64) {
	h.mu.Lock()
	h.total++
	for i, b := range latencyBuckets {
		if ms <= b {
			h.counts[i]++
			h.mu.Unlock()
			return
		}
	}
	h.counts[numBuckets-1]++
	h.mu.Unlock()
}

// copyLocked returns a stable copy of the histogram contents.
func (h *histogram) copyLocked() (counts [numBuckets]int64, total int64) {
	h.mu.Lock()
	counts = h.counts
	total = h.total
	h.mu.Unlock()
	return
}

// percentileFrom computes an approximate percentile from a copied histogram.
func percentileFrom(counts [numBuckets]int64, total int64, p float64) float64 {
	if total == 0 {
		return 0
	}
	target := int64(math.Ceil(p / 100.0 * float64(total)))
	var cum int64
	for i, b := range latencyBuckets {
		cum += counts[i]
		if cum >= target {
			return b
		}
	}
	return latencyBuckets[len(latencyBuckets)-1]
}

// kindStats tracks lock-free counters for one request kind plus a
// mutex-protected histogram. Emit() from 10,000+ goroutines hits only
// atomic operations for the fast path; histogram mutex contention is
// brief (one bucket increment) and read only on render ticks.
type kindStats struct {
	count    atomic.Int64
	errCount atomic.Int64
	bytes    atomic.Int64
	hist     histogram
}

func (k *kindStats) record(e Event) {
	k.count.Add(1)
	k.bytes.Add(e.Bytes)
	if e.Err != nil || (e.HTTPStatus >= 400 && e.HTTPStatus != 0) {
		k.errCount.Add(1)
	}
	ms := float64(e.Latency) / float64(time.Millisecond)
	k.hist.record(ms)
}

// Collector aggregates metrics from all virtual client goroutines.
// Emit() is safe for any number of concurrent goroutines with no channel
// overhead, no drop risk, and no single serialisation bottleneck.
type Collector struct {
	pl      kindStats
	seg     kindStats
	part    kindStats
	started time.Time
	active  atomic.Int32

	// logCh carries events to the optional NDJSON writer goroutine only.
	// Counter accuracy is unaffected by log drops.
	logCh   chan Event
	logStop chan struct{}
	logDone chan struct{}

	// LogWriter, if non-nil, receives every event from a dedicated goroutine.
	// Set before calling Start().
	LogWriter func(e Event)
}

// NewCollector creates a Collector. Call Start() before emitting events.
func NewCollector() *Collector {
	return &Collector{
		started: time.Now(),
		logCh:   make(chan Event, 65536),
		logStop: make(chan struct{}),
		logDone: make(chan struct{}),
	}
}

// Start launches the log-writer goroutine (no-op if LogWriter is nil).
func (c *Collector) Start() {
	go c.logLoop()
}

// Stop waits for the log writer to flush and exit.
func (c *Collector) Stop() {
	close(c.logStop)
	<-c.logDone
}

// Emit records an event. Never blocks, never drops counter data.
func (c *Collector) Emit(e Event) {
	switch e.Kind {
	case KindPlaylist:
		c.pl.record(e)
	case KindSegment:
		c.seg.record(e)
	case KindPart:
		c.part.record(e)
	}
	if c.LogWriter != nil {
		select {
		case c.logCh <- e:
		default:
		}
	}
}

// IncActive increments the active client counter.
func (c *Collector) IncActive() { c.active.Add(1) }

// DecActive decrements the active client counter.
func (c *Collector) DecActive() { c.active.Add(-1) }

func (c *Collector) logLoop() {
	defer close(c.logDone)
	for {
		select {
		case e := <-c.logCh:
			if c.LogWriter != nil {
				c.LogWriter(e)
			}
		case <-c.logStop:
			for {
				select {
				case e := <-c.logCh:
					if c.LogWriter != nil {
						c.LogWriter(e)
					}
				default:
					return
				}
			}
		}
	}
}

// RequestStats holds computed statistics for one request kind.
type RequestStats struct {
	Count          int64
	ErrCount       int64
	ErrPct         float64
	Bytes          int64
	ThroughputMbps float64 // set by reporter as interval delta; not computed here
}

// Stats is a point-in-time snapshot of all aggregated metrics.
type Stats struct {
	ActiveClients  int
	ElapsedSeconds float64
	Playlist       RequestStats
	Segment        RequestStats
	Part           RequestStats
}

// Snapshot returns a consistent point-in-time copy of all stats.
func (c *Collector) Snapshot() Stats {
	return Stats{
		ActiveClients:  int(c.active.Load()),
		ElapsedSeconds: time.Since(c.started).Seconds(),
		Playlist:       computeStats(&c.pl),
		Segment:        computeStats(&c.seg),
		Part:           computeStats(&c.part),
	}
}

// CountAbove returns the number of requests of the given kind whose latency
// exceeded thresholdMS milliseconds.
func (c *Collector) CountAbove(kind EventKind, thresholdMS float64) int64 {
	var k *kindStats
	switch kind {
	case KindPlaylist:
		k = &c.pl
	case KindSegment:
		k = &c.seg
	case KindPart:
		k = &c.part
	default:
		return 0
	}
	counts, _ := k.hist.copyLocked()
	var count int64
	for i, b := range latencyBuckets {
		if b > thresholdMS {
			count += counts[i]
		}
	}
	count += counts[numBuckets-1]
	return count
}

func computeStats(k *kindStats) RequestStats {
	count := k.count.Load()
	errCount := k.errCount.Load()
	bytes := k.bytes.Load()
	rs := RequestStats{
		Count:    count,
		ErrCount: errCount,
		Bytes:    bytes,
	}
	if count > 0 {
		rs.ErrPct = float64(errCount) / float64(count) * 100
	}
	return rs
}
