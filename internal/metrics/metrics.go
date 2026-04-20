package metrics

import (
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

// kindStats tracks lock-free counters for one request kind.
type kindStats struct {
	count    atomic.Int64
	errCount atomic.Int64
	bytes    atomic.Int64
}

func (k *kindStats) record(e Event) {
	k.count.Add(1)
	k.bytes.Add(e.Bytes)
	if e.Err != nil || (e.HTTPStatus >= 400 && e.HTTPStatus != 0) {
		k.errCount.Add(1)
	}
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
