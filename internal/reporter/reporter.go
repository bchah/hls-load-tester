package reporter

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/bchah/hls-load-tester/internal/metrics"
)

const dashboardLines = 13 // number of lines the dashboard occupies

// Reporter renders live stats to a terminal using ANSI cursor control.
type Reporter struct {
	collector      *metrics.Collector
	out            io.Writer
	url            string
	totalClients   int
	duration       time.Duration // 0 = infinite
	startedAt      time.Time
	interval       time.Duration
	slowThreshold  float64 // milliseconds
	prevStats      metrics.Stats
	prevTime       time.Time
	stopCh         chan struct{}
	doneCh         chan struct{}
	firstDraw      bool
}

// New creates a Reporter. Call Start() to begin rendering.
func New(
	collector *metrics.Collector,
	out io.Writer,
	url string,
	totalClients int,
	duration time.Duration,
	interval time.Duration,
	slowThresholdMS float64,
) *Reporter {
	return &Reporter{
		collector:     collector,
		out:           out,
		url:           url,
		totalClients:  totalClients,
		duration:      duration,
		startedAt:     time.Now(),
		interval:      interval,
		slowThreshold: slowThresholdMS,
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
		firstDraw:     true,
	}
}

// Start launches the background render loop.
func (r *Reporter) Start() {
	go r.loop()
}

// Stop halts the render loop and waits for it to finish.
func (r *Reporter) Stop() {
	close(r.stopCh)
	<-r.doneCh
}

// PrintSummary writes the final stats table to r.out.
func (r *Reporter) PrintSummary() {
	snap := r.collector.Snapshot()
	r.clearDashboard()

	// For the summary, use average throughput over the full test duration.
	segSnap := snap.Segment
	plSnap := snap.Playlist
	partSnap := snap.Part
	if snap.ElapsedSeconds > 0 {
		segSnap.ThroughputMbps = float64(snap.Segment.Bytes) * 8 / 1e6 / snap.ElapsedSeconds
		plSnap.ThroughputMbps = float64(snap.Playlist.Bytes) * 8 / 1e6 / snap.ElapsedSeconds
		partSnap.ThroughputMbps = float64(snap.Part.Bytes) * 8 / 1e6 / snap.ElapsedSeconds
	}

	fmt.Fprintln(r.out, strings.Repeat("─", 72))
	fmt.Fprintln(r.out, "  FINAL SUMMARY")
	fmt.Fprintln(r.out, strings.Repeat("─", 72))
	fmt.Fprintf(r.out, "  URL:      %s\n", r.url)
	fmt.Fprintf(r.out, "  Duration: %.1fs   Clients: %d\n", snap.ElapsedSeconds, r.totalClients)
	fmt.Fprintln(r.out)
	r.printKindSummary(r.out, "SEGMENTS ", metrics.KindSegment, segSnap)
	r.printKindSummary(r.out, "PLAYLISTS", metrics.KindPlaylist, plSnap)
	r.printKindSummary(r.out, "PARTS    ", metrics.KindPart, partSnap)
	fmt.Fprintln(r.out, strings.Repeat("─", 72))
}

func (r *Reporter) loop() {
	defer close(r.doneCh)
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.render()
		case <-r.stopCh:
			r.render()
			return
		}
	}
}

func (r *Reporter) render() {
	snap := r.collector.Snapshot()
	now := time.Now()

	// Compute current interval throughput as (bytes this interval) / (interval seconds).
	// This shows actual current transfer rate rather than a decaying historical average.
	var segMbps, plMbps, partMbps float64
	if !r.prevTime.IsZero() {
		dt := now.Sub(r.prevTime).Seconds()
		if dt > 0 {
			segMbps = float64(snap.Segment.Bytes-r.prevStats.Segment.Bytes) * 8 / 1e6 / dt
			plMbps = float64(snap.Playlist.Bytes-r.prevStats.Playlist.Bytes) * 8 / 1e6 / dt
			partMbps = float64(snap.Part.Bytes-r.prevStats.Part.Bytes) * 8 / 1e6 / dt
		}
	}
	r.prevStats = snap
	r.prevTime = now

	// Determine mode tag from whether we have any part activity.
	mode := "Live"
	if snap.Part.Count > 0 {
		mode = "LL-HLS"
	}

	var durationStr string
	if r.duration > 0 {
		durationStr = fmt.Sprintf("%.0fs/%.0fs", snap.ElapsedSeconds, r.duration.Seconds())
	} else {
		durationStr = fmt.Sprintf("%.0fs", snap.ElapsedSeconds)
	}

	if !r.firstDraw {
		r.clearDashboard()
	}
	r.firstDraw = false

	w := r.out

	line(w, "─", 72)
	fmt.Fprintf(w, "  HLS Load Tester   %s\n", truncate(r.url, 52))
	fmt.Fprintf(w, "  Duration: %-12s  Clients: %d/%d active   Mode: %s\n",
		durationStr, snap.ActiveClients, r.totalClients, mode)
	line(w, "─", 72)
	// Inject interval throughput into local copies of the stats before rendering.
	segSnap := snap.Segment
	segSnap.ThroughputMbps = segMbps
	plSnap := snap.Playlist
	plSnap.ThroughputMbps = plMbps
	partSnap := snap.Part
	partSnap.ThroughputMbps = partMbps
	r.printKindRow(w, "SEGMENTS ", metrics.KindSegment, segSnap)
	fmt.Fprintln(w)
	r.printKindRow(w, "PLAYLISTS", metrics.KindPlaylist, plSnap)
	fmt.Fprintln(w)
	r.printKindRow(w, "PARTS    ", metrics.KindPart, partSnap)
	line(w, "─", 72)
}

// clearDashboard moves the cursor up dashboardLines lines and clears them.
func (r *Reporter) clearDashboard() {
	fmt.Fprintf(r.out, "\033[%dA", dashboardLines)
	for i := 0; i < dashboardLines; i++ {
		fmt.Fprint(r.out, "\033[2K\n")
	}
	fmt.Fprintf(r.out, "\033[%dA", dashboardLines)
}

func line(w io.Writer, char string, width int) {
	fmt.Fprintln(w, strings.Repeat(char, width))
}

// printKindRow writes a two-line stats section for one request kind.
func (r *Reporter) printKindRow(w io.Writer, label string, kind metrics.EventKind, rs metrics.RequestStats) {
	totalMB := float64(rs.Bytes) / 1e6
	unit := "MB"
	totalVal := totalMB
	if totalMB >= 1000 {
		totalVal = totalMB / 1000
		unit = "GB"
	}

	fmt.Fprintf(w, "  %s  Fetched: %7d  Errors: %4d (%5.2f%%)  Total: %6.1f %s\n",
		label, rs.Count, rs.ErrCount, rs.ErrPct, totalVal, unit)
	
	// Compute slow requests for this kind.
	slowCount := r.collector.CountAbove(kind, r.slowThreshold)
	slowPct := 0.0
	if rs.Count > 0 {
		slowPct = float64(slowCount) / float64(rs.Count) * 100
	}
	
	fmt.Fprintf(w, "  %s  Slow Requests (>%dms): %6d (%5.2f%%)  Now: %6.1f Mbps\n",
		strings.Repeat(" ", len(label)),
		int(r.slowThreshold), slowCount, slowPct,
		rs.ThroughputMbps)
}

// printKindSummary is the same layout but for the final summary block.
func (r *Reporter) printKindSummary(w io.Writer, label string, kind metrics.EventKind, rs metrics.RequestStats) {
	totalMB := float64(rs.Bytes) / 1e6
	unit := "MB"
	totalVal := totalMB
	if totalMB >= 1000 {
		totalVal = totalMB / 1000
		unit = "GB"
	}
	fmt.Fprintf(w, "  %s  Fetched: %7d  Errors: %4d (%5.2f%%)  Total: %6.1f %s\n",
		label, rs.Count, rs.ErrCount, rs.ErrPct, totalVal, unit)

	slowCount := r.collector.CountAbove(kind, r.slowThreshold)
	slowPct := 0.0
	if rs.Count > 0 {
		slowPct = float64(slowCount) / float64(rs.Count) * 100
	}
	fmt.Fprintf(w, "  %s  Slow Requests (>%dms): %6d (%5.2f%%)  Avg: %6.1f Mbps\n",
		strings.Repeat(" ", len(label)),
		int(r.slowThreshold), slowCount, slowPct,
		rs.ThroughputMbps)
	fmt.Fprintln(w)
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return "…" + s[len(s)-max+1:]
}
