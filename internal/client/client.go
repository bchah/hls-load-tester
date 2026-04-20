package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bchah/hls-load-tester/internal/m3u8"
	"github.com/bchah/hls-load-tester/internal/metrics"
)

// Client simulates a single HLS viewer session.
type Client struct {
	id        int
	cfg       *Config
	dl        *Downloader
	collector *metrics.Collector
}

const (
	defaultReloadSeconds = 3.0
	minReloadInterval    = 1 * time.Second
	maxRetryBackoff      = 4
)

// newClient constructs one virtual client.
func newClient(id int, cfg *Config, collector *metrics.Collector) *Client {
	return &Client{
		id:        id,
		cfg:       cfg,
		dl:        NewDownloader(cfg),
		collector: collector,
	}
}

// Run executes the client's playback simulation until ctx is cancelled.
func (c *Client) Run(ctx context.Context) {
	// Step 1 — fetch the top-level URL and decide master vs. media.
	pr, err := c.dl.FetchPlaylist(ctx, c.cfg.ManifestURL, nil)
	if err != nil {
		if shouldReportRequestErr(ctx, err) {
			c.emitErr(metrics.KindPlaylist, err)
		}
		return
	}
	c.emitPlaylist(pr)

	var mediaURL string

	if m3u8.IsMasterPlaylist(pr.Body) {
		master, err := m3u8.ParseMaster(c.cfg.ManifestURL, pr.Body)
		if err != nil {
			c.emitErr(metrics.KindPlaylist, fmt.Errorf("parse master: %w", err))
			return
		}
		v := selectVariant(master, c.cfg.Rendition)
		if v == nil {
			c.emitErr(metrics.KindPlaylist, fmt.Errorf("no suitable variant found"))
			return
		}
		mediaURL = v.URI
	} else {
		// Direct media playlist URL.
		mediaURL = c.cfg.ManifestURL
	}

	// Step 2 — fetch initial media playlist (tune-in, no blocking params).
	mpr, err := c.dl.FetchPlaylist(ctx, mediaURL, nil)
	if err != nil {
		if shouldReportRequestErr(ctx, err) {
			c.emitErr(metrics.KindPlaylist, err)
		}
		return
	}
	c.emitPlaylist(mpr)

	pl, err := m3u8.ParseMedia(mediaURL, mpr.Body, nil)
	if err != nil {
		c.emitErr(metrics.KindPlaylist, fmt.Errorf("parse media: %w", err))
		return
	}

	// Step 3 — dispatch to the appropriate playback loop.
	if pl.IsVOD {
		c.runVOD(ctx, pl)
		return
	}

	if pl.ServerControl.CanBlockReload {
		c.runLLHLS(ctx, mediaURL, pl)
	} else {
		c.runLive(ctx, mediaURL, pl)
	}
}

// Close releases resources tied to this virtual client.
func (c *Client) Close() {
	if c == nil || c.dl == nil {
		return
	}
	c.dl.Close()
}

// ──────────────────────────────────────────────────────────────────────────────
// VOD loop
// ──────────────────────────────────────────────────────────────────────────────

func (c *Client) runVOD(ctx context.Context, pl *m3u8.MediaPlaylist) {
	var lastMapURI string
	for _, seg := range pl.Segments {
		if ctx.Err() != nil {
			return
		}
		if seg.MapURI != "" && seg.MapURI != lastMapURI {
			// Fetch init section once per map change.
			c.fetchInit(ctx, seg.MapURI)
			lastMapURI = seg.MapURI
		}
		start := time.Now()
		c.fetchSegment(ctx, seg.URI, metrics.KindSegment)
		// Pace to real-time: sleep remainder of segment duration.
		elapsed := time.Since(start)
		pace := time.Duration(seg.Duration*float64(time.Second)) - elapsed
		if pace > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(pace):
			}
		}
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Normal live loop (no blocking reload)
// ──────────────────────────────────────────────────────────────────────────────

func (c *Client) runLive(ctx context.Context, mediaURL string, pl *m3u8.MediaPlaylist) {
	lastDownloadedMSN := -1
	var lastMapURI string
	prev := pl
	consecutiveFetchErr := 0
	consecutiveParseErr := 0

	if c.cfg.DownloadBacklog {
		// Optional stress mode: download the full live window at tune-in.
		for _, seg := range pl.Segments {
			if ctx.Err() != nil {
				return
			}
			if !seg.IsComplete || seg.URI == "" {
				continue
			}
			if seg.MapURI != "" && seg.MapURI != lastMapURI {
				c.fetchInit(ctx, seg.MapURI)
				lastMapURI = seg.MapURI
			}
			c.fetchSegment(ctx, seg.URI, metrics.KindSegment)
			lastDownloadedMSN = seg.MSN
		}
	} else {
		// Browser-like tune-in: start from the newest complete segment only.
		for i := len(pl.Segments) - 1; i >= 0; i-- {
			seg := pl.Segments[i]
			if !seg.IsComplete || seg.URI == "" {
				continue
			}
			if seg.MapURI != "" {
				c.fetchInit(ctx, seg.MapURI)
				lastMapURI = seg.MapURI
			}
			c.fetchSegment(ctx, seg.URI, metrics.KindSegment)
			lastDownloadedMSN = seg.MSN
			break
		}
	}

	for {
		if ctx.Err() != nil {
			return
		}

		// Compute reload wait: last segment duration on success, targetDuration/2 on retry.
		reloadWait := normalizeReloadDuration(pl.TargetDuration/2, prev.TargetDuration)
		if len(pl.Segments) > 0 {
			last := pl.Segments[len(pl.Segments)-1]
			if last.Duration > 0 {
				reloadWait = normalizeReloadDuration(last.Duration, pl.TargetDuration)
			}
		}
		if !sleepWithContext(ctx, c.withClientJitter(reloadWait)) {
			return
		}

		mpr, err := c.dl.FetchPlaylist(ctx, mediaURL, nil)
		if err != nil {
			consecutiveFetchErr++
			if shouldReportRequestErr(ctx, err) {
				c.emitErr(metrics.KindPlaylist, err)
			}
			retryWait := normalizeReloadDuration(prev.TargetDuration, pl.TargetDuration)
			retryWait = backoffDuration(retryWait, consecutiveFetchErr-1)
			if !sleepWithContext(ctx, c.withClientJitter(retryWait)) {
				return
			}
			continue
		}
		consecutiveFetchErr = 0
		c.emitPlaylist(mpr)

		newPL, err := m3u8.ParseMedia(mediaURL, mpr.Body, prev)
		if err != nil {
			consecutiveParseErr++
			retryWait := normalizeReloadDuration(prev.TargetDuration, pl.TargetDuration)
			retryWait = backoffDuration(retryWait, consecutiveParseErr-1)
			if !sleepWithContext(ctx, c.withClientJitter(retryWait)) {
				return
			}
			continue
		}
		consecutiveParseErr = 0
		prev = newPL
		pl = newPL

		// Download segments that are new since last download.
		for _, seg := range pl.Segments {
			if seg.MSN <= lastDownloadedMSN {
				continue
			}
			if !seg.IsComplete || seg.URI == "" {
				continue
			}
			if ctx.Err() != nil {
				return
			}
			if seg.MapURI != "" && seg.MapURI != lastMapURI {
				c.fetchInit(ctx, seg.MapURI)
				lastMapURI = seg.MapURI
			}
			c.fetchSegment(ctx, seg.URI, metrics.KindSegment)
			lastDownloadedMSN = seg.MSN
		}

		if pl.HasEndList {
			return
		}
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// LL-HLS loop (blocking playlist reload)
// ──────────────────────────────────────────────────────────────────────────────

func (c *Client) runLLHLS(ctx context.Context, mediaURL string, pl *m3u8.MediaPlaylist) {
	var lastMapURI string
	prev := pl
	consecutiveBlockingErr := 0
	consecutiveParseErr := 0
	consecutiveNoAdvance := 0

	// Tune-in: if Age >= PartTarget, we may need to advance the blocking target.
	partTarget := pl.PartInf.PartTarget
	if partTarget <= 0 {
		partTarget = pl.TargetDuration
	}
	if partTarget <= 0 {
		partTarget = defaultReloadSeconds
	}

	// Request cursor for blocking reloads tracks the playlist edge, not download state.
	nextMSN, nextPart := computeNextBlockingTarget(pl, -1, -1)

	// Keep lightweight media progress so we only move forward and avoid stale backfill.
	lastSegmentMSN := -1
	lastPartMSN := -1
	lastPartIndex := -1

	// Tune-in: fetch only the newest complete segment once (no backlog).
	if seg := latestCompleteSegment(pl); seg != nil {
		if seg.MapURI != "" {
			c.fetchInit(ctx, seg.MapURI)
			lastMapURI = seg.MapURI
		}
		if seg.URI != "" {
			c.fetchSegment(ctx, seg.URI, metrics.KindSegment)
		}
		lastSegmentMSN = seg.MSN
		lastPartMSN = seg.MSN
		lastPartIndex = -1
	}

	for {
		if ctx.Err() != nil {
			return
		}

		params := map[string]string{
			"_HLS_msn":  strconv.Itoa(nextMSN),
			"_HLS_part": strconv.Itoa(nextPart),
		}

		// Blocking playlist request.
		mpr, err := c.dl.FetchPlaylist(ctx, mediaURL, params)
		if err != nil {
			consecutiveBlockingErr++
			if shouldReportRequestErr(ctx, err) {
				c.emitErr(metrics.KindPlaylist, err)
			}
			// Back off and retry blocking reload at the latest known edge.
			retryWait := normalizeReloadDuration(partTarget, pl.TargetDuration)
			retryWait = backoffDuration(retryWait, consecutiveBlockingErr-1)
			if !sleepWithContext(ctx, c.withClientJitter(retryWait)) {
				return
			}
			continue
		}
		consecutiveBlockingErr = 0
		c.emitPlaylist(mpr)
		if mpr.StatusCode == http.StatusNoContent {
			consecutiveNoAdvance++
			retryWait := normalizeReloadDuration(partTarget, pl.TargetDuration)
			retryWait = backoffDuration(retryWait, consecutiveNoAdvance-1)
			if !sleepWithContext(ctx, c.withClientJitter(retryWait)) {
				return
			}
			continue
		}

		newPL, err := m3u8.ParseMedia(mediaURL, mpr.Body, prev)
		if err != nil {
			consecutiveParseErr++
			retryWait := normalizeReloadDuration(partTarget, pl.TargetDuration)
			retryWait = backoffDuration(retryWait, consecutiveParseErr-1)
			if !sleepWithContext(ctx, c.withClientJitter(retryWait)) {
				return
			}
			continue
		}
		consecutiveParseErr = 0
		playlistAdvanced := hasPlaylistAdvanced(pl, newPL)
		prev = newPL
		pl = newPL

		// Fetch only newest media to avoid stale part backfill and duplicate requests.
		if seg := latestCompleteSegment(pl); seg != nil && seg.MSN > lastSegmentMSN {
			if seg.MapURI != "" && seg.MapURI != lastMapURI {
				c.fetchInit(ctx, seg.MapURI)
				lastMapURI = seg.MapURI
			}
			// If we already fetched parts for this MSN, skip full segment fetch.
			if !(lastPartMSN == seg.MSN && lastPartIndex >= 0) && seg.URI != "" {
				c.fetchSegment(ctx, seg.URI, metrics.KindSegment)
			}
			lastSegmentMSN = seg.MSN
			if lastPartMSN < seg.MSN {
				lastPartMSN = seg.MSN
				lastPartIndex = -1
			}
		}

		if tail := latestOpenSegment(pl); tail != nil {
			if p := latestPart(tail.Parts); p != nil {
				if p.MSN > lastPartMSN || (p.MSN == lastPartMSN && p.PartIndex > lastPartIndex) {
					if tail.MapURI != "" && tail.MapURI != lastMapURI {
						c.fetchInit(ctx, tail.MapURI)
						lastMapURI = tail.MapURI
					}
					c.fetchSegment(ctx, p.URI, metrics.KindPart)
					lastPartMSN = p.MSN
					lastPartIndex = p.PartIndex
				}
			}
		}

		nextMSN, nextPart = advanceBlockingTarget(pl, nextMSN, nextPart)

		// Some origins may return immediately for blocking requests when there is
		// no new media yet; pace retries to avoid over-polling the playlist.
		if !playlistAdvanced {
			consecutiveNoAdvance++
			retryWait := normalizeReloadDuration(partTarget, pl.TargetDuration)
			retryWait = backoffDuration(retryWait, consecutiveNoAdvance-1)
			if !sleepWithContext(ctx, c.withClientJitter(retryWait)) {
				return
			}
		} else {
			consecutiveNoAdvance = 0
		}

		if pl.HasEndList {
			return
		}
	}
}

func latestCompleteSegment(pl *m3u8.MediaPlaylist) *m3u8.Segment {
	if pl == nil {
		return nil
	}
	for i := len(pl.Segments) - 1; i >= 0; i-- {
		if pl.Segments[i].IsComplete {
			return pl.Segments[i]
		}
	}
	return nil
}

func latestOpenSegment(pl *m3u8.MediaPlaylist) *m3u8.Segment {
	if pl == nil || len(pl.Segments) == 0 {
		return nil
	}
	last := pl.Segments[len(pl.Segments)-1]
	if last.IsComplete {
		return nil
	}
	return last
}

func latestPart(parts []*m3u8.Part) *m3u8.Part {
	if len(parts) == 0 {
		return nil
	}
	return parts[len(parts)-1]
}

func advanceBlockingTarget(pl *m3u8.MediaPlaylist, currentMSN, currentPart int) (int, int) {
	if pl == nil {
		if currentMSN < 0 {
			return 0, 0
		}
		return currentMSN, currentPart + 1
	}

	if lp := pl.LastPart(); lp != nil {
		if lp.MSN > currentMSN || (lp.MSN == currentMSN && lp.PartIndex >= currentPart) {
			return lp.MSN, lp.PartIndex + 1
		}
	}

	if ls := pl.LastMSN(); ls >= 0 {
		if ls >= currentMSN {
			return ls + 1, 0
		}
	}

	if currentMSN < 0 {
		return pl.MediaSequence, 0
	}
	return currentMSN, currentPart + 1
}

// computeNextBlockingTarget returns the _HLS_msn and _HLS_part to request next.
func computeNextBlockingTarget(pl *m3u8.MediaPlaylist, lastMSN, lastPart int) (int, int) {
	if lastMSN < 0 {
		// Haven't downloaded anything yet; ask for the very next part after the
		// last advertised one.
		lp := pl.LastPart()
		ls := pl.LastMSN()
		if lp != nil {
			return lp.MSN, lp.PartIndex + 1
		}
		if ls >= 0 {
			return ls + 1, 0
		}
		return pl.MediaSequence, 0
	}

	if lastPart < 0 {
		// Last thing was a complete segment — request next segment's first part.
		return lastMSN + 1, 0
	}
	// Last thing was a part — request the next part.
	return lastMSN, lastPart + 1
}

func hasPlaylistAdvanced(prev, next *m3u8.MediaPlaylist) bool {
	if prev == nil || next == nil {
		return true
	}
	if next.MediaSequence != prev.MediaSequence {
		return true
	}

	prevMSN := prev.LastMSN()
	nextMSN := next.LastMSN()
	if nextMSN != prevMSN {
		return true
	}

	prevLastPart := prev.LastPart()
	nextLastPart := next.LastPart()
	prevPart := -1
	nextPart := -1
	prevPartMSN := -1
	nextPartMSN := -1
	if prevLastPart != nil {
		prevPart = prevLastPart.PartIndex
		prevPartMSN = prevLastPart.MSN
	}
	if nextLastPart != nil {
		nextPart = nextLastPart.PartIndex
		nextPartMSN = nextLastPart.MSN
	}
	if nextPartMSN != prevPartMSN || nextPart != prevPart {
		return true
	}

	prevHint := ""
	nextHint := ""
	if prev.PreloadHint != nil && prev.PreloadHint.Type == "PART" {
		prevHint = prev.PreloadHint.URI
	}
	if next.PreloadHint != nil && next.PreloadHint.Type == "PART" {
		nextHint = next.PreloadHint.URI
	}
	return nextHint != prevHint
}

func secondsToDuration(seconds float64) time.Duration {
	if seconds <= 0 {
		return 0
	}
	d := time.Duration(seconds * float64(time.Second))
	if d <= 0 {
		return 0
	}
	if d < minReloadInterval {
		return minReloadInterval
	}
	return d
}

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

func normalizeReloadDuration(primarySeconds, fallbackSeconds float64) time.Duration {
	if d := secondsToDuration(primarySeconds); d > 0 {
		return d
	}
	if d := secondsToDuration(fallbackSeconds); d > 0 {
		return d
	}
	return secondsToDuration(defaultReloadSeconds)
}

func backoffDuration(base time.Duration, attempt int) time.Duration {
	if base <= 0 {
		base = secondsToDuration(defaultReloadSeconds)
	}
	if attempt <= 0 {
		return base
	}

	factor := 1
	for i := 0; i < attempt && factor < maxRetryBackoff; i++ {
		factor *= 2
	}
	if factor > maxRetryBackoff {
		factor = maxRetryBackoff
	}
	return time.Duration(factor) * base
}

func (c *Client) withClientJitter(base time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	spread := base / 10 // +/-10% deterministic jitter by client ID
	if spread <= 0 {
		return base
	}

	slot := (c.id % 7) - 3 // range [-3, +3]
	jittered := base + (time.Duration(slot) * spread / 3)
	if jittered < minReloadInterval {
		return minReloadInterval
	}
	return jittered
}

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

func (c *Client) fetchSegment(ctx context.Context, uri string, kind metrics.EventKind) {
	if ctx.Err() != nil {
		return
	}
	res, err := c.dl.FetchSegment(ctx, uri)
	if err != nil {
		if shouldReportRequestErr(ctx, err) {
			c.emitErr(kind, err)
		}
		return
	}
	c.collector.Emit(metrics.Event{
		Kind:       kind,
		ClientID:   c.id,
		Latency:    res.Latency,
		TTFB:       res.TTFB,
		Bytes:      res.Bytes,
		HTTPStatus: res.StatusCode,
		Timestamp:  time.Now(),
	})
}

func (c *Client) fetchInit(ctx context.Context, uri string) {
	if ctx.Err() != nil {
		return
	}
	_, err := c.dl.FetchSegment(ctx, uri)
	if err != nil {
		if shouldReportRequestErr(ctx, err) {
			c.emitErr(metrics.KindSegment, fmt.Errorf("init segment: %w", err))
		}
	}
}

func (c *Client) emitPlaylist(pr *PlaylistResult) {
	c.collector.Emit(metrics.Event{
		Kind:       metrics.KindPlaylist,
		ClientID:   c.id,
		Latency:    pr.Latency,
		Bytes:      int64(len(pr.Body)),
		HTTPStatus: pr.StatusCode,
		Timestamp:  time.Now(),
	})
}

func (c *Client) emitErr(kind metrics.EventKind, err error) {
	c.collector.Emit(metrics.Event{
		Kind:      kind,
		ClientID:  c.id,
		Err:       err,
		Timestamp: time.Now(),
	})
}

func shouldReportRequestErr(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if ctx == nil || ctx.Err() == nil {
		return true
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var nerr net.Error
	if errors.As(err, &nerr) && nerr.Timeout() {
		return false
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "context canceled") || strings.Contains(msg, "context deadline exceeded") {
		return false
	}
	return true
}

// ──────────────────────────────────────────────────────────────────────────────
// Variant selection
// ──────────────────────────────────────────────────────────────────────────────

// selectVariant picks a variant from the master playlist according to mode.
// mode: "highest", "lowest", "bw:<bits>"
func selectVariant(master *m3u8.MasterPlaylist, mode string) *m3u8.Variant {
	if len(master.Variants) == 0 {
		return nil
	}

	switch {
	case mode == "highest":
		best := master.Variants[0]
		for _, v := range master.Variants[1:] {
			if v.Bandwidth > best.Bandwidth {
				best = v
			}
		}
		return best

	case mode == "lowest":
		best := master.Variants[0]
		for _, v := range master.Variants[1:] {
			if v.Bandwidth < best.Bandwidth {
				best = v
			}
		}
		return best

	case strings.HasPrefix(mode, "bw:"):
		targetBW, err := strconv.Atoi(mode[3:])
		if err != nil {
			return master.Variants[0]
		}
		// Find the highest-bandwidth variant at or below the target.
		var best *m3u8.Variant
		for _, v := range master.Variants {
			if v.Bandwidth <= targetBW {
				if best == nil || v.Bandwidth > best.Bandwidth {
					best = v
				}
			}
		}
		if best == nil {
			// All variants exceed target — pick the lowest.
			best = master.Variants[0]
			for _, v := range master.Variants[1:] {
				if v.Bandwidth < best.Bandwidth {
					best = v
				}
			}
		}
		return best

	default:
		return master.Variants[0]
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// RunAll
// ──────────────────────────────────────────────────────────────────────────────

// RunAll spawns n virtual clients, optionally staggering their starts over
// rampUp. Each client runs until ctx is cancelled.
func RunAll(ctx context.Context, cfg *Config, n int, rampUp time.Duration, collector *metrics.Collector) {
	var wg sync.WaitGroup

	var delay time.Duration
	if n > 1 && rampUp > 0 {
		delay = rampUp / time.Duration(n)
	}

	for i := 0; i < n; i++ {
		if ctx.Err() != nil {
			break
		}
		if i > 0 && delay > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
		}

		wg.Add(1)
		collector.IncActive()
		id := i + 1

		go func(clientID int) {
			defer wg.Done()
			defer collector.DecActive()
			cl := newClient(clientID, cfg, collector)
			defer cl.Close()
			cl.Run(ctx)
		}(id)
	}

	wg.Wait()
}
