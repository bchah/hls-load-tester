package client

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/streamwell/hls-load-tester/internal/m3u8"
	"github.com/streamwell/hls-load-tester/internal/metrics"
)

// Client simulates a single HLS viewer session.
type Client struct {
	id        int
	cfg       *Config
	dl        *Downloader
	collector *metrics.Collector
}

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
		c.emitErr(metrics.KindPlaylist, err)
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
		c.emitErr(metrics.KindPlaylist, err)
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
		c.runLLHLS(ctx, mediaURL, pl, mpr.Age)
	} else {
		c.runLive(ctx, mediaURL, pl)
	}
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
			c.fetchSegment(ctx, seg.MapURI, metrics.KindSegment)
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

	// Download all backlogged segments up to the live edge on first load.
	for _, seg := range pl.Segments {
		if ctx.Err() != nil {
			return
		}
		if seg.MapURI != "" && seg.MapURI != lastMapURI {
			c.fetchSegment(ctx, seg.MapURI, metrics.KindSegment)
			lastMapURI = seg.MapURI
		}
		c.fetchSegment(ctx, seg.URI, metrics.KindSegment)
		lastDownloadedMSN = seg.MSN
	}

	for {
		if ctx.Err() != nil {
			return
		}

		// Compute reload wait: last segment duration on success, targetDuration/2 on retry.
		reloadWait := pl.TargetDuration / 2
		if len(pl.Segments) > 0 {
			last := pl.Segments[len(pl.Segments)-1]
			if last.Duration > 0 {
				reloadWait = last.Duration
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(reloadWait * float64(time.Second))):
		}

		mpr, err := c.dl.FetchPlaylist(ctx, mediaURL, nil)
		if err != nil {
			c.emitErr(metrics.KindPlaylist, err)
			// On error, wait half target duration before retrying.
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(prev.TargetDuration / 2 * float64(time.Second))):
			}
			continue
		}
		c.emitPlaylist(mpr)

		newPL, err := m3u8.ParseMedia(mediaURL, mpr.Body, prev)
		if err != nil {
			continue
		}
		prev = newPL
		pl = newPL

		// Download segments that are new since last download.
		for _, seg := range pl.Segments {
			if seg.MSN <= lastDownloadedMSN {
				continue
			}
			if ctx.Err() != nil {
				return
			}
			if seg.MapURI != "" && seg.MapURI != lastMapURI {
				c.fetchSegment(ctx, seg.MapURI, metrics.KindSegment)
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

func (c *Client) runLLHLS(ctx context.Context, mediaURL string, pl *m3u8.MediaPlaylist, initialAge float64) {
	var lastMapURI string
	prev := pl

	// Tune-in: if Age >= PartTarget, we may need to advance the blocking target.
	partTarget := pl.PartInf.PartTarget
	if partTarget <= 0 {
		partTarget = 1.0 // fallback
	}

	// Seek to the live edge — do NOT download the full window backlog.
	lastDownloadedMSN := -1
	lastDownloadedPart := -1

	for _, seg := range pl.Segments {
		// Fetch any new init section exactly once — it is tiny and required.
		if seg.MapURI != "" && seg.MapURI != lastMapURI {
			c.fetchSegment(ctx, seg.MapURI, metrics.KindSegment)
			lastMapURI = seg.MapURI
		}
		// Advance position tracker to the live edge without downloading content.
		if seg.IsComplete {
			lastDownloadedMSN = seg.MSN
			lastDownloadedPart = -1
		}
		for _, p := range seg.Parts {
			lastDownloadedMSN = p.MSN
			lastDownloadedPart = p.PartIndex
		}
	}

	// If the playlist advertises a preload hint, count it as already seen so
	// the blocking loop requests the part after it rather than the hint itself.
	if pl.PreloadHint != nil && pl.PreloadHint.Type == "PART" {
		if lastDownloadedPart < 0 {
			lastDownloadedPart = 0
		} else {
			lastDownloadedPart++
		}
	}

	for {
		if ctx.Err() != nil {
			return
		}

		// Compute the next blocking request target.
		nextMSN, nextPart := computeNextBlockingTarget(pl, lastDownloadedMSN, lastDownloadedPart)

		params := map[string]string{
			"_HLS_msn":  strconv.Itoa(nextMSN),
			"_HLS_part": strconv.Itoa(nextPart),
		}

		// Add delta-skip if server supports it and we have a recent enough playlist.
		if pl.ServerControl.CanSkipUntil > 0 && initialAge < pl.ServerControl.CanSkipUntil/2 {
			params["_HLS_skip"] = "YES"
		}

		// Fire preload hint concurrently with the blocking playlist request.
		var (
			hintResult *SegmentResult
			hintDone   = make(chan struct{})
			hintMu     sync.Mutex
		)
		hintURI := ""
		if pl.PreloadHint != nil && pl.PreloadHint.Type == "PART" {
			hintURI = pl.PreloadHint.URI
			go func() {
				defer close(hintDone)
				res, err := c.dl.FetchSegment(ctx, hintURI)
				hintMu.Lock()
				hintResult = res
				hintMu.Unlock()
				if err != nil {
					c.emitErr(metrics.KindPart, err)
					return
				}
				if res != nil {
					c.collector.Emit(metrics.Event{
						Kind:       metrics.KindPart,
						ClientID:   c.id,
						Latency:    res.Latency,
						TTFB:       res.TTFB,
						Bytes:      res.Bytes,
						HTTPStatus: res.StatusCode,
						Timestamp:  time.Now(),
					})
				}
			}()
		} else {
			close(hintDone)
		}

		// Blocking playlist request.
		mpr, err := c.dl.FetchPlaylist(ctx, mediaURL, params)
		if err != nil {
			c.emitErr(metrics.KindPlaylist, err)
			<-hintDone
			// Brief back-off then retry with a plain fetch.
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(partTarget * float64(time.Second))):
			}
			// Reset with a plain fetch.
			mpr2, err2 := c.dl.FetchPlaylist(ctx, mediaURL, nil)
			if err2 != nil {
				continue
			}
			c.emitPlaylist(mpr2)
			newPL, err2 := m3u8.ParseMedia(mediaURL, mpr2.Body, prev)
			if err2 != nil {
				continue
			}
			prev = newPL
			pl = newPL
			initialAge = math.Max(0, mpr2.Age)
			continue
		}
		c.emitPlaylist(mpr)
		initialAge = math.Max(0, mpr.Age)

		newPL, err := m3u8.ParseMedia(mediaURL, mpr.Body, prev)
		if err != nil {
			<-hintDone
			continue
		}
		prev = newPL

		// Wait for hint to finish before we process the new playlist.
		<-hintDone

		// Download newly available parts and full segments.
		for _, seg := range newPL.Segments {
			if seg.IsComplete {
				if seg.MSN <= lastDownloadedMSN && lastDownloadedPart < 0 {
					continue // already have this full segment
				}
				if seg.MSN < lastDownloadedMSN {
					continue
				}
				// If we already fetched parts of this segment, skip individual part downloads.
				for _, p := range seg.Parts {
					if p.MSN < lastDownloadedMSN {
						continue
					}
					if p.MSN == lastDownloadedMSN && p.PartIndex <= lastDownloadedPart {
						continue
					}
					// Skip the part if its URI matches the hint we already fetched.
					if p.URI != hintURI || hintResult == nil {
						c.fetchSegment(ctx, p.URI, metrics.KindPart)
					}
					lastDownloadedMSN = p.MSN
					lastDownloadedPart = p.PartIndex
				}
				if seg.MapURI != "" && seg.MapURI != lastMapURI {
					c.fetchSegment(ctx, seg.MapURI, metrics.KindSegment)
					lastMapURI = seg.MapURI
				}
				// Only download the combined segment URI when there are no parts —
				// if parts exist we already have all the data from them.
				if seg.URI != "" && len(seg.Parts) == 0 {
					c.fetchSegment(ctx, seg.URI, metrics.KindSegment)
				}
				lastDownloadedMSN = seg.MSN
				lastDownloadedPart = -1
			} else {
				// Partial (in-progress) segment.
				for _, p := range seg.Parts {
					if p.MSN < lastDownloadedMSN {
						continue
					}
					if p.MSN == lastDownloadedMSN && p.PartIndex <= lastDownloadedPart {
						continue
					}
					if p.URI != hintURI || hintResult == nil {
						c.fetchSegment(ctx, p.URI, metrics.KindPart)
					}
					lastDownloadedMSN = p.MSN
					lastDownloadedPart = p.PartIndex
				}
			}
		}

		pl = newPL

		if pl.HasEndList {
			return
		}
	}
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

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

func (c *Client) fetchSegment(ctx context.Context, uri string, kind metrics.EventKind) {
	if ctx.Err() != nil {
		return
	}
	res, err := c.dl.FetchSegment(ctx, uri)
	if err != nil {
		c.emitErr(kind, err)
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
			cl.Run(ctx)
		}(id)
	}

	wg.Wait()
}
