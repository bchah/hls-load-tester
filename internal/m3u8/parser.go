package m3u8

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// ParseMaster parses a multivariant (master) HLS playlist.
// baseURL is used to resolve relative variant URIs.
func ParseMaster(baseURL, body string) (*MasterPlaylist, error) {
	base, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid base URL: %w", err)
	}

	pl := &MasterPlaylist{}
	lines := splitLines(body)

	var pendingVariant *Variant

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || line == "#EXTM3U" {
			continue
		}

		if strings.HasPrefix(line, "#EXT-X-STREAM-INF:") {
			attrs := parseAttrs(line[len("#EXT-X-STREAM-INF:"):])
			v := &Variant{}
			if bw, ok := attrs["BANDWIDTH"]; ok {
				v.Bandwidth, _ = strconv.Atoi(bw)
			}
			if abw, ok := attrs["AVERAGE-BANDWIDTH"]; ok {
				v.AvgBandwidth, _ = strconv.Atoi(abw)
			}
			if res, ok := attrs["RESOLUTION"]; ok {
				v.Resolution = res
			}
			if codecs, ok := attrs["CODECS"]; ok {
				v.Codecs = strings.Trim(codecs, `"`)
			}
			if fr, ok := attrs["FRAME-RATE"]; ok {
				v.FrameRate, _ = strconv.ParseFloat(fr, 64)
			}
			if sc, ok := attrs["SCORE"]; ok {
				v.Score, _ = strconv.ParseFloat(sc, 64)
			}
			if audio, ok := attrs["AUDIO"]; ok {
				v.AudioGroupID = strings.Trim(audio, `"`)
			}
			if video, ok := attrs["VIDEO"]; ok {
				v.VideoGroupID = strings.Trim(video, `"`)
			}
			pendingVariant = v
			continue
		}

		if pendingVariant != nil && !strings.HasPrefix(line, "#") {
			pendingVariant.URI = resolveURI(base, line)
			pl.Variants = append(pl.Variants, pendingVariant)
			pendingVariant = nil
			continue
		}

		// Reset pending if we hit another tag before a URI
		if strings.HasPrefix(line, "#") {
			pendingVariant = nil
		}
	}

	if len(pl.Variants) == 0 {
		return nil, fmt.Errorf("no EXT-X-STREAM-INF variants found")
	}
	return pl, nil
}

// IsMasterPlaylist returns true if body looks like a multivariant playlist.
func IsMasterPlaylist(body string) bool {
	return strings.Contains(body, "#EXT-X-STREAM-INF:") || strings.Contains(body, "#EXT-X-I-FRAME-STREAM-INF:")
}

// ParseMedia parses a media (chunklist) playlist.
// baseURL resolves relative URIs. prev is the previously cached playlist used
// for delta update (EXT-X-SKIP) merging; it may be nil.
func ParseMedia(baseURL, body string, prev *MediaPlaylist) (*MediaPlaylist, error) {
	base, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid base URL: %w", err)
	}

	pl := &MediaPlaylist{}
	lines := splitLines(body)

	var (
		currentMapURI  string
		nextDuration   float64
		nextHasGap     bool
		currentMSN     int
		skippedCount   int
		inSegment      bool   // we've seen EXT-X-PART for current MSN but not EXTINF yet
		currentParts   []*Part
		partIndex      int
	)

	// We'll assign MSNs as we go, starting from MediaSequence which we parse first.
	// So we do two-pass: first pass to get MediaSequence, second to assign MSNs.
	// Simpler: track current MSN by incrementing from MediaSequence + skipped.
	msnInitialized := false

	for _, rawLine := range lines {
		line := strings.TrimSpace(rawLine)
		if line == "" || line == "#EXTM3U" {
			continue
		}

		switch {
		case strings.HasPrefix(line, "#EXT-X-VERSION:"):
			pl.Version, _ = strconv.Atoi(line[len("#EXT-X-VERSION:"):])

		case strings.HasPrefix(line, "#EXT-X-TARGETDURATION:"):
			pl.TargetDuration, _ = strconv.ParseFloat(line[len("#EXT-X-TARGETDURATION:"):], 64)

		case strings.HasPrefix(line, "#EXT-X-MEDIA-SEQUENCE:"):
			seq, _ := strconv.Atoi(line[len("#EXT-X-MEDIA-SEQUENCE:"):])
			pl.MediaSequence = seq
			currentMSN = seq
			msnInitialized = true

		case strings.HasPrefix(line, "#EXT-X-PLAYLIST-TYPE:"):
			pt := line[len("#EXT-X-PLAYLIST-TYPE:"):]
			if pt == "VOD" {
				pl.IsVOD = true
			}

		case line == "#EXT-X-ENDLIST":
			pl.HasEndList = true
			pl.IsVOD = true

		case strings.HasPrefix(line, "#EXT-X-SERVER-CONTROL:"):
			attrs := parseAttrs(line[len("#EXT-X-SERVER-CONTROL:"):])
			if v, ok := attrs["CAN-BLOCK-RELOAD"]; ok {
				pl.ServerControl.CanBlockReload = strings.EqualFold(v, "YES")
			}
			if v, ok := attrs["CAN-SKIP-UNTIL"]; ok {
				pl.ServerControl.CanSkipUntil, _ = strconv.ParseFloat(v, 64)
			}
			if v, ok := attrs["CAN-SKIP-DATERANGES"]; ok {
				pl.ServerControl.CanSkipDateRanges = strings.EqualFold(v, "YES")
			}
			if v, ok := attrs["HOLD-BACK"]; ok {
				pl.ServerControl.HoldBack, _ = strconv.ParseFloat(v, 64)
			}
			if v, ok := attrs["PART-HOLD-BACK"]; ok {
				pl.ServerControl.PartHoldBack, _ = strconv.ParseFloat(v, 64)
			}

		case strings.HasPrefix(line, "#EXT-X-PART-INF:"):
			attrs := parseAttrs(line[len("#EXT-X-PART-INF:"):])
			if v, ok := attrs["PART-TARGET"]; ok {
				pl.PartInf.PartTarget, _ = strconv.ParseFloat(v, 64)
			}

		case strings.HasPrefix(line, "#EXT-X-MAP:"):
			attrs := parseAttrs(line[len("#EXT-X-MAP:"):])
			if uri, ok := attrs["URI"]; ok {
				currentMapURI = resolveURI(base, strings.Trim(uri, `"`))
			}

		case strings.HasPrefix(line, "#EXT-X-SKIP:"):
			// Delta update — replace skipped segments with segments from prev.
			attrs := parseAttrs(line[len("#EXT-X-SKIP:"):])
			if v, ok := attrs["SKIPPED-SEGMENTS"]; ok {
				skippedCount, _ = strconv.Atoi(v)
			}
			if prev != nil && skippedCount > 0 {
				// Find where in prev's segments to pull from.
				// prev segments whose MSN < currentMSN+skippedCount are the skipped ones.
				for _, seg := range prev.Segments {
					if seg.MSN >= currentMSN && seg.MSN < currentMSN+skippedCount {
						pl.Segments = append(pl.Segments, seg)
					}
				}
			}
			currentMSN += skippedCount
			skippedCount = 0

		case strings.HasPrefix(line, "#EXTINF:"):
			// Duration, possibly followed by a comma and title.
			val := line[len("#EXTINF:"):]
			if idx := strings.Index(val, ","); idx >= 0 {
				val = val[:idx]
			}
			nextDuration, _ = strconv.ParseFloat(val, 64)

		case line == "#EXT-X-GAP":
			nextHasGap = true

		case strings.HasPrefix(line, "#EXT-X-PART:"):
			attrs := parseAttrs(line[len("#EXT-X-PART:"):])
			if !msnInitialized {
				break
			}
			p := &Part{
				MSN:       currentMSN,
				PartIndex: partIndex,
			}
			if uri, ok := attrs["URI"]; ok {
				p.URI = resolveURI(base, strings.Trim(uri, `"`))
			}
			if d, ok := attrs["DURATION"]; ok {
				p.Duration, _ = strconv.ParseFloat(d, 64)
			}
			if v, ok := attrs["INDEPENDENT"]; ok {
				p.Independent = strings.EqualFold(v, "YES")
			}
			if v, ok := attrs["GAP"]; ok {
				p.HasGap = strings.EqualFold(v, "YES")
			}
			currentParts = append(currentParts, p)
			partIndex++
			inSegment = true

		case strings.HasPrefix(line, "#EXT-X-PRELOAD-HINT:"):
			attrs := parseAttrs(line[len("#EXT-X-PRELOAD-HINT:"):])
			hint := &PreloadHint{ByteRangeLength: -1}
			if t, ok := attrs["TYPE"]; ok {
				hint.Type = t
			}
			if uri, ok := attrs["URI"]; ok {
				hint.URI = resolveURI(base, strings.Trim(uri, `"`))
			}
			if v, ok := attrs["BYTERANGE-START"]; ok {
				hint.ByteRangeStart, _ = strconv.ParseInt(v, 10, 64)
			}
			if v, ok := attrs["BYTERANGE-LENGTH"]; ok {
				hint.ByteRangeLength, _ = strconv.ParseInt(v, 10, 64)
			}
			pl.PreloadHint = hint

		case !strings.HasPrefix(line, "#"):
			// Segment URI line.
			if !msnInitialized {
				break
			}
			seg := &Segment{
				MSN:        currentMSN,
				Duration:   nextDuration,
				URI:        resolveURI(base, line),
				MapURI:     currentMapURI,
				Parts:      currentParts,
				IsComplete: true,
				HasGap:     nextHasGap,
			}
			pl.Segments = append(pl.Segments, seg)
			currentMSN++
			partIndex = 0
			currentParts = nil
			nextDuration = 0
			nextHasGap = false
			inSegment = false

		default:
			// Unknown or unhandled tag — ignore.
		}
	}

	// If there were EXT-X-PART lines but no closing EXTINF/URI yet,
	// it represents an in-progress segment at the live edge.
	if inSegment && len(currentParts) > 0 {
		seg := &Segment{
			MSN:        currentMSN,
			MapURI:     currentMapURI,
			Parts:      currentParts,
			IsComplete: false,
		}
		pl.Segments = append(pl.Segments, seg)
	}

	pl.IsLive = !pl.IsVOD && !pl.HasEndList

	return pl, nil
}

// resolveURI resolves a potentially relative URI against base.
func resolveURI(base *url.URL, ref string) string {
	r, err := url.Parse(ref)
	if err != nil {
		return ref
	}
	return base.ResolveReference(r).String()
}

// splitLines splits a playlist body into individual lines, handling both
// \r\n and \n line endings.
func splitLines(body string) []string {
	body = strings.ReplaceAll(body, "\r\n", "\n")
	return strings.Split(body, "\n")
}

// parseAttrs parses an HLS attribute list into a map.
// e.g. `BANDWIDTH=1000000,CODECS="avc1.4d401e"` → {"BANDWIDTH":"1000000","CODECS":"avc1.4d401e"}
// Handles quoted strings (which may contain commas) correctly.
func parseAttrs(s string) map[string]string {
	attrs := make(map[string]string)
	s = strings.TrimSpace(s)

	for s != "" {
		// Find key=value boundary
		eqIdx := strings.Index(s, "=")
		if eqIdx < 0 {
			break
		}
		key := strings.TrimSpace(s[:eqIdx])
		s = s[eqIdx+1:]

		var value string
		if strings.HasPrefix(s, `"`) {
			// Quoted value — scan to closing quote
			end := strings.Index(s[1:], `"`)
			if end < 0 {
				// Unclosed quote — take rest
				value = s[1:]
				s = ""
			} else {
				value = s[1 : end+1]
				s = s[end+2:]
				// Skip leading comma
				s = strings.TrimPrefix(s, ",")
			}
		} else {
			// Unquoted value — ends at next comma
			commaIdx := strings.Index(s, ",")
			if commaIdx < 0 {
				value = s
				s = ""
			} else {
				value = s[:commaIdx]
				s = s[commaIdx+1:]
			}
		}
		attrs[key] = value
	}
	return attrs
}
