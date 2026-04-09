package m3u8

// MasterPlaylist represents a parsed HLS multivariant playlist.
type MasterPlaylist struct {
	Variants []*Variant
}

// Variant represents a single EXT-X-STREAM-INF entry.
type Variant struct {
	Bandwidth    int
	AvgBandwidth int
	Resolution   string
	Codecs       string
	FrameRate    float64
	Score        float64
	URI          string
	AudioGroupID string
	VideoGroupID string
}

// MediaPlaylist represents a parsed HLS media playlist (chunklist).
type MediaPlaylist struct {
	Version        int
	TargetDuration float64
	MediaSequence  int
	PartInf        PartInf
	ServerControl  ServerControl
	IsLive         bool // no EXT-X-ENDLIST, no EXT-X-PLAYLIST-TYPE:VOD
	IsVOD          bool // EXT-X-PLAYLIST-TYPE:VOD or EXT-X-ENDLIST present
	HasEndList     bool
	Segments       []*Segment
	PreloadHint    *PreloadHint
}

// LastMSN returns the Media Sequence Number of the last segment.
// Returns -1 if there are no segments.
func (p *MediaPlaylist) LastMSN() int {
	if len(p.Segments) == 0 {
		return -1
	}
	return p.Segments[len(p.Segments)-1].MSN
}

// LastPart returns the last Part from the last segment's parts, or nil.
func (p *MediaPlaylist) LastPart() *Part {
	if len(p.Segments) == 0 {
		return nil
	}
	last := p.Segments[len(p.Segments)-1]
	if len(last.Parts) == 0 {
		return nil
	}
	return last.Parts[len(last.Parts)-1]
}

// ServerControl holds EXT-X-SERVER-CONTROL attributes.
type ServerControl struct {
	CanBlockReload    bool
	CanSkipUntil      float64 // seconds
	CanSkipDateRanges bool
	HoldBack          float64 // seconds
	PartHoldBack      float64 // seconds
}

// PartInf holds EXT-X-PART-INF attributes.
type PartInf struct {
	PartTarget float64 // seconds
}

// Segment represents a media segment and its associated partial segments.
type Segment struct {
	MSN        int
	Duration   float64
	URI        string
	MapURI     string // EXT-X-MAP URI (fMP4 init section)
	Parts      []*Part
	IsComplete bool // EXTINF has been seen (full segment, not just parts)
	HasGap     bool // EXT-X-GAP
}

// Part represents a partial segment (EXT-X-PART).
type Part struct {
	MSN         int
	PartIndex   int
	Duration    float64
	URI         string
	Independent bool
	HasGap      bool
}

// PreloadHint represents an EXT-X-PRELOAD-HINT tag.
type PreloadHint struct {
	Type          string // "PART" or "MAP"
	URI           string
	ByteRangeStart int64
	ByteRangeLength int64 // -1 if unknown
}
