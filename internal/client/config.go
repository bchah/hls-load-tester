package client

import "time"

// Config holds all runtime configuration shared across virtual clients.
type Config struct {
	// ManifestURL is the HLS master or media playlist URL to test.
	ManifestURL string

	// Rendition controls which variant stream each client picks.
	// Valid values: "highest", "lowest", "bw:<bits>" (e.g. "bw:4000000").
	Rendition string

	// Timeout is the per-request HTTP timeout.
	Timeout time.Duration

	// UserAgent is sent in every request.
	UserAgent string

	// NoHTTP2 disables HTTP/2 negotiation.
	NoHTTP2 bool

	// DownloadBacklog makes live mode fetch all segments currently visible in
	// the initial playlist window at tune-in. Disabled by default for
	// browser-like behavior.
	DownloadBacklog bool
}
