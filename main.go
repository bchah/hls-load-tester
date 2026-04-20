package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bchah/hls-load-tester/internal/client"
	"github.com/bchah/hls-load-tester/internal/metrics"
	"github.com/bchah/hls-load-tester/internal/reporter"
)

// version is set at build time via -ldflags.
var version = "dev"

func main() {
	cfg, opts, err := parseFlags()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n\nUsage:\n", err)
		printUsage()
		os.Exit(1)
	}
	if opts.showVersion {
		fmt.Println("hls-load-tester", version)
		return
	}
	if opts.showHelp {
		printUsage()
		return
	}

	// Build context: cancelled on SIGINT/SIGTERM or when duration elapses.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	if opts.duration > 0 {
		var durationCancel context.CancelFunc
		ctx, durationCancel = context.WithTimeout(ctx, opts.duration)
		defer durationCancel()
	}

	// Set up collector.
	collector := metrics.NewCollector()

	// Optional NDJSON log.
	if opts.logFile != "" {
		f, err := os.Create(opts.logFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "cannot open log file: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		enc := json.NewEncoder(f)
		collector.LogWriter = func(e metrics.Event) {
			type logEntry struct {
				Kind       string  `json:"kind"`
				ClientID   int     `json:"client_id"`
				LatencyMS  float64 `json:"latency_ms"`
				TTFBMS     float64 `json:"ttfb_ms"`
				Bytes      int64   `json:"bytes"`
				HTTPStatus int     `json:"http_status"`
				Error      string  `json:"error,omitempty"`
				Timestamp  string  `json:"timestamp"`
			}
			kindStr := [...]string{"playlist", "segment", "part", "error"}[e.Kind]
			entry := logEntry{
				Kind:       kindStr,
				ClientID:   e.ClientID,
				LatencyMS:  float64(e.Latency) / float64(time.Millisecond),
				TTFBMS:     float64(e.TTFB) / float64(time.Millisecond),
				Bytes:      e.Bytes,
				HTTPStatus: e.HTTPStatus,
				Timestamp:  e.Timestamp.UTC().Format(time.RFC3339Nano),
			}
			if e.Err != nil {
				entry.Error = e.Err.Error()
			}
			enc.Encode(entry) //nolint:errcheck
		}
	}
	collector.Start()

	// Set up reporter.
	rep := reporter.New(collector, os.Stdout, cfg.ManifestURL, opts.clients, opts.duration, opts.interval)
	rep.Start()

	// Launch clients in a goroutine so we can also listen for signals.
	done := make(chan struct{})
	go func() {
		defer close(done)
		client.RunAll(ctx, cfg, opts.clients, opts.rampUp, collector)
	}()

	// Wait for completion or interrupt.
	select {
	case <-sigCh:
		cancel()
	case <-done:
	case <-ctx.Done():
	}

	// Drain remaining clients after cancel.
	<-done

	rep.Stop()
	collector.Stop()
	rep.PrintSummary()
}

// ─── CLI parsing ──────────────────────────────────────────────────────────────

type options struct {
	clients     int
	duration    time.Duration
	rampUp      time.Duration
	interval    time.Duration
	logFile     string
	showVersion bool
	showHelp    bool
}

// parseFlags does minimal flag parsing without importing flag or cobra.
func parseFlags() (*client.Config, *options, error) {
	args := os.Args[1:]

	cfg := &client.Config{
		Rendition: "highest",
		Timeout:   60 * time.Second,
		UserAgent: "hls-load-tester/" + version,
	}
	opts := &options{
		clients:  10,
		duration: 60 * time.Second,
		interval: 1 * time.Second,
	}

	positional := []string{}

	for i := 0; i < len(args); i++ {
		a := args[i]
		switch a {
		case "-h", "--help":
			opts.showHelp = true
			return cfg, opts, nil
		case "--version":
			opts.showVersion = true
			return cfg, opts, nil
		case "-c", "--clients":
			i++
			if i >= len(args) {
				return nil, nil, fmt.Errorf("%s requires a value", a)
			}
			var v int
			if _, err := fmt.Sscanf(args[i], "%d", &v); err != nil || v < 1 {
				return nil, nil, fmt.Errorf("--clients must be a positive integer")
			}
			opts.clients = v
		case "-d", "--duration":
			i++
			if i >= len(args) {
				return nil, nil, fmt.Errorf("%s requires a value", a)
			}
			d, err := time.ParseDuration(args[i])
			if err != nil {
				return nil, nil, fmt.Errorf("--duration: %w", err)
			}
			opts.duration = d
		case "-r", "--rendition":
			i++
			if i >= len(args) {
				return nil, nil, fmt.Errorf("%s requires a value", a)
			}
			cfg.Rendition = args[i]
		case "--ramp-up":
			i++
			if i >= len(args) {
				return nil, nil, fmt.Errorf("%s requires a value", a)
			}
			d, err := time.ParseDuration(args[i])
			if err != nil {
				return nil, nil, fmt.Errorf("--ramp-up: %w", err)
			}
			opts.rampUp = d
		case "-t", "--timeout":
			i++
			if i >= len(args) {
				return nil, nil, fmt.Errorf("%s requires a value", a)
			}
			d, err := time.ParseDuration(args[i])
			if err != nil {
				return nil, nil, fmt.Errorf("--timeout: %w", err)
			}
			cfg.Timeout = d
		case "--ua":
			i++
			if i >= len(args) {
				return nil, nil, fmt.Errorf("%s requires a value", a)
			}
			cfg.UserAgent = args[i]
		case "--no-http2":
			cfg.NoHTTP2 = true
		case "--download-backlog":
			cfg.DownloadBacklog = true
		case "--interval":
			i++
			if i >= len(args) {
				return nil, nil, fmt.Errorf("%s requires a value", a)
			}
			d, err := time.ParseDuration(args[i])
			if err != nil {
				return nil, nil, fmt.Errorf("--interval: %w", err)
			}
			opts.interval = d
		case "--log":
			i++
			if i >= len(args) {
				return nil, nil, fmt.Errorf("%s requires a value", a)
			}
			opts.logFile = args[i]
		default:
			if len(a) > 0 && a[0] == '-' {
				return nil, nil, fmt.Errorf("unknown flag: %s", a)
			}
			positional = append(positional, a)
		}
	}

	if len(positional) == 0 {
		return nil, nil, fmt.Errorf("URL argument is required")
	}
	if len(positional) > 1 {
		return nil, nil, fmt.Errorf("unexpected extra arguments: %v", positional[1:])
	}
	cfg.ManifestURL = positional[0]
	return cfg, opts, nil
}

func printUsage() {
	fmt.Print(`Usage: hls-load-tester [flags] <url>

  <url>   HLS master or media playlist URL to test.

Flags:
  -c, --clients <n>          Number of concurrent virtual clients (default: 10)
  -d, --duration <duration>  Test duration, e.g. 60s, 5m, 0=forever (default: 60s)
  -r, --rendition <mode>     Variant selection: highest | lowest | bw:<bits> (default: highest)
      --ramp-up <duration>   Spread client starts over this duration (default: 0)
	  -t, --timeout <duration>   Per-request HTTP timeout (default: 60s)
      --ua <string>          User-Agent header (default: hls-load-tester/dev)
      --no-http2             Disable HTTP/2
	      --download-backlog     Download entire live window backlog at tune-in
      --interval <duration>  Dashboard refresh interval (default: 2s)
      --log <file>           Write NDJSON event log to file
  -h, --help                 Show this help
      --version              Print version

Examples:
  hls-load-tester https://example.com/stream.m3u8
  hls-load-tester -c 50 -d 5m --ramp-up 30s https://example.com/stream.m3u8
	  hls-load-tester -c 10 -r lowest --log events.ndjson https://example.com/live.m3u8
`)
}
