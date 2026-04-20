package client

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/http2"
)

// Downloader wraps an http.Client configured for one virtual client.
// Each virtual client has its own transport + connection pool so that
// connections are not shared across simulated viewers.
type Downloader struct {
	hc        *http.Client
	transport *http.Transport
	userAgent string
}

// newTransport builds an http.Transport. If useHTTP2 is true, HTTP/2 is
// negotiated via ALPN on TLS connections.
func newTransport(useHTTP2 bool) *http.Transport {
	t := &http.Transport{
		MaxIdleConns:          4,
		MaxIdleConnsPerHost:   4,
		MaxConnsPerHost:       4,
		IdleConnTimeout:       30 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    true, // we handle gzip ourselves for playlists
	}
	if useHTTP2 {
		if err := http2.ConfigureTransport(t); err != nil {
			// Fall back to HTTP/1.1 silently.
			return t
		}
	}
	return t
}

// NewDownloader creates a Downloader for one virtual client.
func NewDownloader(cfg *Config) *Downloader {
	transport := newTransport(!cfg.NoHTTP2)
	return &Downloader{
		hc: &http.Client{
			Transport: transport,
			Timeout:   cfg.Timeout,
		},
		transport: transport,
		userAgent: cfg.UserAgent,
	}
}

// Close closes idle keep-alive connections held by this downloader.
func (d *Downloader) Close() {
	if d == nil || d.transport == nil {
		return
	}
	d.transport.CloseIdleConnections()
}

// PlaylistResult holds the outcome of a playlist fetch.
type PlaylistResult struct {
	Body       string
	Age        float64 // seconds from Age header, -1 if absent
	StatusCode int
	Latency    time.Duration
}

// FetchPlaylist fetches an HLS playlist (master or media).
// params are appended as query string key=value pairs.
func (d *Downloader) FetchPlaylist(ctx context.Context, rawURL string, params map[string]string) (*PlaylistResult, error) {
	u, err := buildURL(rawURL, params)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", d.userAgent)
	req.Header.Set("Accept", "application/vnd.apple.mpegurl, */*")
	req.Header.Set("Accept-Encoding", "gzip")

	start := time.Now()
	resp, err := d.hc.Do(req)
	latency := time.Since(start)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusNoContent {
			io.Copy(io.Discard, resp.Body) //nolint:errcheck
			return &PlaylistResult{
				StatusCode: resp.StatusCode,
				Latency:    latency,
			}, nil
		}
		io.Copy(io.Discard, resp.Body) //nolint:errcheck
		return &PlaylistResult{
			StatusCode: resp.StatusCode,
			Latency:    latency,
		}, fmt.Errorf("HTTP %d: %s", resp.StatusCode, u)
	}

	// Decompress gzip if the server sent it.
	var reader io.Reader = resp.Body
	if strings.EqualFold(resp.Header.Get("Content-Encoding"), "gzip") {
		gr, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("gzip decode: %w", err)
		}
		defer gr.Close()
		reader = gr
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	age := -1.0
	if a := resp.Header.Get("Age"); a != "" {
		if f, err := strconv.ParseFloat(a, 64); err == nil {
			age = f
		}
	}

	return &PlaylistResult{
		Body:       string(data),
		Age:        age,
		StatusCode: resp.StatusCode,
		Latency:    latency,
	}, nil
}

// SegmentResult holds the outcome of a segment or part fetch.
type SegmentResult struct {
	Bytes      int64
	TTFB       time.Duration
	Latency    time.Duration
	StatusCode int
}

// FetchSegment downloads a segment (or part) and discards the bytes.
// It accurately measures TTFB and total transfer time.
func (d *Downloader) FetchSegment(ctx context.Context, segURL string) (*SegmentResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, segURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", d.userAgent)

	start := time.Now()
	resp, err := d.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	ttfb := time.Since(start)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		io.Copy(io.Discard, resp.Body) //nolint:errcheck
		return &SegmentResult{
			StatusCode: resp.StatusCode,
			TTFB:       ttfb,
			Latency:    ttfb,
		}, fmt.Errorf("HTTP %d: %s", resp.StatusCode, segURL)
	}

	n, err := io.Copy(io.Discard, resp.Body)
	latency := time.Since(start)
	if err != nil {
		return &SegmentResult{
			Bytes:      n,
			TTFB:       ttfb,
			Latency:    latency,
			StatusCode: resp.StatusCode,
		}, fmt.Errorf("read segment: %w", err)
	}

	return &SegmentResult{
		Bytes:      n,
		TTFB:       ttfb,
		Latency:    latency,
		StatusCode: resp.StatusCode,
	}, nil
}

// buildURL appends key=value query parameters to the given URL string.
func buildURL(rawURL string, params map[string]string) (string, error) {
	if len(params) == 0 {
		return rawURL, nil
	}
	sep := "?"
	if strings.Contains(rawURL, "?") {
		sep = "&"
	}
	var sb strings.Builder
	sb.WriteString(rawURL)
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	first := true
	for _, k := range keys {
		v := params[k]
		if first {
			sb.WriteString(sep)
			first = false
		} else {
			sb.WriteByte('&')
		}
		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(v)
	}
	return sb.String(), nil
}
