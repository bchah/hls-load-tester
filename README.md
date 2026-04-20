# hls-load-tester

Simulate concurrent HLS/LL-HLS viewers against a streaming origin or CDN. Requires Golang.

## Installation

```sh
go install github.com/bchah/hls-load-tester@latest
```

Or build from source:

```sh
git clone https://github.com/bchah/hls-load-tester
cd hls-load-tester
go build -o hls-load-tester .
```

## Usage

```
hls-load-tester [flags] <url>
```

`<url>` can be a master playlist or a direct media playlist.

## Flags

| Flag | Default | Description |
|---|---|---|
| `-c`, `--clients` | `10` | Number of concurrent virtual clients |
| `-d`, `--duration` | `60s` | Test duration (`0` = run until interrupted) |
| `-r`, `--rendition` | `highest` | Variant selection: `highest`, `lowest`, or `bw:<bits>` |
| `--ramp-up` | `0` | Spread client starts evenly over this duration |
| `-t`, `--timeout` | `60s` | Per-request HTTP timeout |
| `--ua` | `hls-load-tester/dev` | `User-Agent` header value |
| `--no-http2` | — | Force HTTP/1.1 |
| `--download-backlog` | — | Stress mode: download full live window backlog on tune-in |
| `--interval` | `2s` | Dashboard refresh interval |
| `--log` | — | Write per-request NDJSON event log to a file |
| `-h`, `--help` | — | Show help |
| `--version` | — | Print version |

## Examples

**Quick test — 10 clients, 60 seconds:**
```sh
hls-load-tester https://example.com/live/stream.m3u8
```

**100 clients ramping up over 30 seconds, run for 5 minutes:**
```sh
hls-load-tester -c 100 -d 5m --ramp-up 30s https://example.com/live/stream.m3u8
```

**Test a specific bitrate rendition:**
```sh
hls-load-tester -c 50 -r bw:4000000 https://example.com/live/stream.m3u8
```

**Capture a full event log for offline analysis:**
```sh
hls-load-tester -c 200 -d 10m --log events.ndjson https://example.com/live/stream.m3u8
```

**Force HTTP/1.1 (e.g. to test origin without HTTP/2):**
```sh
hls-load-tester -c 50 --no-http2 https://example.com/live/stream.m3u8
```

**Enable backlog stress mode in normal live playback:**
```sh
hls-load-tester -c 50 --download-backlog https://example.com/live/stream.m3u8
```

## Live Dashboard

The terminal updates every `--interval` seconds while the test runs:

```
────────────────────────────────────────────────────────────────────────
  HLS Load Tester   https://example.com/live/stream.m3u8
  Duration: 45s/300s      Clients: 100/100 active   Mode: LL-HLS
────────────────────────────────────────────────────────────────────────
  SEGMENTS   Fetched:     100  Errors:    0 ( 0.00%)  Total:    0.1 MB
             Now:    0.0 Mbps

  PLAYLISTS  Fetched:    8823  Errors:    0 ( 0.00%)  Total:  194.2 MB
             Now:   35.1 Mbps

  PARTS      Fetched:    8857  Errors:    0 ( 0.00%)  Total:    5.4 GB
             Now:  241.3 Mbps
────────────────────────────────────────────────────────────────────────
```

A final summary with average throughput is printed when the test ends.

## Log Format

When `--log` is specified each request is written as a JSON object on its own line:

```json
{"kind":"part","client_id":42,"latency_ms":312.5,"ttfb_ms":18.2,"bytes":65536,"http_status":200,"timestamp":"2026-04-09T12:00:00.000Z"}
```

## Tips

- For large client counts (>1000), raise the open-file limit first: `ulimit -n 200000`
- Use `--ramp-up` to avoid a thundering-herd spike on your origin at test start
- `--rendition lowest` is useful for isolating origin capacity from CDN caching effects
- For very high concurrency, consider increasing `--timeout` further if you observe client-side `context deadline exceeded` responses
