# API Reference & Usage Guide

## 1. Build & Run

### Build Binary
```bash
go build -o buffered-proxy ./cmd/proxy
```

### Start Proxy Service
```bash
./buffered-proxy \
  -host 0.0.0.0 \
  -port 8080 \
  -upstream http://127.0.0.1:8000 \
  -max-buffer-mb 32 \
  -low-water-mb 24
```

---

## 2. Command-Line Options

| Flag | Default | Description |
| :--- | :--- | :--- |
| `-host` | `0.0.0.0` | IP address / host to bind the proxy server |
| `-port` | `8080` | Port to listen on |
| `-upstream` | `http://127.0.0.1:8000` | Target AI provider or CLIProxyAPI URL |
| `-max-buffer-mb` | `32` | Maximum per-stream buffer memory limit (high watermark in MB), triggers upstream backpressure when reached |
| `-low-water-mb` | `24` | Buffer low watermark threshold (MB) to resume upstream reader |
| `-min-coalesce-ms` | `0` | Optional cooperative coalesce delay in milliseconds (default: 0, purely downstream-driven) |
| `-enable-metrics` | `true` | Enable `/metrics` monitoring endpoint |

---

## 3. Supported Endpoints & Routing

### 3.1 `POST /v1/chat/completions` (Streaming Aggregation)
Triggered when request payload specifies `"stream": true`:
- Spawns independent upstream reader and downstream writer routines.
- Coalesces reasoning tokens, content deltas, and tool call argument fragments safely.
- Ignores repeated roles without creating barriers.
- Enforces strict semantic order (`Reasoning -> Content -> Tool Calls -> Content -> Finish`).
- Automatically passes through upstream errors or non-200 HTTP statuses.

#### Example Request
```bash
curl -N -X POST http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{
    "model": "deepseek-r1",
    "stream": true,
    "messages": [{"role": "user", "content": "Hello"}]
  }'
```

---

### 3.2 `POST /v1/chat/completions` (Non-streaming Transparent Pass-through)
When `"stream": false` or omitted, requests are transparently reverse-proxied to upstream without alteration.

---

### 3.3 `GET /v1/models` (Transparent Pass-through)
Passes through model list requests directly, preserving upstream headers and response body.

#### Example Request
```bash
curl http://localhost:8080/v1/models \
  -H "Authorization: Bearer $API_KEY"
```

---

### 3.4 `GET /metrics` (Monitoring & Metrics)
Exposes aggregated throughput and coalescing statistics since server start:

```json
{
  "upstream_sse_events": 1420,
  "downstream_sse_events": 118,
  "overall_coalescing_ratio": 12.03,
  "reasoning_fragments_in": 600,
  "reasoning_events_out": 42,
  "reasoning_coalescing_ratio": 14.28,
  "content_fragments_in": 520,
  "content_events_out": 40,
  "content_coalescing_ratio": 13.00,
  "tool_fragments_in": 300,
  "tool_events_out": 36,
  "tool_coalescing_ratio": 8.33,
  "upstream_bytes": 1048576,
  "downstream_bytes": 985120,
  "pending_bytes_max": 204800,
  "reader_pause_count": 0,
  "reader_pause_duration_ns": 0,
  "downstream_write_ns": 125000000
}
```
