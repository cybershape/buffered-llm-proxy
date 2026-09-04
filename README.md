# buffered-llm-proxy

High-performance, semantic-coalescing streaming proxy for OpenAI-compatible Chat Completions, written in Go.

It sits between upstream AI providers (or CLIProxyAPI) and downstream clients, buffering and aggregating fragmented Server-Sent Events (SSE) into larger batches driven by downstream network throughput, rather than rigid timers.

---

## Features

- **Decoupled Concurrency Model**: Upstream reader ingests tokens as fast as possible; downstream writer consumes accumulated batches via atomic snapshots.
- **Adaptive Batching**: Slower clients automatically receive larger batches; fast clients experience near-zero latency.
- **Protocol & Semantic Preservation**:
  - **Reasoning**: Preserves original reasoning fields (`reasoning_content`, `reasoning`, `reasoning_text`, `thought`).
  - **Content**: Coalesces adjacent text deltas for the same choice.
  - **Tool Calls**: Concatenates arguments byte-for-byte; isolates multiple tool calls across `(choice.index, tool_call.index)`.
  - **Role Idempotence**: Ignores duplicate role deltas without disrupting aggregation.
  - **Strict Barriers**: Preserves causal ordering between reasoning, content, tool calls, finish reasons, and `[DONE]`.
- **Transparent Pass-Through**:
  - `POST /v1/chat/completions` with `stream=false` is proxied transparently.
  - `GET /v1/models` is passed through directly.
- **Backpressure & Bounded Memory**: Configurable high/low watermarks (defaults: 32MB / 24MB) pause the upstream reader when the client is blocked.
- **Metrics**: Real-time stats and coalescing ratios available via `/metrics`.

---

## Quick Start

### Installation

#### Download Prebuilt Binary
Download the latest Linux release from [GitHub Releases](https://github.com/cybershape/buffered-llm-proxy/releases).

#### Build from Source
```bash
git clone https://github.com/cybershape/buffered-llm-proxy.git
cd buffered-llm-proxy
go build -o buffered-proxy ./cmd/proxy
```

### Running the Proxy

```bash
./buffered-proxy -host 0.0.0.0 -port 8080 -upstream http://127.0.0.1:8000
```

### CLI Flags

| Flag | Default | Description |
| :--- | :--- | :--- |
| `-host` | `0.0.0.0` | Listen host |
| `-port` | `8080` | Listen port |
| `-upstream` | `http://127.0.0.1:8000` | Target upstream base URL |
| `-max-buffer-mb` | `32` | High watermark memory buffer limit per stream (MB) |
| `-low-water-mb` | `24` | Low watermark memory buffer threshold (MB) |
| `-min-coalesce-ms` | `0` | Optional minimum coalesce delay in milliseconds |
| `-enable-metrics` | `true` | Enable `/metrics` endpoint |

---

## Example Usage

### Streaming Request (Aggregated)

```bash
curl -N -X POST http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{
    "model": "deepseek-r1",
    "stream": true,
    "messages": [
      {"role": "user", "content": "Explain quantum computing briefly"}
    ]
  }'
```

### Non-streaming Request (Transparent Pass-through)

```bash
curl -X POST http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{
    "model": "gpt-4o",
    "stream": false,
    "messages": [
      {"role": "user", "content": "Hello!"}
    ]
  }'
```

### Model Listing

```bash
curl http://localhost:8080/v1/models \
  -H "Authorization: Bearer $API_KEY"
```

### Inspecting Metrics

```bash
curl http://localhost:8080/metrics
```

---

## Documentation

- [Architecture & Engine Design](docs/architecture.md)
- [API Reference & Usage Guide](docs/api_and_usage.md)
- [Agent & Engineering Guidelines](AGENTS.md)

---

## Testing

Run unit and end-to-end race detector tests:

```bash
go test -race -v ./...
```
