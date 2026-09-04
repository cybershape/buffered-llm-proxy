# AGENTS.md

## Project Overview

This project is a high-performance, semantic-segment-coalescing streaming aggregation proxy for OpenAI-compatible Chat Completions, written in Go.

## Architecture & Core Principles

1. **Decoupled Concurrency Model**:
   - The Upstream Reader and Downstream Writer are fully decoupled concurrently.
   - The Reader continuously reads upstream SSE chunks at maximum network ingestion speed, parses them into semantic segments, and buffers them in bounded memory.
   - The Writer is downstream-completion-driven: after each network write and flush finishes, it atomically swaps the currently accumulated snapshot and flushes it to the client.
   - The slower the downstream client/network, the larger the batch size automatically becomes; the faster the downstream, the closer output latency is to real-time.

2. **Strict Semantic Ordering & Barrier**:
   - Preserves upstream semantic event ordering without cross-segment reordering.
   - Reasoning -> Content forms a barrier (never mixed in a single delta).
   - Content -> Tool Call forms a barrier.
   - Tool Call -> Content forms a barrier.
   - `finish_reason`, `usage`, `[DONE]`, and `error` form control barriers, ensuring all preceding buffered tokens are flushed before emission.
   - Repeated `role` deltas for the same choice are ignored and never form a barrier, preventing unnecessary segmentation.

3. **Protocol & Schema Compatibility**:
   - Preserves original reasoning field names (such as `reasoning_content`, `reasoning`, `reasoning_text`, `thought`), without changing schema.
   - Tool call arguments are concatenated byte-by-byte as raw strings without parsing or stringifying partial JSON.
   - Preserves common response metadata (`id`, `model`, `system_fingerprint`, etc.) and passes through unrecognized custom extensions.
   - Transparently passes through `GET /v1/models` and non-streaming `POST /v1/chat/completions` (`stream=false`).

## Engineering Guidelines

- Code style follows official Go standards (`gofmt`), with no redundant comments.
- Must pass race detection before commit: `go test -race ./...`.
- Update corresponding `fixtures/` and `tests/` test cases when modifying aggregation rules.
