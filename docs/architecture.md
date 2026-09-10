# Architecture & Aggregation Engine Design

## 1. Overall System Architecture

```text
+-------------+         +------------------+         +--------------------+         +------------+
| AI Provider | ------> | Upstream Reader  | ------> | PendingBuffer      | ------> | Downstream | ------> Client
| (Upstream)  | (SSE)   | (Fast Ingestion) | (Mutex) | (High/Low Water)   | (Swap)  | Writer     | (SSE)
+-------------+         +------------------+         +--------------------+         +------------+
```

The primary design goal is to eliminate fixed timer batching overhead (e.g., rigid 50ms timers) and avoid excessive micro-chunk writes, establishing an adaptive coalescing mechanism driven purely by downstream completion (**Downstream-Driven Snapshot**).

---

## 2. Decoupled Concurrency Model

- **Upstream Reader**:
  - Runs in a dedicated goroutine consuming upstream HTTP responses as fast as possible.
  - Reassembles raw chunked network transfers into complete SSE frames using `sse.Reader` (handling fragmented lines, multi-events per read, leading whitespace, and comments).
  - Parses SSE JSON payloads into typed `semantic.Segment` instances and enqueues them via `PendingBuffer.Append()`.
  - While downstream writes or flushes are in progress, the reader continues ingesting and aggregating arriving deltas concurrently.

- **Downstream Writer**:
  - Operates on a downstream-completion-driven loop: waits for data in `PendingBuffer`.
  - Atomically takes the current accumulated snapshot via `PendingBuffer.Swap()` and resets pending memory to empty.
  - Serializes snapshot segments into OpenAI-compatible SSE events and flushes them to the client.
  - Immediately begins the next swap cycle once the current write completes.
  - **Adaptive Coalescing**: If the client or network is slow, more tokens accumulate during the write window, automatically enlarging the next batch; if the client is fast, data is flushed almost immediately with minimal latency.

---

## 3. Bounded Memory Buffer & Backpressure

To prevent unbounded memory growth when handling slow or stalled clients, high and low watermarks provide backpressure:

- `high_watermark` (default: 32MB):
  - When `currentBytes >= high_watermark`, the upstream reader suspends ingestion (`sync.Cond.Wait`), pausing reads from the upstream socket.
  - TCP window saturation propagates backpressure up to the provider.
- `low_watermark` (default: 24MB):
  - Once the downstream writer consumes and swaps buffered data below this threshold, a broadcast signal (`sync.Cond.Broadcast`) wakes the reader to resume fast ingestion.
  - Context cancellation during high-watermark wait immediately unblocks and cleans up resources.

---

## 4. Semantic Segmentation & Coalescing Rules

### 4.1 Segment Categories
1. `REASONING_DELTA`: Chain-of-thought tokens (supports `reasoning_content`, `reasoning`, `reasoning_text`, `thought`, as well as Responses API `response.reasoning_text.delta` and `response.reasoning_summary_text.delta`).
2. `CONTENT_DELTA`: Message body text deltas (`delta.content` in Chat Completions, `response.output_text.delta` and `response.refusal.delta` in Responses API).
3. `TOOL_CALL_DELTA`: Tool invocations (`tool_calls` in Chat Completions, `response.function_call_arguments.delta`, `response.custom_tool_call_input.delta`, `response.mcp_call_arguments.delta`, etc. in Responses API).
4. `ROLE`: Initial assistant role declaration.
5. `FINISH`: Termination state (`finish_reason`).
6. `USAGE`: Token usage metrics (`usage` in Chat Completions, `usage.output_tokens` in Responses API `response.completed`).
7. `CONTROL / LIFECYCLE`: Responses API boundary events (`response.created`, `response.in_progress`, `.added`, `.done`, `response.completed`).
8. `ERROR`: Error payloads (`error`, `response.failed`).
9. `DONE`: `[DONE]` termination stream indicator.
10. `UNKNOWN`: Unrecognized or provider-specific custom extension events.

### 4.2 Merging Rules & Barrier Guarantees
- **Role Idempotence & Non-Barrier**:
  - The first encountered `role` (e.g. `"role": "assistant"`) is emitted.
  - Subsequent duplicate roles in subsequent chunks for the same choice are ignored and **never create a barrier**, ensuring seamless content/reasoning aggregation.
- **Homogeneous Merging**:
  - Adjacent `ReasoningSegment` items (same choice index and field name) append strings. Adjacent Responses API reasoning deltas (same item ID and output/summary index) append strings and advance `sequence_number`.
  - Adjacent `ContentSegment` items (same choice index) append strings. Adjacent Responses API `response.output_text.delta` items (same item ID, output index, and content index) append strings, advance `sequence_number`, and concatenate logprobs when present.
  - Adjacent `ToolCallSegment` items concatenate `arguments` byte-for-byte. Adjacent Responses API tool argument deltas concatenate arguments byte-for-byte and advance `sequence_number`. Partial JSON is never parsed or re-serialized mid-stream to avoid modifying whitespace, escaping, or partial tokens.
- **Heterogeneous Barriers**:
  - `Reasoning -> Content` forms a barrier (never mixed into a single delta).
  - `Content -> Tool Call` forms a barrier, strictly preserving causal sequence.
  - `Tool Call -> Content` forms a barrier.
  - Lifecycle boundaries (`response.output_item.added/done`, `response.content_part.added/done`, `response.output_text.done`), `Finish`, `Usage`, `[DONE]`, and `Error` act as strict barriers, ensuring all preceding accumulated data is emitted first.
  - Unrecognized events (`UNKNOWN`) pass through safely in their original sequence.
