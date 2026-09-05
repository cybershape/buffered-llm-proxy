package metrics

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type ModelMetricSnapshot struct {
	Requests                  int64   `json:"requests"`
	TotalTokens               int64   `json:"total_tokens"`
	AvgTTFTMs                 float64 `json:"avg_ttft_ms"`
	LastTTFTMs                float64 `json:"last_ttft_ms"`
	TotalGenerationDurationMs float64 `json:"total_generation_duration_ms"`
	TotalClientDurationMs     float64 `json:"total_client_duration_ms"`
	TPS                       float64 `json:"tps"`
	ClientTPS                 float64 `json:"client_tps"`
}

type modelAccumulator struct {
	mu                        sync.Mutex
	requests                  int64
	totalTokens               int64
	decodeTokens              int64
	totalTTFTNs               int64
	lastTTFTNs                int64
	totalGenerationDurationNs int64
	totalClientDurationNs     int64
}

type StreamMetrics struct {
	UpstreamSSEEvents            int64
	DownstreamSSEEvents          int64
	ReasoningFragmentsIn         int64
	ReasoningEventsOut           int64
	ContentFragmentsIn           int64
	ContentEventsOut             int64
	ToolArgumentFragmentsIn      int64
	ToolEventsOut                int64
	UpstreamBytes                int64
	DownstreamBytes              int64
	CompressionUncompressedBytes int64
	CompressionCompressedBytes   int64
	PendingBytesCurrent          int64
	PendingBytesMax              int64
	ReaderPauseCount             int64
	ReaderPauseDurationNs        int64
	DownstreamWriteNs            int64

	modelsMu sync.RWMutex
	models   map[string]*modelAccumulator
}

func (m *StreamMetrics) IncUpstreamEvents() {
	atomic.AddInt64(&m.UpstreamSSEEvents, 1)
}

func (m *StreamMetrics) IncDownstreamEvents() {
	atomic.AddInt64(&m.DownstreamSSEEvents, 1)
}

func (m *StreamMetrics) IncReasoningFragmentsIn() {
	atomic.AddInt64(&m.ReasoningFragmentsIn, 1)
}

func (m *StreamMetrics) IncReasoningEventsOut() {
	atomic.AddInt64(&m.ReasoningEventsOut, 1)
}

func (m *StreamMetrics) IncContentFragmentsIn() {
	atomic.AddInt64(&m.ContentFragmentsIn, 1)
}

func (m *StreamMetrics) IncContentEventsOut() {
	atomic.AddInt64(&m.ContentEventsOut, 1)
}

func (m *StreamMetrics) IncToolArgumentFragmentsIn() {
	atomic.AddInt64(&m.ToolArgumentFragmentsIn, 1)
}

func (m *StreamMetrics) IncToolEventsOut() {
	atomic.AddInt64(&m.ToolEventsOut, 1)
}

func (m *StreamMetrics) AddUpstreamBytes(n int64) {
	atomic.AddInt64(&m.UpstreamBytes, n)
}

func (m *StreamMetrics) AddDownstreamBytes(n int64) {
	atomic.AddInt64(&m.DownstreamBytes, n)
}

func (m *StreamMetrics) SetPendingBytes(n int64) {
	atomic.StoreInt64(&m.PendingBytesCurrent, n)
	for {
		curMax := atomic.LoadInt64(&m.PendingBytesMax)
		if n <= curMax {
			break
		}
		if atomic.CompareAndSwapInt64(&m.PendingBytesMax, curMax, n) {
			break
		}
	}
}

func (m *StreamMetrics) AddReaderPause(d time.Duration) {
	atomic.AddInt64(&m.ReaderPauseCount, 1)
	atomic.AddInt64(&m.ReaderPauseDurationNs, d.Nanoseconds())
}

func (m *StreamMetrics) AddDownstreamWrite(d time.Duration) {
	atomic.AddInt64(&m.DownstreamWriteNs, d.Nanoseconds())
}

func (m *StreamMetrics) OverallCoalescingRatio() float64 {
	in := atomic.LoadInt64(&m.UpstreamSSEEvents)
	out := atomic.LoadInt64(&m.DownstreamSSEEvents)
	if out == 0 {
		return float64(in)
	}
	return float64(in) / float64(out)
}

func (m *StreamMetrics) ReasoningCoalescingRatio() float64 {
	in := atomic.LoadInt64(&m.ReasoningFragmentsIn)
	out := atomic.LoadInt64(&m.ReasoningEventsOut)
	if out == 0 {
		return float64(in)
	}
	return float64(in) / float64(out)
}

func (m *StreamMetrics) ContentCoalescingRatio() float64 {
	in := atomic.LoadInt64(&m.ContentFragmentsIn)
	out := atomic.LoadInt64(&m.ContentEventsOut)
	if out == 0 {
		return float64(in)
	}
	return float64(in) / float64(out)
}

func (m *StreamMetrics) ToolCoalescingRatio() float64 {
	in := atomic.LoadInt64(&m.ToolArgumentFragmentsIn)
	out := atomic.LoadInt64(&m.ToolEventsOut)
	if out == 0 {
		return float64(in)
	}
	return float64(in) / float64(out)
}

func (m *StreamMetrics) AddCompressionBytes(uncompressed, compressed int64) {
	atomic.AddInt64(&m.CompressionUncompressedBytes, uncompressed)
	atomic.AddInt64(&m.CompressionCompressedBytes, compressed)
}

func (m *StreamMetrics) CompressionRatio() float64 {
	in := atomic.LoadInt64(&m.CompressionUncompressedBytes)
	out := atomic.LoadInt64(&m.CompressionCompressedBytes)
	if out == 0 {
		return 0
	}
	return float64(in) / float64(out)
}

func (m *StreamMetrics) CompressionSavingsRatio() float64 {
	in := atomic.LoadInt64(&m.CompressionUncompressedBytes)
	out := atomic.LoadInt64(&m.CompressionCompressedBytes)
	if in == 0 {
		return 0
	}
	return float64(in-out) / float64(in)
}

func (m *StreamMetrics) Summary() string {
	return fmt.Sprintf(
		"SSE Events: [Upstream: %d, Downstream: %d, Ratio: %.2f:1], "+
			"Reasoning: [In: %d, Out: %d, Ratio: %.2f:1], "+
			"Content: [In: %d, Out: %d, Ratio: %.2f:1], "+
			"Tool: [In: %d, Out: %d, Ratio: %.2f:1], "+
			"Bytes: [Upstream: %d, Downstream: %d], "+
			"Compression: [Uncompressed: %d, Compressed: %d, Ratio: %.2f:1, Savings: %.1f%%], "+
			"Buffer: [Max: %d bytes], "+
			"Reader Pauses: [Count: %d, Total: %v], "+
			"Write Duration Total: %v",
		atomic.LoadInt64(&m.UpstreamSSEEvents),
		atomic.LoadInt64(&m.DownstreamSSEEvents),
		m.OverallCoalescingRatio(),
		atomic.LoadInt64(&m.ReasoningFragmentsIn),
		atomic.LoadInt64(&m.ReasoningEventsOut),
		m.ReasoningCoalescingRatio(),
		atomic.LoadInt64(&m.ContentFragmentsIn),
		atomic.LoadInt64(&m.ContentEventsOut),
		m.ContentCoalescingRatio(),
		atomic.LoadInt64(&m.ToolArgumentFragmentsIn),
		atomic.LoadInt64(&m.ToolEventsOut),
		m.ToolCoalescingRatio(),
		atomic.LoadInt64(&m.UpstreamBytes),
		atomic.LoadInt64(&m.DownstreamBytes),
		atomic.LoadInt64(&m.CompressionUncompressedBytes),
		atomic.LoadInt64(&m.CompressionCompressedBytes),
		m.CompressionRatio(),
		m.CompressionSavingsRatio()*100,
		atomic.LoadInt64(&m.PendingBytesMax),
		atomic.LoadInt64(&m.ReaderPauseCount),
		time.Duration(atomic.LoadInt64(&m.ReaderPauseDurationNs)),
		time.Duration(atomic.LoadInt64(&m.DownstreamWriteNs)),
	)
}

func (m *StreamMetrics) RecordModelMetrics(model string, tokens int64, ttft time.Duration, upstreamGenDuration time.Duration, clientDurations ...time.Duration) {
	if model == "" {
		model = "unknown"
	}
	clientGenDuration := upstreamGenDuration
	if len(clientDurations) > 0 {
		clientGenDuration = clientDurations[0]
	}

	m.modelsMu.Lock()
	if m.models == nil {
		m.models = make(map[string]*modelAccumulator)
	}
	acc, ok := m.models[model]
	if !ok {
		acc = &modelAccumulator{}
		m.models[model] = acc
	}
	m.modelsMu.Unlock()

	acc.mu.Lock()
	defer acc.mu.Unlock()
	acc.requests++
	acc.totalTokens += tokens
	if tokens > 1 {
		acc.decodeTokens += (tokens - 1)
	} else {
		acc.decodeTokens += tokens
	}
	acc.totalTTFTNs += ttft.Nanoseconds()
	acc.lastTTFTNs = ttft.Nanoseconds()
	acc.totalGenerationDurationNs += upstreamGenDuration.Nanoseconds()
	acc.totalClientDurationNs += clientGenDuration.Nanoseconds()
}

func (m *StreamMetrics) ModelSnapshots() map[string]ModelMetricSnapshot {
	m.modelsMu.RLock()
	defer m.modelsMu.RUnlock()

	if len(m.models) == 0 {
		return make(map[string]ModelMetricSnapshot)
	}

	result := make(map[string]ModelMetricSnapshot, len(m.models))
	for name, acc := range m.models {
		acc.mu.Lock()
		reqs := acc.requests
		tokens := acc.totalTokens
		decTokens := acc.decodeTokens
		ttftNs := acc.totalTTFTNs
		lastTtftNs := acc.lastTTFTNs
		genNs := acc.totalGenerationDurationNs
		clientNs := acc.totalClientDurationNs
		acc.mu.Unlock()

		snap := ModelMetricSnapshot{
			Requests:                  reqs,
			TotalTokens:               tokens,
			LastTTFTMs:                float64(lastTtftNs) / 1e6,
			TotalGenerationDurationMs: float64(genNs) / 1e6,
			TotalClientDurationMs:     float64(clientNs) / 1e6,
		}
		if reqs > 0 {
			snap.AvgTTFTMs = float64(ttftNs) / float64(reqs) / 1e6
		}

		// 上游模型推理 Decode TPS：使用生成阶段生成的 decodeTokens 与 upstream 耗时
		if genNs > 0 {
			if decTokens > 0 {
				snap.TPS = float64(decTokens) / (float64(genNs) / 1e9)
			} else {
				snap.TPS = float64(tokens) / (float64(genNs) / 1e9)
			}
		} else if reqs > 0 && ttftNs > 0 {
			snap.TPS = float64(tokens) / (float64(ttftNs) / 1e9)
		}

		// 客户端实际感知输出 TPS
		if clientNs > 0 {
			snap.ClientTPS = float64(tokens) / (float64(clientNs) / 1e9)
		} else if snap.TPS > 0 {
			snap.ClientTPS = snap.TPS
		}

		result[name] = snap
	}
	return result
}
