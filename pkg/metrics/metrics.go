package metrics

import (
	"fmt"
	"sync/atomic"
	"time"
)

type StreamMetrics struct {
	UpstreamSSEEvents       int64
	DownstreamSSEEvents     int64
	ReasoningFragmentsIn    int64
	ReasoningEventsOut      int64
	ContentFragmentsIn      int64
	ContentEventsOut        int64
	ToolArgumentFragmentsIn int64
	ToolEventsOut           int64
	UpstreamBytes           int64
	DownstreamBytes         int64
	PendingBytesCurrent     int64
	PendingBytesMax         int64
	ReaderPauseCount        int64
	ReaderPauseDurationNs   int64
	DownstreamWriteNs       int64
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

func (m *StreamMetrics) Summary() string {
	return fmt.Sprintf(
		"SSE Events: [Upstream: %d, Downstream: %d, Ratio: %.2f:1], "+
			"Reasoning: [In: %d, Out: %d, Ratio: %.2f:1], "+
			"Content: [In: %d, Out: %d, Ratio: %.2f:1], "+
			"Tool: [In: %d, Out: %d, Ratio: %.2f:1], "+
			"Bytes: [Upstream: %d, Downstream: %d], "+
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
		atomic.LoadInt64(&m.PendingBytesMax),
		atomic.LoadInt64(&m.ReaderPauseCount),
		time.Duration(atomic.LoadInt64(&m.ReaderPauseDurationNs)),
		time.Duration(atomic.LoadInt64(&m.DownstreamWriteNs)),
	)
}
