package aggregator

import (
	"context"
	"errors"
	"sync"
	"time"

	"buffered-proxy/pkg/metrics"
	"buffered-proxy/pkg/semantic"
)

var (
	ErrBufferClosed = errors.New("buffer closed")
	ErrContextDone  = errors.New("context done")
)

type BufferConfig struct {
	HighWatermark   int64
	LowWatermark    int64
	MinCoalesceWait time.Duration
}

func DefaultBufferConfig() BufferConfig {
	return BufferConfig{
		HighWatermark:   32 * 1024 * 1024,
		LowWatermark:    24 * 1024 * 1024,
		MinCoalesceWait: 0,
	}
}

type PendingBuffer struct {
	mu              sync.Mutex
	notEmpty        *sync.Cond
	belowHighWater  *sync.Cond
	segments        []semantic.Segment
	currentBytes    int64
	highWatermark   int64
	lowWatermark    int64
	minCoalesceWait time.Duration
	closed          bool
	upstreamErr     error
	metrics         *metrics.StreamMetrics
}

func NewPendingBuffer(cfg BufferConfig, m *metrics.StreamMetrics) *PendingBuffer {
	if cfg.HighWatermark <= 0 {
		cfg.HighWatermark = 32 * 1024 * 1024
	}
	if cfg.LowWatermark <= 0 || cfg.LowWatermark >= cfg.HighWatermark {
		cfg.LowWatermark = cfg.HighWatermark * 3 / 4
	}
	pb := &PendingBuffer{
		highWatermark:   cfg.HighWatermark,
		lowWatermark:    cfg.LowWatermark,
		minCoalesceWait: cfg.MinCoalesceWait,
		metrics:         m,
	}
	pb.notEmpty = sync.NewCond(&pb.mu)
	pb.belowHighWater = sync.NewCond(&pb.mu)
	return pb
}

func (b *PendingBuffer) Append(ctx context.Context, seg semantic.Segment) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return ErrBufferClosed
	}

	if b.currentBytes >= b.highWatermark {
		pauseStart := time.Now()
		for b.currentBytes >= b.lowWatermark && !b.closed && ctx.Err() == nil {
			b.belowHighWater.Wait()
		}
		if b.metrics != nil {
			b.metrics.AddReaderPause(time.Since(pauseStart))
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if b.closed {
			return ErrBufferClosed
		}
	}

	appendedBytes := int64(seg.BytesLen())
	if len(b.segments) > 0 {
		last := b.segments[len(b.segments)-1]
		if last.CanMerge(seg) {
			oldBytes := int64(last.BytesLen())
			last.Merge(seg)
			newBytes := int64(last.BytesLen())
			diff := newBytes - oldBytes
			if diff > 0 {
				b.currentBytes += diff
			}
			if b.metrics != nil {
				b.metrics.SetPendingBytes(b.currentBytes)
			}
			b.notEmpty.Signal()
			return nil
		}
	}

	b.segments = append(b.segments, seg)
	b.currentBytes += appendedBytes
	if b.metrics != nil {
		b.metrics.SetPendingBytes(b.currentBytes)
	}
	b.notEmpty.Signal()
	return nil
}

func (b *PendingBuffer) Swap(ctx context.Context) ([]semantic.Segment, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for len(b.segments) == 0 && !b.closed && b.upstreamErr == nil && ctx.Err() == nil {
		b.notEmpty.Wait()
	}

	if ctx.Err() != nil {
		return nil, b.closed, ctx.Err()
	}

	if len(b.segments) == 0 {
		return nil, b.closed, b.upstreamErr
	}

	if b.minCoalesceWait > 0 {
		b.mu.Unlock()
		time.Sleep(b.minCoalesceWait)
		b.mu.Lock()
	}

	snapshot := b.segments
	b.segments = nil
	b.currentBytes = 0

	if b.metrics != nil {
		b.metrics.SetPendingBytes(0)
	}
	b.belowHighWater.Broadcast()

	return snapshot, b.closed, b.upstreamErr
}

func (b *PendingBuffer) Close(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return
	}
	b.closed = true
	b.upstreamErr = err
	b.notEmpty.Broadcast()
	b.belowHighWater.Broadcast()
}

func (b *PendingBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.segments)
}

func (b *PendingBuffer) CurrentBytes() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.currentBytes
}
