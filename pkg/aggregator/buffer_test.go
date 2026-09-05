package aggregator

import (
	"context"
	"sync"
	"testing"
	"time"

	"buffered-proxy/pkg/metrics"
	"buffered-proxy/pkg/semantic"
)

func TestPendingBufferAppendAndSwap(t *testing.T) {
	m := &metrics.StreamMetrics{}
	pb := NewPendingBuffer(BufferConfig{HighWatermark: 1024 * 1024, LowWatermark: 512 * 1024}, m)

	ctx := context.Background()
	_ = pb.Append(ctx, &semantic.ContentSegment{ChoiceIndex: 0, Text: "a"})
	_ = pb.Append(ctx, &semantic.ContentSegment{ChoiceIndex: 0, Text: "b"})
	_ = pb.Append(ctx, &semantic.ContentSegment{ChoiceIndex: 0, Text: "c"})

	if pb.Len() != 1 {
		t.Fatalf("expected 1 coalesced segment, got %d", pb.Len())
	}

	snap, _, err := pb.Swap(ctx)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(snap) != 1 {
		t.Fatalf("expected 1 segment in snapshot, got %d", len(snap))
	}
	cs := snap[0].(*semantic.ContentSegment)
	if cs.Text != "abc" {
		t.Fatalf("expected text 'abc', got %q", cs.Text)
	}

	if pb.CurrentBytes() != 0 {
		t.Fatalf("expected 0 bytes after swap, got %d", pb.CurrentBytes())
	}
}

func TestPendingBufferHighWatermarkBackpressure(t *testing.T) {
	m := &metrics.StreamMetrics{}
	cfg := BufferConfig{
		HighWatermark: 200,
		LowWatermark:  100,
	}
	pb := NewPendingBuffer(cfg, m)
	ctx := context.Background()

	_ = pb.Append(ctx, &semantic.RoleSegment{ChoiceIndex: 0, Role: "assistant"})

	var wg sync.WaitGroup
	wg.Add(1)
	appendDone := make(chan struct{})

	go func() {
		defer wg.Done()
		_ = pb.Append(ctx, &semantic.ReasoningSegment{ChoiceIndex: 0, FieldName: "reasoning", Text: string(make([]byte, 250))})
		close(appendDone)
	}()

	select {
	case <-appendDone:
	case <-time.After(100 * time.Millisecond):
	}

	wg.Add(1)
	secondAppendBlocked := make(chan struct{})
	go func() {
		defer wg.Done()
		_ = pb.Append(ctx, &semantic.ContentSegment{ChoiceIndex: 0, Text: string(make([]byte, 100))})
		close(secondAppendBlocked)
	}()

	time.Sleep(50 * time.Millisecond)
	select {
	case <-secondAppendBlocked:
		t.Fatalf("expected second append to block on high watermark")
	default:
	}

	snap, _, _ := pb.Swap(ctx)
	if len(snap) == 0 {
		t.Fatalf("expected snapshot items")
	}

	select {
	case <-secondAppendBlocked:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("expected second append to unblock after swap")
	}

	wg.Wait()
	if m.ReaderPauseCount == 0 {
		t.Fatalf("expected reader pause count > 0, got %d", m.ReaderPauseCount)
	}
}

func TestPendingBufferHighWatermarkContextCancel(t *testing.T) {
	m := &metrics.StreamMetrics{}
	cfg := BufferConfig{
		HighWatermark: 100,
		LowWatermark:  50,
	}
	pb := NewPendingBuffer(cfg, m)

	cancelCtx, cancel := context.WithCancel(context.Background())
	_ = pb.Append(cancelCtx, &semantic.ContentSegment{ChoiceIndex: 0, Text: string(make([]byte, 120))})

	errCh := make(chan error, 1)
	go func() {
		err := pb.Append(cancelCtx, &semantic.ContentSegment{ChoiceIndex: 0, Text: string(make([]byte, 50))})
		errCh <- err
	}()

	time.Sleep(30 * time.Millisecond)
	cancel()
	pb.Close(cancelCtx.Err())

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatalf("expected error on cancelled context append, got nil")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("Append remained hung after context cancellation")
	}
}

func TestPendingBufferMinCoalesceWaitBarrierBypass(t *testing.T) {
	pb := NewPendingBuffer(BufferConfig{
		HighWatermark:   1024 * 1024,
		LowWatermark:    512 * 1024,
		MinCoalesceWait: 500 * time.Millisecond,
	}, nil)

	ctx := context.Background()
	// Append reasoning segment
	_ = pb.Append(ctx, &semantic.ReasoningSegment{ChoiceIndex: 0, FieldName: "reasoning", Text: "thinking..."})
	// Append content segment - this creates a barrier transition
	_ = pb.Append(ctx, &semantic.ContentSegment{ChoiceIndex: 0, Text: "Hello"})

	start := time.Now()
	snap, _, err := pb.Swap(ctx)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(snap) != 2 {
		t.Fatalf("expected 2 segments in snapshot, got %d", len(snap))
	}
	if elapsed >= 300*time.Millisecond {
		t.Fatalf("expected swap to bypass MinCoalesceWait due to barrier, took %v", elapsed)
	}
}

func TestPendingBufferMinCoalesceWaitInterruptedByBarrier(t *testing.T) {
	pb := NewPendingBuffer(BufferConfig{
		HighWatermark:   1024 * 1024,
		LowWatermark:    512 * 1024,
		MinCoalesceWait: 500 * time.Millisecond,
	}, nil)

	ctx := context.Background()
	_ = pb.Append(ctx, &semantic.ReasoningSegment{ChoiceIndex: 0, FieldName: "reasoning", Text: "thinking..."})

	go func() {
		time.Sleep(50 * time.Millisecond)
		// Arrival of content creates a barrier transition while Swap is waiting
		_ = pb.Append(ctx, &semantic.ContentSegment{ChoiceIndex: 0, Text: "Hello"})
	}()

	start := time.Now()
	snap, _, err := pb.Swap(ctx)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(snap) != 2 {
		t.Fatalf("expected 2 segments in snapshot, got %d", len(snap))
	}
	if elapsed >= 300*time.Millisecond {
		t.Fatalf("expected swap to be interrupted quickly, took %v", elapsed)
	}
}

func TestPendingBufferNonCoalesceableSegmentBypassesWait(t *testing.T) {
	nonCoalesceableSegs := []semantic.Segment{
		&semantic.RoleSegment{ChoiceIndex: 0, Role: "assistant"},
		&semantic.FinishSegment{ChoiceIndex: 0, FinishReason: "stop"},
		&semantic.UsageSegment{Usage: map[string]int{"total_tokens": 10}},
		&semantic.RawSegment{SegmentType: semantic.EventDone, Data: []byte("[DONE]")},
	}

	for _, seg := range nonCoalesceableSegs {
		pb := NewPendingBuffer(BufferConfig{
			HighWatermark:   1024 * 1024,
			LowWatermark:    512 * 1024,
			MinCoalesceWait: 500 * time.Millisecond,
		}, nil)

		ctx := context.Background()
		_ = pb.Append(ctx, seg)

		start := time.Now()
		snap, _, err := pb.Swap(ctx)
		elapsed := time.Since(start)

		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if len(snap) != 1 {
			t.Fatalf("expected 1 segment in snapshot, got %d", len(snap))
		}
		if elapsed >= 200*time.Millisecond {
			t.Fatalf("segment type %v should bypass min coalesce wait, took %v", seg.Type(), elapsed)
		}
	}
}
