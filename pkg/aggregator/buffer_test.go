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
