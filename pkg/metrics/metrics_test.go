package metrics

import (
	"sync"
	"testing"
	"time"
)

func TestRecordModelMetrics(t *testing.T) {
	m := &StreamMetrics{}

	m.RecordModelMetrics("gpt-4o", 100, 200*time.Millisecond, 2*time.Second)
	m.RecordModelMetrics("gpt-4o", 200, 400*time.Millisecond, 4*time.Second)

	snaps := m.ModelSnapshots()
	gpt4o, ok := snaps["gpt-4o"]
	if !ok {
		t.Fatalf("expected gpt-4o in snapshots")
	}

	if gpt4o.Requests != 2 {
		t.Errorf("expected 2 requests, got %d", gpt4o.Requests)
	}
	if gpt4o.TotalTokens != 300 {
		t.Errorf("expected 300 tokens, got %d", gpt4o.TotalTokens)
	}
	// Avg TTFT = (200ms + 400ms)/2 = 300ms
	if gpt4o.AvgTTFTMs != 300.0 {
		t.Errorf("expected avg TTFT 300ms, got %f", gpt4o.AvgTTFTMs)
	}
	if gpt4o.LastTTFTMs != 400.0 {
		t.Errorf("expected last TTFT 400ms, got %f", gpt4o.LastTTFTMs)
	}
	// Total gen duration = 6s, tokens = 300 -> TPS = 50.0
	if gpt4o.TPS != 50.0 {
		t.Errorf("expected TPS 50.0, got %f", gpt4o.TPS)
	}
}

func TestRecordModelMetricsConcurrent(t *testing.T) {
	m := &StreamMetrics{}
	var wg sync.WaitGroup

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			model := "model-a"
			if idx%2 == 1 {
				model = "model-b"
			}
			m.RecordModelMetrics(model, 10, 100*time.Millisecond, 1*time.Second)
		}(i)
	}
	wg.Wait()

	snaps := m.ModelSnapshots()
	if snaps["model-a"].Requests != 10 {
		t.Errorf("expected 10 requests for model-a, got %d", snaps["model-a"].Requests)
	}
	if snaps["model-b"].Requests != 10 {
		t.Errorf("expected 10 requests for model-b, got %d", snaps["model-b"].Requests)
	}
}
