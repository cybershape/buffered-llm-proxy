package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"buffered-proxy/pkg/aggregator"
	"buffered-proxy/pkg/metrics"
	"buffered-proxy/pkg/sse"
)

type slowDownstreamWriter struct {
	buf   bytes.Buffer
	mu    sync.Mutex
	delay time.Duration
}

func (w *slowDownstreamWriter) Write(p []byte) (n int, err error) {
	if w.delay > 0 {
		time.Sleep(w.delay)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *slowDownstreamWriter) Flush() {}

func (w *slowDownstreamWriter) Bytes() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	cp := make([]byte, w.buf.Len())
	copy(cp, w.buf.Bytes())
	return cp
}

func TestPipelineWithFixtures(t *testing.T) {
	fixtureFiles := []string{
		"../fixtures/chat_content.sse",
		"../fixtures/chat_reasoning.sse",
		"../fixtures/chat_tool_call.sse",
		"../fixtures/chat_reasoning_content_tool.sse",
	}

	for _, fixturePath := range fixtureFiles {
		t.Run(fixturePath, func(t *testing.T) {
			data, err := os.ReadFile(fixturePath)
			if err != nil {
				t.Fatalf("failed to read fixture: %v", err)
			}

			pipeIn := io.NopCloser(bytes.NewReader(data))
			outBuf := &bytes.Buffer{}
			m := &metrics.StreamMetrics{}

			pipeline := aggregator.NewStreamPipeline(aggregator.DefaultBufferConfig(), m)
			if err := pipeline.ProcessStream(context.Background(), pipeIn, outBuf); err != nil {
				t.Fatalf("pipeline failed: %v", err)
			}

			if m.UpstreamSSEEvents == 0 {
				t.Fatalf("expected upstream events > 0")
			}
			if m.DownstreamSSEEvents == 0 {
				t.Fatalf("expected downstream events > 0")
			}

			r := sse.NewReader(bytes.NewReader(outBuf.Bytes()))
			hasDone := false
			for {
				ev, err := r.ReadEvent()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("failed to read out event: %v", err)
				}
				if string(ev.Data) == "[DONE]" {
					hasDone = true
				}
			}
			if !hasDone {
				t.Fatalf("missing [DONE] in downstream")
			}
		})
	}
}

func Test100ContentDeltasAggregation(t *testing.T) {
	var inSSE bytes.Buffer
	var expectedText strings.Builder

	inSSE.WriteString("data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"}}]}\n\n")

	for i := 1; i <= 100; i++ {
		frag := fmt.Sprintf("word%d ", i)
		expectedText.WriteString(frag)
		inSSE.WriteString(fmt.Sprintf("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":%q}}]}\n\n", frag))
	}
	inSSE.WriteString("data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n")
	inSSE.WriteString("data: [DONE]\n\n")

	slowWriter := &slowDownstreamWriter{delay: 2 * time.Millisecond}
	m := &metrics.StreamMetrics{}
	cfg := aggregator.DefaultBufferConfig()
	pipeline := aggregator.NewStreamPipeline(cfg, m)

	err := pipeline.ProcessStream(context.Background(), io.NopCloser(&inSSE), slowWriter)
	if err != nil {
		t.Fatalf("stream err: %v", err)
	}

	r := sse.NewReader(bytes.NewReader(slowWriter.Bytes()))
	var gatheredText strings.Builder
	contentEventsCount := 0

	for {
		ev, err := r.ReadEvent()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if string(ev.Data) == "[DONE]" {
			continue
		}
		var chunk map[string]interface{}
		if json.Unmarshal(ev.Data, &chunk) == nil {
			if choices, ok := chunk["choices"].([]interface{}); ok && len(choices) > 0 {
				first := choices[0].(map[string]interface{})
				if delta, ok := first["delta"].(map[string]interface{}); ok {
					if c, ok := delta["content"].(string); ok && len(c) > 0 {
						gatheredText.WriteString(c)
						contentEventsCount++
					}
				}
			}
		}
	}

	if gatheredText.String() != expectedText.String() {
		t.Fatalf("content mismatch, expected len %d, got len %d", len(expectedText.String()), len(gatheredText.String()))
	}

	if contentEventsCount >= 100 {
		t.Fatalf("expected content events count to be coalesced (< 100), got %d", contentEventsCount)
	}
}

func Test100ReasoningDeltasAggregation(t *testing.T) {
	var inSSE bytes.Buffer
	var expectedReasoning strings.Builder

	inSSE.WriteString("data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"}}]}\n\n")

	for i := 1; i <= 100; i++ {
		frag := fmt.Sprintf("thought%d ", i)
		expectedReasoning.WriteString(frag)
		inSSE.WriteString(fmt.Sprintf("data: {\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":%q}}]}\n\n", frag))
	}
	inSSE.WriteString("data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n")
	inSSE.WriteString("data: [DONE]\n\n")

	slowWriter := &slowDownstreamWriter{delay: 2 * time.Millisecond}
	m := &metrics.StreamMetrics{}
	pipeline := aggregator.NewStreamPipeline(aggregator.DefaultBufferConfig(), m)

	err := pipeline.ProcessStream(context.Background(), io.NopCloser(&inSSE), slowWriter)
	if err != nil {
		t.Fatalf("stream err: %v", err)
	}

	r := sse.NewReader(bytes.NewReader(slowWriter.Bytes()))
	var gatheredReasoning strings.Builder
	reasoningEventsCount := 0

	for {
		ev, err := r.ReadEvent()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if string(ev.Data) == "[DONE]" {
			continue
		}
		var chunk map[string]interface{}
		if json.Unmarshal(ev.Data, &chunk) == nil {
			if choices, ok := chunk["choices"].([]interface{}); ok && len(choices) > 0 {
				first := choices[0].(map[string]interface{})
				if delta, ok := first["delta"].(map[string]interface{}); ok {
					if rc, ok := delta["reasoning_content"].(string); ok && len(rc) > 0 {
						gatheredReasoning.WriteString(rc)
						reasoningEventsCount++
					}
					if _, ok := delta["reasoning"]; ok {
						t.Fatalf("reasoning field name should not be mutated")
					}
				}
			}
		}
	}

	if gatheredReasoning.String() != expectedReasoning.String() {
		t.Fatalf("reasoning mismatch")
	}
	if reasoningEventsCount >= 100 {
		t.Fatalf("expected reasoning coalesced (< 100), got %d", reasoningEventsCount)
	}
}

func Test100ToolArgumentFragmentsExactByteConcat(t *testing.T) {
	rawFragments := []string{
		"{", "\"", "k", "e", "y", "\"", ":", " ", "\"", "v", "a", "l", "u", "e", "\"", ",",
		"\"", "n", "u", "m", "\"", ":", "1", "2", "3", ",",
		"\"", "a", "r", "r", "\"", ":", "[", "1", ",", "2", ",", "3", "]", "}"}

	for len(rawFragments) < 100 {
		rawFragments = append(rawFragments, " ")
	}

	var expectedArgs strings.Builder
	var inSSE bytes.Buffer
	inSSE.WriteString("data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"}}]}\n\n")
	inSSE.WriteString("data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_abc\",\"type\":\"function\",\"function\":{\"name\":\"do_something\",\"arguments\":\"\"}}]}}]}\n\n")

	for _, frag := range rawFragments {
		expectedArgs.WriteString(frag)
		inSSE.WriteString(fmt.Sprintf("data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":%q}}]}}]}\n\n", frag))
	}
	inSSE.WriteString("data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n")
	inSSE.WriteString("data: [DONE]\n\n")

	slowWriter := &slowDownstreamWriter{delay: 1 * time.Millisecond}
	m := &metrics.StreamMetrics{}
	pipeline := aggregator.NewStreamPipeline(aggregator.DefaultBufferConfig(), m)

	err := pipeline.ProcessStream(context.Background(), io.NopCloser(&inSSE), slowWriter)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	r := sse.NewReader(bytes.NewReader(slowWriter.Bytes()))
	var gatheredArgs strings.Builder
	var foundID string
	var foundName string

	for {
		ev, err := r.ReadEvent()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if string(ev.Data) == "[DONE]" {
			continue
		}
		var chunk map[string]interface{}
		if json.Unmarshal(ev.Data, &chunk) == nil {
			if choices, ok := chunk["choices"].([]interface{}); ok && len(choices) > 0 {
				first := choices[0].(map[string]interface{})
				if delta, ok := first["delta"].(map[string]interface{}); ok {
					if tcs, ok := delta["tool_calls"].([]interface{}); ok {
						for _, rawTc := range tcs {
							tc := rawTc.(map[string]interface{})
							if id, ok := tc["id"].(string); ok && id != "" {
								foundID = id
							}
							if fn, ok := tc["function"].(map[string]interface{}); ok {
								if n, ok := fn["name"].(string); ok && n != "" {
									foundName = n
								}
								if a, ok := fn["arguments"].(string); ok {
									gatheredArgs.WriteString(a)
								}
							}
						}
					}
				}
			}
		}
	}

	if foundID != "call_abc" {
		t.Fatalf("expected id call_abc, got %q", foundID)
	}
	if foundName != "do_something" {
		t.Fatalf("expected name do_something, got %q", foundName)
	}
	if gatheredArgs.String() != expectedArgs.String() {
		t.Fatalf("arguments byte mismatch: expected %q, got %q", expectedArgs.String(), gatheredArgs.String())
	}
}

func TestMultiToolCallIsolation(t *testing.T) {
	var inSSE bytes.Buffer
	inSSE.WriteString("data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"}}]}\n\n")
	inSSE.WriteString("data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_0\",\"type\":\"function\",\"function\":{\"name\":\"fn0\",\"arguments\":\"\"}},{\"index\":1,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"fn1\",\"arguments\":\"\"}}]}}]}\n\n")

	inSSE.WriteString("data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"A0_part1\"}}]}}]}\n\n")
	inSSE.WriteString("data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":1,\"function\":{\"arguments\":\"B1_part1\"}}]}}]}\n\n")
	inSSE.WriteString("data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"A0_part2\"}}]}}]}\n\n")
	inSSE.WriteString("data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":1,\"function\":{\"arguments\":\"B1_part2\"}}]}}]}\n\n")
	inSSE.WriteString("data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n")
	inSSE.WriteString("data: [DONE]\n\n")

	slowWriter := &slowDownstreamWriter{delay: 2 * time.Millisecond}
	pipeline := aggregator.NewStreamPipeline(aggregator.DefaultBufferConfig(), nil)

	err := pipeline.ProcessStream(context.Background(), io.NopCloser(&inSSE), slowWriter)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	r := sse.NewReader(bytes.NewReader(slowWriter.Bytes()))
	args0 := ""
	args1 := ""

	for {
		ev, err := r.ReadEvent()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if string(ev.Data) == "[DONE]" {
			continue
		}
		var chunk map[string]interface{}
		if json.Unmarshal(ev.Data, &chunk) == nil {
			if choices, ok := chunk["choices"].([]interface{}); ok && len(choices) > 0 {
				first := choices[0].(map[string]interface{})
				if delta, ok := first["delta"].(map[string]interface{}); ok {
					if tcs, ok := delta["tool_calls"].([]interface{}); ok {
						for _, rawTc := range tcs {
							tc := rawTc.(map[string]interface{})
							idx := int(tc["index"].(float64))
							fn := tc["function"].(map[string]interface{})
							arg := fn["arguments"].(string)
							if idx == 0 {
								args0 += arg
							} else if idx == 1 {
								args1 += arg
							}
						}
					}
				}
			}
		}
	}

	if args0 != "A0_part1A0_part2" {
		t.Fatalf("expected args0 'A0_part1A0_part2', got %q", args0)
	}
	if args1 != "B1_part1B1_part2" {
		t.Fatalf("expected args1 'B1_part1B1_part2', got %q", args1)
	}
}

func TestStrictBarrierOrder(t *testing.T) {
	fixtureData, err := os.ReadFile("../fixtures/chat_reasoning_content_tool.sse")
	if err != nil {
		t.Fatalf("failed to read fixture: %v", err)
	}

	slowWriter := &slowDownstreamWriter{delay: 2 * time.Millisecond}
	pipeline := aggregator.NewStreamPipeline(aggregator.DefaultBufferConfig(), nil)

	err = pipeline.ProcessStream(context.Background(), io.NopCloser(bytes.NewReader(fixtureData)), slowWriter)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	r := sse.NewReader(bytes.NewReader(slowWriter.Bytes()))
	var eventTypes []string

	for {
		ev, err := r.ReadEvent()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if string(ev.Data) == "[DONE]" {
			eventTypes = append(eventTypes, "DONE")
			continue
		}
		var chunk map[string]interface{}
		if json.Unmarshal(ev.Data, &chunk) == nil {
			if choices, ok := chunk["choices"].([]interface{}); ok && len(choices) > 0 {
				first := choices[0].(map[string]interface{})
				if fr, ok := first["finish_reason"].(string); ok && fr != "" {
					eventTypes = append(eventTypes, "FINISH")
					continue
				}
				if delta, ok := first["delta"].(map[string]interface{}); ok {
					if _, ok := delta["role"]; ok {
						eventTypes = append(eventTypes, "ROLE")
					} else if _, ok := delta["reasoning_content"]; ok {
						eventTypes = append(eventTypes, "REASONING")
					} else if _, ok := delta["content"]; ok {
						eventTypes = append(eventTypes, "CONTENT")
					} else if _, ok := delta["tool_calls"]; ok {
						eventTypes = append(eventTypes, "TOOL")
					}
				}
			}
		}
	}

	var collapsedStages []string
	for _, st := range eventTypes {
		if len(collapsedStages) == 0 || collapsedStages[len(collapsedStages)-1] != st {
			collapsedStages = append(collapsedStages, st)
		}
	}

	expectedOrder := []string{"ROLE", "REASONING", "CONTENT", "TOOL", "CONTENT", "FINISH", "DONE"}
	if len(collapsedStages) != len(expectedOrder) {
		t.Fatalf("expected exactly %d ordered stage transitions %v, got %d %v (raw: %v)", len(expectedOrder), expectedOrder, len(collapsedStages), collapsedStages, eventTypes)
	}
	for i, expected := range expectedOrder {
		if collapsedStages[i] != expected {
			t.Fatalf("stage transition %d mismatch: expected %s, got %s. Full sequence: %v", i, expected, collapsedStages[i], collapsedStages)
		}
	}
}

func TestSlowClientBackpressureAndRecovery(t *testing.T) {
	var inSSE bytes.Buffer
	inSSE.WriteString("data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"}}]}\n\n")

	largeData := strings.Repeat("x", 1000)
	for i := 0; i < 50; i++ {
		inSSE.WriteString(fmt.Sprintf("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":%q}}]}\n\n", largeData))
	}
	inSSE.WriteString("data: [DONE]\n\n")

	m := &metrics.StreamMetrics{}
	cfg := aggregator.BufferConfig{
		HighWatermark: 5 * 1024,
		LowWatermark:  2 * 1024,
	}

	slowWriter := &slowDownstreamWriter{delay: 10 * time.Millisecond}
	pipeline := aggregator.NewStreamPipeline(cfg, m)

	err := pipeline.ProcessStream(context.Background(), io.NopCloser(&inSSE), slowWriter)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if m.ReaderPauseCount == 0 {
		t.Fatalf("expected reader pause count > 0 during slow downstream, got %d", m.ReaderPauseCount)
	}
	if m.DownstreamSSEEvents >= m.UpstreamSSEEvents {
		t.Fatalf("expected coalescing to reduce downstream events (upstream: %d, downstream: %d)", m.UpstreamSSEEvents, m.DownstreamSSEEvents)
	}
}

func TestDuplicateRoleStreamDoesNotBreakCoalescing(t *testing.T) {
	var inSSE bytes.Buffer
	for i := 1; i <= 20; i++ {
		inSSE.WriteString(fmt.Sprintf("data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"part%d-\"}}]}\n\n", i))
	}
	inSSE.WriteString("data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n")
	inSSE.WriteString("data: [DONE]\n\n")

	slowWriter := &slowDownstreamWriter{delay: 2 * time.Millisecond}
	pipeline := aggregator.NewStreamPipeline(aggregator.DefaultBufferConfig(), nil)

	err := pipeline.ProcessStream(context.Background(), io.NopCloser(&inSSE), slowWriter)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	r := sse.NewReader(bytes.NewReader(slowWriter.Bytes()))
	roleCount := 0
	var allContent strings.Builder

	for {
		ev, err := r.ReadEvent()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if string(ev.Data) == "[DONE]" {
			continue
		}
		var chunk map[string]interface{}
		if json.Unmarshal(ev.Data, &chunk) == nil {
			if choices, ok := chunk["choices"].([]interface{}); ok && len(choices) > 0 {
				first := choices[0].(map[string]interface{})
				if delta, ok := first["delta"].(map[string]interface{}); ok {
					if _, ok := delta["role"]; ok {
						roleCount++
					}
					if c, ok := delta["content"].(string); ok {
						allContent.WriteString(c)
					}
				}
			}
		}
	}

	if roleCount != 1 {
		t.Fatalf("expected exactly 1 role downstream, got %d", roleCount)
	}
	expectedConcat := "part1-part2-part3-part4-part5-part6-part7-part8-part9-part10-part11-part12-part13-part14-part15-part16-part17-part18-part19-part20-"
	if allContent.String() != expectedConcat {
		t.Fatalf("expected concatenated content %q, got %q", expectedConcat, allContent.String())
	}
}

func TestPipelineChunkWithoutCreatedGetsTimestamp(t *testing.T) {
	var inSSE bytes.Buffer
	inSSE.WriteString("data: {\"id\":\"FBGcati3GsG6mtkP4v7lmQM\",\"object\":\"chat.completion.chunk\",\"model\":\"gemini-3.8-flash\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"}}]}\n\n")
	inSSE.WriteString("data: {\"id\":\"FBGcati3GsG6mtkP4v7lmQM\",\"object\":\"chat.completion.chunk\",\"model\":\"gemini-3.8-flash\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"}}]}\n\n")
	inSSE.WriteString("data: [DONE]\n\n")

	downstream := &slowDownstreamWriter{}
	m := &metrics.StreamMetrics{}
	pipeline := aggregator.NewStreamPipeline(aggregator.DefaultBufferConfig(), m)
	reqStartTime := time.Now()
	pipeline.SetRequestInfo("gemini-3.8-flash", reqStartTime)

	if err := pipeline.ProcessStream(context.Background(), io.NopCloser(&inSSE), downstream); err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}

	r := sse.NewReader(bytes.NewReader(downstream.Bytes()))
	chunksChecked := 0
	for {
		ev, err := r.ReadEvent()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if string(ev.Data) == "[DONE]" {
			continue
		}
		var chunk map[string]interface{}
		if err := json.Unmarshal(ev.Data, &chunk); err != nil {
			t.Fatalf("failed to unmarshal chunk: %v", err)
		}
		createdVal, hasCreated := chunk["created"]
		if !hasCreated {
			t.Fatalf("chunk missing 'created': %s", string(ev.Data))
		}
		cFloat, ok := createdVal.(float64)
		if !ok || int64(cFloat) <= 0 {
			t.Fatalf("invalid created timestamp: %v", createdVal)
		}
		if chunk["model"] != "gemini-3.8-flash" {
			t.Fatalf("unexpected model: %v", chunk["model"])
		}
		chunksChecked++
	}
	if chunksChecked == 0 {
		t.Fatalf("expected at least 1 chunk checked")
	}
}
