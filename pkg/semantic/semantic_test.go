package semantic

import (
	"encoding/json"
	"strings"
	"testing"

	"buffered-proxy/pkg/sse"
)

func TestReasoningMerge(t *testing.T) {
	r1 := &ReasoningSegment{
		ChoiceIndex: 0,
		FieldName:   "reasoning_content",
		Text:        "我",
		Metadata:    CommonMetadata{ID: "chat-1"},
	}
	r2 := &ReasoningSegment{
		ChoiceIndex: 0,
		FieldName:   "reasoning_content",
		Text:        "需要分析",
		Metadata:    CommonMetadata{ID: "chat-1"},
	}

	if !r1.CanMerge(r2) {
		t.Fatalf("expected r1 and r2 can merge")
	}
	r1.Merge(r2)
	if r1.Text != "我需要分析" {
		t.Fatalf("unexpected merged text: %s", r1.Text)
	}

	rDiffField := &ReasoningSegment{
		ChoiceIndex: 0,
		FieldName:   "reasoning",
		Text:        "其他",
	}
	if r1.CanMerge(rDiffField) {
		t.Fatalf("different reasoning field should not merge")
	}

	rDiffChoice := &ReasoningSegment{
		ChoiceIndex: 1,
		FieldName:   "reasoning_content",
		Text:        "其他",
	}
	if r1.CanMerge(rDiffChoice) {
		t.Fatalf("different choice index should not merge")
	}
}

func TestContentMerge(t *testing.T) {
	c1 := &ContentSegment{ChoiceIndex: 0, Text: "你"}
	c2 := &ContentSegment{ChoiceIndex: 0, Text: "好"}
	c3 := &ContentSegment{ChoiceIndex: 0, Text: "！"}

	if !c1.CanMerge(c2) {
		t.Fatalf("expected c1 and c2 can merge")
	}
	c1.Merge(c2)
	c1.Merge(c3)

	if c1.Text != "你好！" {
		t.Fatalf("unexpected content text: %s", c1.Text)
	}
}

func TestToolCallMerge(t *testing.T) {
	tc1 := &ToolCallSegment{
		ChoiceIndex: 0,
		Order:       []int{0},
		Calls: map[int]*ToolCallAccumulator{
			0: {
				Index:        0,
				ID:           "call_123",
				Type:         "function",
				FunctionName: "search",
				Arguments:    "{\"q\"",
			},
		},
	}

	tc2 := &ToolCallSegment{
		ChoiceIndex: 0,
		Order:       []int{0, 1},
		Calls: map[int]*ToolCallAccumulator{
			0: {
				Index:     0,
				Arguments: ":\"hello\"}",
			},
			1: {
				Index:        1,
				ID:           "call_456",
				Type:         "function",
				FunctionName: "get_time",
				Arguments:    "{\"zone\":\"UTC\"}",
			},
		},
	}

	if !tc1.CanMerge(tc2) {
		t.Fatalf("expected tc1 and tc2 can merge")
	}
	tc1.Merge(tc2)

	if len(tc1.Order) != 2 {
		t.Fatalf("expected 2 calls, got %d", len(tc1.Order))
	}
	call0 := tc1.Calls[0]
	if call0.ID != "call_123" || call0.FunctionName != "search" || call0.Arguments != "{\"q\":\"hello\"}" {
		t.Fatalf("unexpected call0: %+v", call0)
	}
	call1 := tc1.Calls[1]
	if call1.ID != "call_456" || call1.FunctionName != "get_time" || call1.Arguments != "{\"zone\":\"UTC\"}" {
		t.Fatalf("unexpected call1: %+v", call1)
	}
}

func TestBarrierRules(t *testing.T) {
	reasoning := &ReasoningSegment{ChoiceIndex: 0, FieldName: "reasoning", Text: "thinking"}
	content := &ContentSegment{ChoiceIndex: 0, Text: "result"}
	tool := &ToolCallSegment{ChoiceIndex: 0, Calls: make(map[int]*ToolCallAccumulator)}
	finish := &FinishSegment{ChoiceIndex: 0, FinishReason: "stop"}

	if reasoning.CanMerge(content) {
		t.Fatalf("reasoning and content must not merge")
	}
	if content.CanMerge(tool) {
		t.Fatalf("content and tool must not merge")
	}
	if tool.CanMerge(content) {
		t.Fatalf("tool and content must not merge")
	}
	if content.CanMerge(finish) {
		t.Fatalf("content and finish must not merge")
	}
	if finish.CanMerge(content) {
		t.Fatalf("finish and content must not merge")
	}
}

func TestParserAndSerializer(t *testing.T) {
	p := NewParser()
	s := NewSerializer()

	ev1 := &sse.Event{
		Data: []byte(`{"id":"chat-99","model":"gpt-4o","choices":[{"index":0,"delta":{"role":"assistant"}}]}`),
	}
	segs1, err := p.ParseEvent(ev1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(segs1) != 1 || segs1[0].Type() != EventRole {
		t.Fatalf("expected 1 role segment, got %v", segs1)
	}

	ev2 := &sse.Event{
		Data: []byte(`{"id":"chat-99","choices":[{"index":0,"delta":{"reasoning_content":"思考一"}}]}`),
	}
	segs2, _ := p.ParseEvent(ev2)
	ev3 := &sse.Event{
		Data: []byte(`{"choices":[{"index":0,"delta":{"reasoning_content":"思考二"}}]}`),
	}
	segs3, _ := p.ParseEvent(ev3)

	rSeg := segs2[0]
	if !rSeg.CanMerge(segs3[0]) {
		t.Fatalf("expected can merge")
	}
	rSeg.Merge(segs3[0])

	outBytes := s.SerializeSegment(rSeg)
	outStr := string(outBytes)
	if !strings.Contains(outStr, "思考一思考二") {
		t.Fatalf("expected concatenated reasoning text, got %s", outStr)
	}
	if !strings.Contains(outStr, "chat-99") {
		t.Fatalf("expected preserved id, got %s", outStr)
	}

	var parsed map[string]interface{}
	payload := strings.TrimPrefix(strings.TrimSpace(outStr), "data: ")
	if err := json.Unmarshal([]byte(payload), &parsed); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	choices := parsed["choices"].([]interface{})
	firstChoice := choices[0].(map[string]interface{})
	delta := firstChoice["delta"].(map[string]interface{})
	if delta["reasoning_content"] != "思考一思考二" {
		t.Fatalf("unexpected serialized reasoning: %v", delta["reasoning_content"])
	}
}

func TestDuplicateRoleIgnoredAndNotBarrier(t *testing.T) {
	p := NewParser()

	evRole := &sse.Event{
		Data: []byte(`{"choices":[{"index":0,"delta":{"role":"assistant"}}]}`),
	}
	segs1, err := p.ParseEvent(evRole)
	if err != nil || len(segs1) != 1 || segs1[0].Type() != EventRole {
		t.Fatalf("expected 1 role segment initially")
	}

	evContentWithDupRole := &sse.Event{
		Data: []byte(`{"choices":[{"index":0,"delta":{"role":"assistant","content":"hello"}}]}`),
	}
	segs2, err := p.ParseEvent(evContentWithDupRole)
	if err != nil {
		t.Fatalf("unexpected parse err: %v", err)
	}
	if len(segs2) != 1 || segs2[0].Type() != EventContent {
		t.Fatalf("duplicate role should be ignored, expected only 1 content segment, got: %v", segs2)
	}

	evContent2WithDupRole := &sse.Event{
		Data: []byte(`{"choices":[{"index":0,"delta":{"role":"assistant","content":" world"}}]}`),
	}
	segs3, err := p.ParseEvent(evContent2WithDupRole)
	if err != nil {
		t.Fatalf("unexpected parse err: %v", err)
	}
	if len(segs3) != 1 || segs3[0].Type() != EventContent {
		t.Fatalf("duplicate role should be ignored on next chunk, got: %v", segs3)
	}

	c1 := segs2[0].(*ContentSegment)
	if !c1.CanMerge(segs3[0]) {
		t.Fatalf("content segments should be able to merge seamlessly without role barrier")
	}
	c1.Merge(segs3[0])
	if c1.Text != "hello world" {
		t.Fatalf("expected merged text 'hello world', got %q", c1.Text)
	}

	roleSeg1 := &RoleSegment{ChoiceIndex: 0, Role: "assistant"}
	roleSeg2 := &RoleSegment{ChoiceIndex: 0, Role: "assistant"}
	if !roleSeg1.CanMerge(roleSeg2) {
		t.Fatalf("identical role segments should be mergeable/absorbable")
	}
}

func TestCreatedTimestampHandling(t *testing.T) {
	p := NewParser()
	s := NewSerializer()

	// 1. Upstream chunk lacking "created" (e.g. Gemini)
	evNoCreated := &sse.Event{
		Data: []byte(`{"id":"FBGcati3GsG6mtkP4v7lmQM","object":"chat.completion.chunk","model":"gemini-3.8-flash","choices":[{"index":0,"delta":{"content":"Hello"}}]}`),
	}
	segs, err := p.ParseEvent(evNoCreated)
	if err != nil || len(segs) != 1 {
		t.Fatalf("parse failed: %v", err)
	}

	out := s.SerializeSegment(segs[0])
	payload := strings.TrimPrefix(strings.TrimSpace(string(out)), "data: ")
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(payload), &parsed); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	createdVal, ok := parsed["created"]
	if !ok {
		t.Fatalf("expected 'created' field in output, but missing: %s", payload)
	}
	createdFloat, ok := createdVal.(float64)
	if !ok || int64(createdFloat) <= 0 {
		t.Fatalf("expected positive integer timestamp for 'created', got: %v", createdVal)
	}

	// 2. Upstream chunk with explicit "created"
	pWithCreated := NewParser()
	evWithCreated := &sse.Event{
		Data: []byte(`{"id":"chat-123","created":1741234567,"model":"gpt-4o","choices":[{"index":0,"delta":{"content":"Hi"}}]}`),
	}
	segs2, err := pWithCreated.ParseEvent(evWithCreated)
	if err != nil || len(segs2) != 1 {
		t.Fatalf("parse failed: %v", err)
	}
	out2 := s.SerializeSegment(segs2[0])
	var parsed2 map[string]interface{}
	_ = json.Unmarshal([]byte(strings.TrimPrefix(strings.TrimSpace(string(out2)), "data: ")), &parsed2)
	if int64(parsed2["created"].(float64)) != 1741234567 {
		t.Fatalf("expected preserved created timestamp 1741234567, got %v", parsed2["created"])
	}

	// 3. Fallback when segment Metadata has Created: 0
	bareSeg := &ContentSegment{
		ChoiceIndex: 0,
		Text:        "bare",
		Metadata:    CommonMetadata{ID: "bare-id"},
	}
	out3 := s.SerializeSegment(bareSeg)
	var parsed3 map[string]interface{}
	_ = json.Unmarshal([]byte(strings.TrimPrefix(strings.TrimSpace(string(out3)), "data: ")), &parsed3)
	if int64(parsed3["created"].(float64)) <= 0 {
		t.Fatalf("expected fallback unix timestamp for bare segment, got %v", parsed3["created"])
	}
}
