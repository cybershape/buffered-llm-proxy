package semantic

import (
	"encoding/json"
	"time"

	"buffered-proxy/pkg/sse"
)

type Serializer struct{}

func NewSerializer() *Serializer {
	return &Serializer{}
}

func (s *Serializer) SerializeSegment(seg Segment) []byte {
	switch v := seg.(type) {
	case *ReasoningSegment:
		return s.serializeReasoning(v)
	case *ContentSegment:
		return s.serializeContent(v)
	case *ToolCallSegment:
		return s.serializeToolCall(v)
	case *RoleSegment:
		return s.serializeRole(v)
	case *FinishSegment:
		return s.serializeFinish(v)
	case *UsageSegment:
		return s.serializeUsage(v)
	case *RawSegment:
		return s.serializeRaw(v)
	default:
		return nil
	}
}

func (s *Serializer) baseMap(meta CommonMetadata) map[string]interface{} {
	m := make(map[string]interface{})
	if meta.ID != "" {
		m["id"] = meta.ID
	}
	if meta.Object != "" {
		m["object"] = meta.Object
	} else {
		m["object"] = "chat.completion.chunk"
	}
	if meta.Created != 0 {
		m["created"] = meta.Created
	} else {
		m["created"] = time.Now().Unix()
	}
	if meta.Model != "" {
		m["model"] = meta.Model
	}
	if meta.SystemFingerprint != "" {
		m["system_fingerprint"] = meta.SystemFingerprint
	}
	if meta.ServiceTier != "" {
		m["service_tier"] = meta.ServiceTier
	}
	for k, val := range meta.Extra {
		m[k] = val
	}
	return m
}

func (s *Serializer) serializeReasoning(seg *ReasoningSegment) []byte {
	root := s.baseMap(seg.Metadata)
	delta := map[string]interface{}{
		seg.FieldName: seg.Text,
	}
	choice := map[string]interface{}{
		"index": seg.ChoiceIndex,
		"delta": delta,
	}
	root["choices"] = []interface{}{choice}
	data, _ := json.Marshal(root)
	return sse.EncodeData(data)
}

func (s *Serializer) serializeContent(seg *ContentSegment) []byte {
	root := s.baseMap(seg.Metadata)
	delta := map[string]interface{}{
		"content": seg.Text,
	}
	choice := map[string]interface{}{
		"index": seg.ChoiceIndex,
		"delta": delta,
	}
	root["choices"] = []interface{}{choice}
	data, _ := json.Marshal(root)
	return sse.EncodeData(data)
}

func (s *Serializer) serializeToolCall(seg *ToolCallSegment) []byte {
	root := s.baseMap(seg.Metadata)
	tcList := make([]map[string]interface{}, 0, len(seg.Order))
	for _, idx := range seg.Order {
		call := seg.Calls[idx]
		fnMap := map[string]interface{}{
			"name":      call.FunctionName,
			"arguments": call.Arguments,
		}
		item := map[string]interface{}{
			"index":    call.Index,
			"function": fnMap,
		}
		if call.ID != "" {
			item["id"] = call.ID
		}
		if call.Type != "" {
			item["type"] = call.Type
		}
		tcList = append(tcList, item)
	}
	delta := map[string]interface{}{
		"tool_calls": tcList,
	}
	choice := map[string]interface{}{
		"index": seg.ChoiceIndex,
		"delta": delta,
	}
	root["choices"] = []interface{}{choice}
	data, _ := json.Marshal(root)
	return sse.EncodeData(data)
}

func (s *Serializer) serializeRole(seg *RoleSegment) []byte {
	root := s.baseMap(seg.Metadata)
	delta := map[string]interface{}{
		"role": seg.Role,
	}
	choice := map[string]interface{}{
		"index": seg.ChoiceIndex,
		"delta": delta,
	}
	root["choices"] = []interface{}{choice}
	data, _ := json.Marshal(root)
	return sse.EncodeData(data)
}

func (s *Serializer) serializeFinish(seg *FinishSegment) []byte {
	root := s.baseMap(seg.Metadata)
	choice := map[string]interface{}{
		"index":         seg.ChoiceIndex,
		"delta":         map[string]interface{}{},
		"finish_reason": seg.FinishReason,
	}
	root["choices"] = []interface{}{choice}
	data, _ := json.Marshal(root)
	return sse.EncodeData(data)
}

func (s *Serializer) serializeUsage(seg *UsageSegment) []byte {
	root := s.baseMap(seg.Metadata)
	root["choices"] = []interface{}{}
	root["usage"] = seg.Usage
	data, _ := json.Marshal(root)
	return sse.EncodeData(data)
}

func (s *Serializer) serializeRaw(seg *RawSegment) []byte {
	if seg.SegmentType == EventDone {
		return sse.EncodeDone()
	}
	return sse.EncodeData(seg.Data)
}
