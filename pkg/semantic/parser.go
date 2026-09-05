package semantic

import (
	"bytes"
	"encoding/json"
	"strconv"
	"time"

	"buffered-proxy/pkg/sse"
)

type Parser struct {
	meta      CommonMetadata
	seenRoles map[int]string
}

func NewParser() *Parser {
	return &Parser{
		meta: CommonMetadata{
			Created: time.Now().Unix(),
		},
		seenRoles: make(map[int]string),
	}
}

func (p *Parser) SetStartTime(t time.Time) {
	if !t.IsZero() {
		p.meta.Created = t.Unix()
	}
}

func (p *Parser) Metadata() CommonMetadata {
	return p.meta
}

func (p *Parser) Model() string {
	return p.meta.Model
}

func (p *Parser) ParseEvent(ev *sse.Event) ([]Segment, error) {
	data := bytes.TrimSpace(ev.Data)
	if len(data) == 0 {
		return nil, nil
	}
	if string(data) == "[DONE]" {
		return []Segment{&RawSegment{
			SegmentType: EventDone,
			Data:        data,
			Metadata:    p.meta,
		}}, nil
	}

	var rootMap map[string]json.RawMessage
	if err := json.Unmarshal(data, &rootMap); err != nil {
		return []Segment{&RawSegment{
			SegmentType: EventUnknown,
			Data:        data,
			Metadata:    p.meta,
		}}, nil
	}

	if _, hasErr := rootMap["error"]; hasErr {
		return []Segment{&RawSegment{
			SegmentType: EventError,
			Data:        data,
			Metadata:    p.meta,
		}}, nil
	}

	p.extractMetadata(rootMap)

	var segments []Segment

	rawChoices, hasChoices := rootMap["choices"]
	if hasChoices && len(rawChoices) > 0 && string(rawChoices) != "null" {
		var choices []ChoiceChunk
		if err := json.Unmarshal(rawChoices, &choices); err == nil {
			for _, choice := range choices {
				choiceSegments := p.parseChoice(choice)
				segments = append(segments, choiceSegments...)
			}
		}
	}

	if rawUsage, hasUsage := rootMap["usage"]; hasUsage && len(rawUsage) > 0 && string(rawUsage) != "null" {
		var usageVal interface{}
		if err := json.Unmarshal(rawUsage, &usageVal); err == nil {
			segments = append(segments, &UsageSegment{
				Usage:    usageVal,
				Metadata: p.meta,
			})
		}
	}

	if len(segments) == 0 {
		segments = append(segments, &RawSegment{
			SegmentType: EventUnknown,
			Data:        data,
			Metadata:    p.meta,
		})
	}

	return segments, nil
}

func (p *Parser) extractMetadata(rootMap map[string]json.RawMessage) {
	if raw, ok := rootMap["id"]; ok {
		var s string
		if json.Unmarshal(raw, &s) == nil && s != "" {
			p.meta.ID = s
		}
	}
	if raw, ok := rootMap["object"]; ok {
		var s string
		if json.Unmarshal(raw, &s) == nil && s != "" {
			p.meta.Object = s
		}
	}
	if raw, ok := rootMap["created"]; ok {
		var c int64
		if json.Unmarshal(raw, &c) == nil && c != 0 {
			p.meta.Created = c
		} else {
			var f float64
			if json.Unmarshal(raw, &f) == nil && f != 0 {
				p.meta.Created = int64(f)
			} else {
				var s string
				if json.Unmarshal(raw, &s) == nil && s != "" {
					if parsedInt, err := strconv.ParseInt(s, 10, 64); err == nil && parsedInt != 0 {
						p.meta.Created = parsedInt
					}
				}
			}
		}
	}
	if raw, ok := rootMap["model"]; ok {
		var s string
		if json.Unmarshal(raw, &s) == nil && s != "" {
			p.meta.Model = s
		}
	}
	if raw, ok := rootMap["system_fingerprint"]; ok {
		var s string
		if json.Unmarshal(raw, &s) == nil && s != "" {
			p.meta.SystemFingerprint = s
		}
	}
	if raw, ok := rootMap["service_tier"]; ok {
		var s string
		if json.Unmarshal(raw, &s) == nil && s != "" {
			p.meta.ServiceTier = s
		}
	}

	knownKeys := map[string]bool{
		"id":                 true,
		"object":             true,
		"created":            true,
		"model":              true,
		"system_fingerprint": true,
		"service_tier":       true,
		"choices":            true,
		"usage":              true,
		"error":              true,
	}

	for k, raw := range rootMap {
		if !knownKeys[k] {
			if p.meta.Extra == nil {
				p.meta.Extra = make(map[string]interface{})
			}
			var val interface{}
			if json.Unmarshal(raw, &val) == nil {
				p.meta.Extra[k] = val
			}
		}
	}
}

func (p *Parser) parseChoice(choice ChoiceChunk) []Segment {
	var segments []Segment

	if len(choice.Delta) > 0 && string(choice.Delta) != "null" {
		var deltaMap map[string]json.RawMessage
		if err := json.Unmarshal(choice.Delta, &deltaMap); err == nil {
			if rawRole, ok := deltaMap["role"]; ok {
				var roleStr string
				if json.Unmarshal(rawRole, &roleStr) == nil && roleStr != "" {
					if p.seenRoles[choice.Index] != roleStr {
						p.seenRoles[choice.Index] = roleStr
						segments = append(segments, &RoleSegment{
							ChoiceIndex: choice.Index,
							Role:        roleStr,
							Metadata:    p.meta,
						})
					}
				}
			}

			reasoningKeys := []string{"reasoning_content", "reasoning", "reasoning_text", "thought"}
			for _, key := range reasoningKeys {
				if rawR, ok := deltaMap[key]; ok {
					var rStr string
					if json.Unmarshal(rawR, &rStr) == nil && len(rStr) > 0 {
						segments = append(segments, &ReasoningSegment{
							ChoiceIndex: choice.Index,
							FieldName:   key,
							Text:        rStr,
							Metadata:    p.meta,
						})
						break
					}
				}
			}

			if rawContent, ok := deltaMap["content"]; ok {
				var cStr *string
				if json.Unmarshal(rawContent, &cStr) == nil && cStr != nil && len(*cStr) > 0 {
					segments = append(segments, &ContentSegment{
						ChoiceIndex: choice.Index,
						Text:        *cStr,
						Metadata:    p.meta,
					})
				}
			}

			if rawTools, ok := deltaMap["tool_calls"]; ok && len(rawTools) > 0 && string(rawTools) != "null" {
				var tcList []ToolCallDelta
				if json.Unmarshal(rawTools, &tcList) == nil && len(tcList) > 0 {
					tcSeg := &ToolCallSegment{
						ChoiceIndex: choice.Index,
						Order:       make([]int, 0, len(tcList)),
						Calls:       make(map[int]*ToolCallAccumulator),
						Metadata:    p.meta,
					}
					for _, tc := range tcList {
						idx := tc.Index
						accum := &ToolCallAccumulator{
							Index: idx,
							ID:    tc.ID,
							Type:  tc.Type,
						}
						if tc.Function != nil {
							accum.FunctionName = tc.Function.Name
							accum.Arguments = tc.Function.Arguments
						}
						tcSeg.Order = append(tcSeg.Order, idx)
						tcSeg.Calls[idx] = accum
					}
					segments = append(segments, tcSeg)
				}
			}

			knownDeltaKeys := map[string]bool{
				"role":              true,
				"reasoning_content": true,
				"reasoning":         true,
				"reasoning_text":    true,
				"thought":           true,
				"content":           true,
				"tool_calls":        true,
			}
			var extraDelta map[string]interface{}
			for k, raw := range deltaMap {
				if !knownDeltaKeys[k] {
					if extraDelta == nil {
						extraDelta = make(map[string]interface{})
					}
					var v interface{}
					if json.Unmarshal(raw, &v) == nil {
						extraDelta[k] = v
					}
				}
			}
			if len(extraDelta) > 0 && len(segments) == 0 {
				segments = append(segments, &RawSegment{
					SegmentType: EventUnknown,
					Data:        choice.Delta,
					Metadata:    p.meta,
				})
			}
		}
	}

	if choice.FinishReason != nil && *choice.FinishReason != "" && *choice.FinishReason != "null" {
		segments = append(segments, &FinishSegment{
			ChoiceIndex:  choice.Index,
			FinishReason: *choice.FinishReason,
			Metadata:     p.meta,
		})
	}

	return segments
}
