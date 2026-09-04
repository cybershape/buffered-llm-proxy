package semantic

import (
	"encoding/json"
)

type EventType int

const (
	EventUnknown EventType = iota
	EventRole
	EventReasoning
	EventContent
	EventToolCall
	EventFinish
	EventUsage
	EventError
	EventDone
)

type FunctionCallDelta struct {
	Name      string `json:"name,omitempty"`
	Arguments string `json:"arguments,omitempty"`
}

type ToolCallDelta struct {
	Index    int                `json:"index"`
	ID       string             `json:"id,omitempty"`
	Type     string             `json:"type,omitempty"`
	Function *FunctionCallDelta `json:"function,omitempty"`
}

type DeltaPayload struct {
	Role             string                 `json:"role,omitempty"`
	Content          *string                `json:"content,omitempty"`
	ReasoningContent *string                `json:"reasoning_content,omitempty"`
	Reasoning        *string                `json:"reasoning,omitempty"`
	ReasoningText    *string                `json:"reasoning_text,omitempty"`
	Thought          *string                `json:"thought,omitempty"`
	ToolCalls        []ToolCallDelta        `json:"tool_calls,omitempty"`
	Extra            map[string]interface{} `json:"-"`
}

type ChoiceChunk struct {
	Index        int             `json:"index"`
	Delta        json.RawMessage `json:"delta,omitempty"`
	FinishReason *string         `json:"finish_reason,omitempty"`
}

type StreamChunk struct {
	ID                string                 `json:"id,omitempty"`
	Object            string                 `json:"object,omitempty"`
	Created           int64                  `json:"created,omitempty"`
	Model             string                 `json:"model,omitempty"`
	SystemFingerprint string                 `json:"system_fingerprint,omitempty"`
	ServiceTier       string                 `json:"service_tier,omitempty"`
	Choices           []ChoiceChunk          `json:"choices,omitempty"`
	Usage             interface{}            `json:"usage,omitempty"`
	Error             interface{}            `json:"error,omitempty"`
	Extra             map[string]interface{} `json:"-"`
}

type CommonMetadata struct {
	ID                string
	Object            string
	Created           int64
	Model             string
	SystemFingerprint string
	ServiceTier       string
	Extra             map[string]interface{}
}

func (m *CommonMetadata) UpdateFrom(chunk *StreamChunk) {
	if chunk.ID != "" {
		m.ID = chunk.ID
	}
	if chunk.Object != "" {
		m.Object = chunk.Object
	}
	if chunk.Created != 0 {
		m.Created = chunk.Created
	}
	if chunk.Model != "" {
		m.Model = chunk.Model
	}
	if chunk.SystemFingerprint != "" {
		m.SystemFingerprint = chunk.SystemFingerprint
	}
	if chunk.ServiceTier != "" {
		m.ServiceTier = chunk.ServiceTier
	}
	if len(chunk.Extra) > 0 {
		if m.Extra == nil {
			m.Extra = make(map[string]interface{})
		}
		for k, v := range chunk.Extra {
			m.Extra[k] = v
		}
	}
}

type Segment interface {
	Type() EventType
	BytesLen() int
	CanMerge(next Segment) bool
	Merge(next Segment) bool
}
