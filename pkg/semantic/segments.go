package semantic

import (
	"strings"
)

type ReasoningSegment struct {
	ChoiceIndex int
	FieldName   string
	Text        string
	Metadata    CommonMetadata
}

func (s *ReasoningSegment) Type() EventType {
	return EventReasoning
}

func (s *ReasoningSegment) BytesLen() int {
	return len(s.Text) + 64
}

func (s *ReasoningSegment) CanMerge(next Segment) bool {
	n, ok := next.(*ReasoningSegment)
	if !ok {
		return false
	}
	return s.ChoiceIndex == n.ChoiceIndex && s.FieldName == n.FieldName
}

func (s *ReasoningSegment) Merge(next Segment) bool {
	if !s.CanMerge(next) {
		return false
	}
	n := next.(*ReasoningSegment)
	s.Text += n.Text
	return true
}

type ContentSegment struct {
	ChoiceIndex int
	Text        string
	Metadata    CommonMetadata
}

func (s *ContentSegment) Type() EventType {
	return EventContent
}

func (s *ContentSegment) BytesLen() int {
	return len(s.Text) + 64
}

func (s *ContentSegment) CanMerge(next Segment) bool {
	n, ok := next.(*ContentSegment)
	if !ok {
		return false
	}
	return s.ChoiceIndex == n.ChoiceIndex
}

func (s *ContentSegment) Merge(next Segment) bool {
	if !s.CanMerge(next) {
		return false
	}
	n := next.(*ContentSegment)
	s.Text += n.Text
	return true
}

type ToolCallAccumulator struct {
	Index        int
	ID           string
	Type         string
	FunctionName string
	Arguments    string
}

type ToolCallSegment struct {
	ChoiceIndex int
	Order       []int
	Calls       map[int]*ToolCallAccumulator
	Metadata    CommonMetadata
}

func (s *ToolCallSegment) Type() EventType {
	return EventToolCall
}

func (s *ToolCallSegment) BytesLen() int {
	total := 128
	for _, call := range s.Calls {
		total += len(call.ID) + len(call.Type) + len(call.FunctionName) + len(call.Arguments) + 32
	}
	return total
}

func (s *ToolCallSegment) CanMerge(next Segment) bool {
	n, ok := next.(*ToolCallSegment)
	if !ok {
		return false
	}
	if s.ChoiceIndex != n.ChoiceIndex {
		return false
	}
	for idx, nextCall := range n.Calls {
		if existing, exists := s.Calls[idx]; exists {
			if existing.ID != "" && nextCall.ID != "" && existing.ID != nextCall.ID {
				return false
			}
		}
	}
	return true
}

func (s *ToolCallSegment) Merge(next Segment) bool {
	if !s.CanMerge(next) {
		return false
	}
	n := next.(*ToolCallSegment)
	for _, idx := range n.Order {
		nextCall := n.Calls[idx]
		existing, exists := s.Calls[idx]
		if !exists {
			s.Order = append(s.Order, idx)
			clone := *nextCall
			s.Calls[idx] = &clone
			continue
		}
		if existing.ID == "" && nextCall.ID != "" {
			existing.ID = nextCall.ID
		}
		if existing.Type == "" && nextCall.Type != "" {
			existing.Type = nextCall.Type
		}
		if nextCall.FunctionName != "" {
			if existing.FunctionName == "" {
				existing.FunctionName = nextCall.FunctionName
			} else if existing.FunctionName != nextCall.FunctionName {
				if strings.HasPrefix(nextCall.FunctionName, existing.FunctionName) {
					existing.FunctionName = nextCall.FunctionName
				} else if !strings.HasPrefix(existing.FunctionName, nextCall.FunctionName) {
					existing.FunctionName += nextCall.FunctionName
				}
			}
		}
		existing.Arguments += nextCall.Arguments
	}
	return true
}

type RoleSegment struct {
	ChoiceIndex int
	Role        string
	Metadata    CommonMetadata
}

func (s *RoleSegment) Type() EventType {
	return EventRole
}

func (s *RoleSegment) BytesLen() int {
	return len(s.Role) + 64
}

func (s *RoleSegment) CanMerge(next Segment) bool {
	n, ok := next.(*RoleSegment)
	if !ok {
		return false
	}
	return s.ChoiceIndex == n.ChoiceIndex && s.Role == n.Role
}

func (s *RoleSegment) Merge(next Segment) bool {
	return s.CanMerge(next)
}

type FinishSegment struct {
	ChoiceIndex  int
	FinishReason string
	Usage        interface{}
	Metadata     CommonMetadata
}

func (s *FinishSegment) Type() EventType {
	return EventFinish
}

func (s *FinishSegment) BytesLen() int {
	return len(s.FinishReason) + 64
}

func (s *FinishSegment) CanMerge(next Segment) bool {
	return false
}

func (s *FinishSegment) Merge(next Segment) bool {
	return false
}

type UsageSegment struct {
	Usage    interface{}
	Metadata CommonMetadata
}

func (s *UsageSegment) Type() EventType {
	return EventUsage
}

func (s *UsageSegment) BytesLen() int {
	return 128
}

func (s *UsageSegment) CanMerge(next Segment) bool {
	return false
}

func (s *UsageSegment) Merge(next Segment) bool {
	return false
}

type RawSegment struct {
	SegmentType EventType
	Data        []byte
	Metadata    CommonMetadata
}

func (s *RawSegment) Type() EventType {
	return s.SegmentType
}

func (s *RawSegment) BytesLen() int {
	return len(s.Data)
}

func (s *RawSegment) CanMerge(next Segment) bool {
	return false
}

func (s *RawSegment) Merge(next Segment) bool {
	return false
}

type ResponseTextDeltaSegment struct {
	EventType      string
	ResponseID     string
	ItemID         string
	OutputIndex    int
	ContentIndex   int
	SequenceNumber int
	Delta          string
	HasLogprobs    bool
	Logprobs       []interface{}
	SSEID          string
	Extra          map[string]interface{}
	Metadata       CommonMetadata
}

func (s *ResponseTextDeltaSegment) Type() EventType {
	return EventContent
}

func (s *ResponseTextDeltaSegment) BytesLen() int {
	return len(s.Delta) + 128
}

func (s *ResponseTextDeltaSegment) CanMerge(next Segment) bool {
	n, ok := next.(*ResponseTextDeltaSegment)
	if !ok {
		return false
	}
	return s.EventType == n.EventType &&
		s.ItemID == n.ItemID &&
		s.OutputIndex == n.OutputIndex &&
		s.ContentIndex == n.ContentIndex
}

func (s *ResponseTextDeltaSegment) Merge(next Segment) bool {
	if !s.CanMerge(next) {
		return false
	}
	n := next.(*ResponseTextDeltaSegment)
	s.Delta += n.Delta
	s.SequenceNumber = n.SequenceNumber
	if n.ResponseID != "" {
		s.ResponseID = n.ResponseID
	}
	if n.SSEID != "" {
		s.SSEID = n.SSEID
	}
	if n.HasLogprobs {
		s.HasLogprobs = true
		s.Logprobs = append(s.Logprobs, n.Logprobs...)
	}
	for k, v := range n.Extra {
		if s.Extra == nil {
			s.Extra = make(map[string]interface{})
		}
		s.Extra[k] = v
	}
	return true
}

type ResponseReasoningDeltaSegment struct {
	EventType      string
	ResponseID     string
	ItemID         string
	OutputIndex    int
	ContentIndex   *int
	SummaryIndex   *int
	SequenceNumber int
	Delta          string
	SSEID          string
	Extra          map[string]interface{}
	Metadata       CommonMetadata
}

func (s *ResponseReasoningDeltaSegment) Type() EventType {
	return EventReasoning
}

func (s *ResponseReasoningDeltaSegment) BytesLen() int {
	return len(s.Delta) + 128
}

func (s *ResponseReasoningDeltaSegment) CanMerge(next Segment) bool {
	n, ok := next.(*ResponseReasoningDeltaSegment)
	if !ok {
		return false
	}
	if s.EventType != n.EventType || s.ItemID != n.ItemID || s.OutputIndex != n.OutputIndex {
		return false
	}
	if (s.SummaryIndex == nil) != (n.SummaryIndex == nil) {
		return false
	}
	if s.SummaryIndex != nil && *s.SummaryIndex != *n.SummaryIndex {
		return false
	}
	if (s.ContentIndex == nil) != (n.ContentIndex == nil) {
		return false
	}
	if s.ContentIndex != nil && *s.ContentIndex != *n.ContentIndex {
		return false
	}
	return true
}

func (s *ResponseReasoningDeltaSegment) Merge(next Segment) bool {
	if !s.CanMerge(next) {
		return false
	}
	n := next.(*ResponseReasoningDeltaSegment)
	s.Delta += n.Delta
	s.SequenceNumber = n.SequenceNumber
	if n.ResponseID != "" {
		s.ResponseID = n.ResponseID
	}
	if n.SSEID != "" {
		s.SSEID = n.SSEID
	}
	for k, v := range n.Extra {
		if s.Extra == nil {
			s.Extra = make(map[string]interface{})
		}
		s.Extra[k] = v
	}
	return true
}

type ResponseToolCallDeltaSegment struct {
	EventType      string
	ResponseID     string
	ItemID         string
	OutputIndex    int
	CallID         string
	SequenceNumber int
	Delta          string
	SSEID          string
	Extra          map[string]interface{}
	Metadata       CommonMetadata
}

func (s *ResponseToolCallDeltaSegment) Type() EventType {
	return EventToolCall
}

func (s *ResponseToolCallDeltaSegment) BytesLen() int {
	return len(s.Delta) + 128
}

func (s *ResponseToolCallDeltaSegment) CanMerge(next Segment) bool {
	n, ok := next.(*ResponseToolCallDeltaSegment)
	if !ok {
		return false
	}
	return s.EventType == n.EventType &&
		s.ItemID == n.ItemID &&
		s.OutputIndex == n.OutputIndex
}

func (s *ResponseToolCallDeltaSegment) Merge(next Segment) bool {
	if !s.CanMerge(next) {
		return false
	}
	n := next.(*ResponseToolCallDeltaSegment)
	s.Delta += n.Delta
	s.SequenceNumber = n.SequenceNumber
	if s.CallID == "" && n.CallID != "" {
		s.CallID = n.CallID
	}
	if n.ResponseID != "" {
		s.ResponseID = n.ResponseID
	}
	if n.SSEID != "" {
		s.SSEID = n.SSEID
	}
	for k, v := range n.Extra {
		if s.Extra == nil {
			s.Extra = make(map[string]interface{})
		}
		s.Extra[k] = v
	}
	return true
}

type ResponseControlSegment struct {
	EventType string
	ID        string
	Data      []byte
	Usage     interface{}
	Model     string
	IsDone    bool
	IsError   bool
	Metadata  CommonMetadata
}

func (s *ResponseControlSegment) Type() EventType {
	if s.IsDone {
		return EventDone
	}
	if s.IsError {
		return EventError
	}
	if s.Usage != nil {
		return EventUsage
	}
	return EventUnknown
}

func (s *ResponseControlSegment) BytesLen() int {
	return len(s.Data) + len(s.EventType) + len(s.ID) + 32
}

func (s *ResponseControlSegment) CanMerge(next Segment) bool {
	return false
}

func (s *ResponseControlSegment) Merge(next Segment) bool {
	return false
}
