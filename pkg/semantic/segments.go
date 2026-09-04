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
