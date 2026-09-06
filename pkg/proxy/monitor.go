package proxy

import (
	"sync"
)

type PacketDirection string

const (
	DirectionUpstream   PacketDirection = "upstream"
	DirectionDownstream PacketDirection = "downstream"
)

type MonitorPacketEvent struct {
	SessionID   string          `json:"session_id"`
	Model       string          `json:"model"`
	TimestampMs int64           `json:"timestamp_ms"`
	Direction   PacketDirection `json:"direction"`
	PacketType  string          `json:"packet_type"`
	Payload     string          `json:"payload"`
	Summary     string          `json:"summary,omitempty"`
}

type MonitorHub struct {
	mu          sync.RWMutex
	subscribers map[string]map[chan MonitorPacketEvent]struct{}
	allSubs     map[chan MonitorPacketEvent]struct{}
}

func NewMonitorHub() *MonitorHub {
	return &MonitorHub{
		subscribers: make(map[string]map[chan MonitorPacketEvent]struct{}),
		allSubs:     make(map[chan MonitorPacketEvent]struct{}),
	}
}

func (h *MonitorHub) HasSubscribers(model string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.allSubs) > 0 {
		return true
	}
	if model != "" {
		if subs, ok := h.subscribers[model]; ok && len(subs) > 0 {
			return true
		}
	}
	return false
}

func (h *MonitorHub) Subscribe(model string) (chan MonitorPacketEvent, func()) {
	h.mu.Lock()
	defer h.mu.Unlock()

	ch := make(chan MonitorPacketEvent, 256)

	if model == "" {
		h.allSubs[ch] = struct{}{}
	} else {
		subs, ok := h.subscribers[model]
		if !ok {
			subs = make(map[chan MonitorPacketEvent]struct{})
			h.subscribers[model] = subs
		}
		subs[ch] = struct{}{}
	}

	var once sync.Once
	unsub := func() {
		once.Do(func() {
			h.mu.Lock()
			defer h.mu.Unlock()

			if model == "" {
				delete(h.allSubs, ch)
			} else if subs, ok := h.subscribers[model]; ok {
				delete(subs, ch)
				if len(subs) == 0 {
					delete(h.subscribers, model)
				}
			}
			close(ch)
		})
	}

	return ch, unsub
}

func (h *MonitorHub) Broadcast(ev MonitorPacketEvent) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for ch := range h.allSubs {
		select {
		case ch <- ev:
		default:
		}
	}

	if ev.Model != "" {
		if subs, ok := h.subscribers[ev.Model]; ok {
			for ch := range subs {
				select {
				case ch <- ev:
				default:
				}
			}
		}
	}
}
