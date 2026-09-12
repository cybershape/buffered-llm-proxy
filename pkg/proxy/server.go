package proxy

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"buffered-proxy/pkg/aggregator"
	"buffered-proxy/pkg/compress"
	"buffered-proxy/pkg/metrics"
	"buffered-proxy/pkg/semantic"
)

//go:embed dashboard.html
var dashboardHTMLTemplate string

var dashboardTmpl = template.Must(template.New("dashboard").Parse(dashboardHTMLTemplate))

type UpstreamTarget struct {
	Name   string   `json:"name"`
	URL    *url.URL `json:"url"`
	APIKey string   `json:"api_key,omitempty"`
}

type EffectiveConfig struct {
	UpstreamURL        string           `json:"upstream_url"`
	Upstreams          []UpstreamTarget `json:"upstreams,omitempty"`
	HighWatermarkMB    int64            `json:"high_watermark_mb"`
	HighWatermarkBytes int64            `json:"high_watermark_bytes"`
	LowWatermarkMB     int64            `json:"low_watermark_mb"`
	LowWatermarkBytes  int64            `json:"low_watermark_bytes"`
	MinCoalesceWaitMs  int64            `json:"min_coalesce_wait_ms"`
	CompressionEnabled bool             `json:"compression_enabled"`
	MetricsAPIEnabled  bool             `json:"metrics_api_enabled"`
}

type ServerConfig struct {
	UpstreamURL        *url.URL
	Upstreams          []UpstreamTarget
	BufferConfig       aggregator.BufferConfig
	HTTPClient         *http.Client
	AllowMetricsAPI    bool
	DisableCompression bool
}

type cachedModelsResponse struct {
	statusCode int
	header     http.Header
	body       []byte
}

type LastRequestRecord struct {
	Model       string `json:"model"`
	TimestampMs int64  `json:"timestamp_ms"`
	DurationMs  int64  `json:"duration_ms"`
	StatusCode  int    `json:"status_code"`
	Stream      bool   `json:"stream"`
	Request     string `json:"request"`
	Response    string `json:"response"`
}

type responseRecorder struct {
	http.ResponseWriter
	buf       bytes.Buffer
	maxRecord int
}

func (rr *responseRecorder) Write(p []byte) (int, error) {
	if rr.buf.Len() < rr.maxRecord {
		avail := rr.maxRecord - rr.buf.Len()
		if len(p) <= avail {
			rr.buf.Write(p)
		} else {
			rr.buf.Write(p[:avail])
		}
	}
	return rr.ResponseWriter.Write(p)
}

func (rr *responseRecorder) Flush() {
	if f, ok := rr.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

type ProxyServer struct {
	cfg          ServerConfig
	client       *http.Client
	totalMetrics *metrics.StreamMetrics
	monitorHub   *MonitorHub
	sessionSeq   uint64
	upstreams    []UpstreamTarget
	upstreamMap  map[string]*UpstreamTarget

	modelsCache    atomic.Pointer[cachedModelsResponse]
	modelsUpdating atomic.Bool
	modelsInitMu   sync.Mutex

	lastReqMu sync.RWMutex
	lastReqs  map[string]*LastRequestRecord
}

func NewProxyServer(cfg ServerConfig) *ProxyServer {
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				DialContext: (&net.Dialer{
					Timeout:   30 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				ForceAttemptHTTP2:     true,
				MaxIdleConns:          200,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
				ResponseHeaderTimeout: 300 * time.Second,
			},
		}
	}

	var upstreams []UpstreamTarget
	upstreamMap := make(map[string]*UpstreamTarget)

	if len(cfg.Upstreams) > 0 {
		for i := range cfg.Upstreams {
			u := cfg.Upstreams[i]
			upstreams = append(upstreams, u)
			targetPtr := &upstreams[len(upstreams)-1]
			if u.Name != "" {
				upstreamMap[u.Name] = targetPtr
			}
		}
		if cfg.UpstreamURL == nil && len(upstreams) > 0 {
			cfg.UpstreamURL = upstreams[0].URL
		}
	} else if cfg.UpstreamURL != nil {
		target := UpstreamTarget{
			Name: "",
			URL:  cfg.UpstreamURL,
		}
		upstreams = append(upstreams, target)
		cfg.Upstreams = upstreams
		upstreamMap[""] = &upstreams[0]
	}

	return &ProxyServer{
		cfg:          cfg,
		client:       cfg.HTTPClient,
		totalMetrics: &metrics.StreamMetrics{},
		monitorHub:   NewMonitorHub(),
		upstreams:    upstreams,
		upstreamMap:  upstreamMap,
		lastReqs:     make(map[string]*LastRequestRecord),
	}
}

func (s *ProxyServer) MonitorHub() *MonitorHub {
	return s.monitorHub
}

func (s *ProxyServer) TotalMetrics() *metrics.StreamMetrics {
	return s.totalMetrics
}

func (s *ProxyServer) EffectiveConfig() EffectiveConfig {
	var upstreamStr string
	if len(s.upstreams) > 0 {
		var parts []string
		for _, u := range s.upstreams {
			if u.Name != "" {
				parts = append(parts, fmt.Sprintf("%s=%s", u.Name, u.URL.String()))
			} else {
				parts = append(parts, u.URL.String())
			}
		}
		upstreamStr = strings.Join(parts, ", ")
	} else if s.cfg.UpstreamURL != nil {
		upstreamStr = s.cfg.UpstreamURL.String()
	}
	hw := s.cfg.BufferConfig.HighWatermark
	lw := s.cfg.BufferConfig.LowWatermark
	return EffectiveConfig{
		UpstreamURL:        upstreamStr,
		Upstreams:          s.upstreams,
		HighWatermarkMB:    hw / (1024 * 1024),
		HighWatermarkBytes: hw,
		LowWatermarkMB:     lw / (1024 * 1024),
		LowWatermarkBytes:  lw,
		MinCoalesceWaitMs:  s.cfg.BufferConfig.MinCoalesceWait.Milliseconds(),
		CompressionEnabled: !s.cfg.DisableCompression,
		MetricsAPIEnabled:  s.cfg.AllowMetricsAPI,
	}
}

func (s *ProxyServer) resolveUpstream(model string) (*UpstreamTarget, string, error) {
	if prefix, targetModel, found := strings.Cut(model, "/"); found {
		if tgt, ok := s.upstreamMap[prefix]; ok {
			return tgt, targetModel, nil
		}
		return nil, "", fmt.Errorf("upstream %q not found for model %q", prefix, model)
	}

	if len(s.upstreams) == 1 {
		return &s.upstreams[0], model, nil
	}
	if tgt, ok := s.upstreamMap["default"]; ok {
		return tgt, model, nil
	}
	if tgt, ok := s.upstreamMap[""]; ok {
		return tgt, model, nil
	}
	if tgt, ok := s.upstreamMap[model]; ok {
		return tgt, model, nil
	}

	return nil, "", fmt.Errorf("model %q does not specify upstream prefix (expected <upstream>/<model>)", model)
}

func applyAuthHeader(h http.Header, apiKey string) {
	if apiKey != "" {
		auth := apiKey
		if !strings.HasPrefix(strings.ToLower(auth), "bearer ") {
			auth = "Bearer " + auth
		}
		h.Set("Authorization", auth)
	}
}

func rewriteModelInBody(bodyBytes []byte, targetModel string) []byte {
	var rawMap map[string]json.RawMessage
	if err := json.Unmarshal(bodyBytes, &rawMap); err != nil {
		return bodyBytes
	}
	targetModelJSON, err := json.Marshal(targetModel)
	if err != nil {
		return bodyBytes
	}
	rawMap["model"] = targetModelJSON
	modified, err := json.Marshal(rawMap)
	if err != nil {
		return bodyBytes
	}
	return modified
}

func (s *ProxyServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !s.cfg.DisableCompression {
		cw, cleanup := compress.WrapResponseWriter(w, r, s.totalMetrics)
		defer cleanup()
		w = cw
	}

	cleanPath := strings.TrimSuffix(r.URL.Path, "/")
	if s.cfg.AllowMetricsAPI && (cleanPath == "/metrics" || cleanPath == "/dashboard" || cleanPath == "/monitor" || cleanPath == "/last_req" || cleanPath == "/last-request") {
		if cleanPath == "/dashboard" {
			s.handleDashboard(w, r)
			return
		}
		if cleanPath == "/monitor" {
			s.handleMonitor(w, r)
			return
		}
		if cleanPath == "/last_req" || cleanPath == "/last-request" {
			s.handleLastReq(w, r)
			return
		}
		s.handleMetrics(w, r)
		return
	}

	if cleanPath == "/v1/chat/completions" {
		if r.Method == http.MethodPost {
			s.handleChatCompletions(w, r)
			return
		}
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if cleanPath == "/v1/responses" {
		if r.Method == http.MethodPost {
			s.handleResponses(w, r)
			return
		}
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if cleanPath == "/v1/models" && r.Method == http.MethodGet {
		s.handleModels(w, r)
		return
	}

	http.NotFound(w, r)
}

type MetricsResponse struct {
	UpstreamSSEEvents      int64   `json:"upstream_sse_events"`
	DownstreamSSEEvents    int64   `json:"downstream_sse_events"`
	OverallCoalescingRatio float64 `json:"overall_coalescing_ratio"`

	ReasoningFragmentsIn     int64   `json:"reasoning_fragments_in"`
	ReasoningEventsOut       int64   `json:"reasoning_events_out"`
	ReasoningCoalescingRatio float64 `json:"reasoning_coalescing_ratio"`

	ContentFragmentsIn     int64   `json:"content_fragments_in"`
	ContentEventsOut       int64   `json:"content_events_out"`
	ContentCoalescingRatio float64 `json:"content_coalescing_ratio"`

	ToolFragmentsIn     int64   `json:"tool_fragments_in"`
	ToolEventsOut       int64   `json:"tool_events_out"`
	ToolCoalescingRatio float64 `json:"tool_coalescing_ratio"`

	UpstreamBytes                  int64   `json:"upstream_bytes"`
	DownstreamBytes                int64   `json:"downstream_bytes"`
	CompressionUncompressedBytes   int64   `json:"compression_uncompressed_bytes"`
	CompressionCompressedBytes     int64   `json:"compression_compressed_bytes"`
	CompressionRatio               float64 `json:"compression_ratio"`
	CompressionSavingsRatio        float64 `json:"compression_savings_ratio"`
	CompressionSavingsRatioPercent string  `json:"compression_savings_ratio_percent"`

	PendingBytesMax       int64                                  `json:"pending_bytes_max"`
	ReaderPauseCount      int64                                  `json:"reader_pause_count"`
	ReaderPauseDurationNs int64                                  `json:"reader_pause_duration_ns"`
	DownstreamWriteNs     int64                                  `json:"downstream_write_ns"`
	Models                map[string]metrics.ModelMetricSnapshot `json:"models"`
}

func formatRatioPercent(r float64) string {
	return fmt.Sprintf("%.2f%%", r*100)
}

func (s *ProxyServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	modelsSnap := s.totalMetrics.ModelSnapshots()
	if modelsSnap == nil {
		modelsSnap = make(map[string]metrics.ModelMetricSnapshot)
	}

	summary := MetricsResponse{
		UpstreamSSEEvents:      s.totalMetrics.UpstreamSSEEvents,
		DownstreamSSEEvents:    s.totalMetrics.DownstreamSSEEvents,
		OverallCoalescingRatio: s.totalMetrics.OverallCoalescingRatio(),

		ReasoningFragmentsIn:     s.totalMetrics.ReasoningFragmentsIn,
		ReasoningEventsOut:       s.totalMetrics.ReasoningEventsOut,
		ReasoningCoalescingRatio: s.totalMetrics.ReasoningCoalescingRatio(),

		ContentFragmentsIn:     s.totalMetrics.ContentFragmentsIn,
		ContentEventsOut:       s.totalMetrics.ContentEventsOut,
		ContentCoalescingRatio: s.totalMetrics.ContentCoalescingRatio(),

		ToolFragmentsIn:     s.totalMetrics.ToolArgumentFragmentsIn,
		ToolEventsOut:       s.totalMetrics.ToolEventsOut,
		ToolCoalescingRatio: s.totalMetrics.ToolCoalescingRatio(),

		UpstreamBytes:                  s.totalMetrics.UpstreamBytes,
		DownstreamBytes:                s.totalMetrics.DownstreamBytes,
		CompressionUncompressedBytes:   s.totalMetrics.CompressionUncompressedBytes,
		CompressionCompressedBytes:     s.totalMetrics.CompressionCompressedBytes,
		CompressionRatio:               s.totalMetrics.CompressionRatio(),
		CompressionSavingsRatio:        s.totalMetrics.CompressionSavingsRatio(),
		CompressionSavingsRatioPercent: formatRatioPercent(s.totalMetrics.CompressionSavingsRatio()),

		PendingBytesMax:       s.totalMetrics.PendingBytesMax,
		ReaderPauseCount:      s.totalMetrics.ReaderPauseCount,
		ReaderPauseDurationNs: s.totalMetrics.ReaderPauseDurationNs,
		DownstreamWriteNs:     s.totalMetrics.DownstreamWriteNs,
		Models:                modelsSnap,
	}
	_ = json.NewEncoder(w).Encode(summary)
}

type dashboardViewData struct {
	ConfigJSON template.JS
}

func (s *ProxyServer) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	cfgJSON, _ := json.Marshal(s.EffectiveConfig())
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}
	_ = dashboardTmpl.Execute(w, dashboardViewData{
		ConfigJSON: template.JS(cfgJSON),
	})
}

func (s *ProxyServer) recordLastReq(rec *LastRequestRecord) {
	if rec == nil || rec.Model == "" {
		return
	}
	s.lastReqMu.Lock()
	defer s.lastReqMu.Unlock()
	if s.lastReqs == nil {
		s.lastReqs = make(map[string]*LastRequestRecord)
	}
	s.lastReqs[rec.Model] = rec
}

func (s *ProxyServer) getLastReq(model string) *LastRequestRecord {
	s.lastReqMu.RLock()
	defer s.lastReqMu.RUnlock()
	if s.lastReqs == nil {
		return nil
	}
	return s.lastReqs[model]
}

func (s *ProxyServer) GetLastRequest(model string) *LastRequestRecord {
	return s.getLastReq(model)
}

func (s *ProxyServer) handleLastReq(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	model := r.URL.Query().Get("model")
	w.Header().Set("Content-Type", "application/json")

	if model == "" {
		s.lastReqMu.RLock()
		all := make(map[string]*LastRequestRecord, len(s.lastReqs))
		for k, v := range s.lastReqs {
			all[k] = v
		}
		s.lastReqMu.RUnlock()
		_ = json.NewEncoder(w).Encode(all)
		return
	}

	rec := s.getLastReq(model)
	if rec == nil {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"error": fmt.Sprintf("no request recorded for model: %s", model),
		})
		return
	}

	_ = json.NewEncoder(w).Encode(rec)
}

type streamCheckPayload struct {
	Stream bool   `json:"stream"`
	Model  string `json:"model"`
}

func (s *ProxyServer) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	s.handleStreamingEndpoint(w, r, semantic.ProtocolChatCompletions)
}

func (s *ProxyServer) handleResponses(w http.ResponseWriter, r *http.Request) {
	s.handleStreamingEndpoint(w, r, semantic.ProtocolResponses)
}

func (s *ProxyServer) handleStreamingEndpoint(w http.ResponseWriter, r *http.Request, proto semantic.Protocol) {
	reqStartTime := time.Now()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to read body: %v", err), http.StatusBadRequest)
		return
	}
	_ = r.Body.Close()

	var payload streamCheckPayload
	_ = json.Unmarshal(bodyBytes, &payload)

	sessionID := fmt.Sprintf("sess_%d_%04d", time.Now().UnixMilli(), atomic.AddUint64(&s.sessionSeq, 1)%10000)

	if s.monitorHub.HasSubscribers(payload.Model) {
		s.monitorHub.Broadcast(MonitorPacketEvent{
			SessionID:   sessionID,
			Model:       payload.Model,
			TimestampMs: time.Now().UnixMilli(),
			Direction:   DirectionUpstream,
			PacketType:  "request",
			Payload:     string(bodyBytes),
			Summary:     "Client Request",
		})
	}

	target, targetModel, err := s.resolveUpstream(payload.Model)
	if err != nil {
		errResp := map[string]interface{}{
			"error": map[string]interface{}{
				"message": err.Error(),
				"type":    "invalid_request_error",
				"param":   "model",
				"code":    "model_not_found",
			},
		}
		errBytes, _ := json.Marshal(errResp)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write(errBytes)
		s.recordLastReq(&LastRequestRecord{
			Model:       payload.Model,
			TimestampMs: reqStartTime.UnixMilli(),
			DurationMs:  time.Since(reqStartTime).Milliseconds(),
			StatusCode:  http.StatusBadRequest,
			Stream:      payload.Stream,
			Request:     string(bodyBytes),
			Response:    string(errBytes),
		})
		return
	}

	upstreamBodyBytes := bodyBytes
	if targetModel != "" && targetModel != payload.Model {
		upstreamBodyBytes = rewriteModelInBody(bodyBytes, targetModel)
	}

	if !payload.Stream {
		s.transparentProxy(w, r, target, upstreamBodyBytes, sessionID, payload.Model, reqStartTime, bodyBytes)
		return
	}

	destURL := *target.URL
	destURL.Path = singleJoiningSlash(destURL.Path, r.URL.Path)
	destURL.RawQuery = r.URL.RawQuery

	req, err := http.NewRequestWithContext(r.Context(), r.Method, destURL.String(), bytes.NewReader(upstreamBodyBytes))
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to create upstream request: %v", err), http.StatusInternalServerError)
		return
	}

	copyHeaders(req.Header, r.Header)
	applyAuthHeader(req.Header, target.APIKey)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Del("Accept-Encoding")

	resp, err := s.client.Do(req)
	if err != nil {
		http.Error(w, fmt.Sprintf("upstream error: %v", err), http.StatusBadGateway)
		s.recordLastReq(&LastRequestRecord{
			Model:       payload.Model,
			TimestampMs: reqStartTime.UnixMilli(),
			DurationMs:  time.Since(reqStartTime).Milliseconds(),
			StatusCode:  http.StatusBadGateway,
			Stream:      true,
			Request:     string(bodyBytes),
			Response:    fmt.Sprintf("upstream error: %v", err),
		})
		return
	}
	defer resp.Body.Close()

	cType := resp.Header.Get("Content-Type")
	if resp.StatusCode != http.StatusOK || !strings.Contains(cType, "text/event-stream") {
		copyHeaders(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)
		respBodyBytes, _ := io.ReadAll(resp.Body)
		_, _ = w.Write(respBodyBytes)
		s.recordLastReq(&LastRequestRecord{
			Model:       payload.Model,
			TimestampMs: reqStartTime.UnixMilli(),
			DurationMs:  time.Since(reqStartTime).Milliseconds(),
			StatusCode:  resp.StatusCode,
			Stream:      true,
			Request:     string(bodyBytes),
			Response:    string(respBodyBytes),
		})
		return
	}

	copyHeaders(w.Header(), resp.Header)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Del("Content-Length")

	w.WriteHeader(http.StatusOK)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}

	reqMetrics := s.totalMetrics
	pipeline := aggregator.NewStreamPipeline(s.cfg.BufferConfig, reqMetrics)
	pipeline.SetProtocol(proto)
	pipeline.SetRequestInfo(payload.Model, reqStartTime)
	pipeline.SetModelOverride(payload.Model)

	pipeline.SetPacketCallback(func(direction string, packetType string, data []byte) {
		if !s.monitorHub.HasSubscribers(payload.Model) {
			return
		}
		cleanPayload := extractSSEPayload(data)
		if len(cleanPayload) == 0 {
			return
		}
		s.monitorHub.Broadcast(MonitorPacketEvent{
			SessionID:   sessionID,
			Model:       payload.Model,
			TimestampMs: time.Now().UnixMilli(),
			Direction:   DirectionDownstream,
			PacketType:  "downstream_chunk",
			Payload:     cleanPayload,
			Summary:     "Downstream Coalesced Packet",
		})
	})

	defer func() {
		if s.monitorHub.HasSubscribers(payload.Model) {
			s.monitorHub.Broadcast(MonitorPacketEvent{
				SessionID:   sessionID,
				Model:       payload.Model,
				TimestampMs: time.Now().UnixMilli(),
				Direction:   DirectionDownstream,
				PacketType:  "session_end",
				Payload:     `{"status":"completed"}`,
				Summary:     "Connection Closed",
			})
		}
	}()

	recWriter := &responseRecorder{
		ResponseWriter: w,
		maxRecord:      4 * 1024 * 1024,
	}

	_ = pipeline.ProcessStream(r.Context(), resp.Body, recWriter)

	s.recordLastReq(&LastRequestRecord{
		Model:       payload.Model,
		TimestampMs: reqStartTime.UnixMilli(),
		DurationMs:  time.Since(reqStartTime).Milliseconds(),
		StatusCode:  resp.StatusCode,
		Stream:      true,
		Request:     string(bodyBytes),
		Response:    recWriter.buf.String(),
	})
}

func (s *ProxyServer) handleModels(w http.ResponseWriter, r *http.Request) {
	cached := s.modelsCache.Load()
	if cached == nil {
		s.modelsInitMu.Lock()
		cached = s.modelsCache.Load()
		if cached == nil {
			res, err := s.fetchModelsData(r.Context(), r.Header, r.URL.RawQuery)
			if err != nil {
				s.modelsInitMu.Unlock()
				http.Error(w, fmt.Sprintf("upstream error: %v", err), http.StatusBadGateway)
				return
			}
			if res.statusCode == http.StatusOK {
				s.modelsCache.Store(res)
			}
			s.modelsInitMu.Unlock()
			s.writeModelsResponse(w, res)
			return
		}
		s.modelsInitMu.Unlock()
	}

	s.writeModelsResponse(w, cached)

	if s.modelsUpdating.CompareAndSwap(false, true) {
		headerCopy := r.Header.Clone()
		rawQuery := r.URL.RawQuery
		go func() {
			defer s.modelsUpdating.Store(false)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			newRes, err := s.fetchModelsData(ctx, headerCopy, rawQuery)
			if err == nil && newRes != nil && newRes.statusCode == http.StatusOK {
				s.modelsCache.Store(newRes)
			}
		}()
	}
}

func (s *ProxyServer) writeModelsResponse(w http.ResponseWriter, resp *cachedModelsResponse) {
	copyHeaders(w.Header(), resp.header)
	w.Header().Del("Content-Length")
	w.WriteHeader(resp.statusCode)
	_, _ = w.Write(resp.body)
}

func (s *ProxyServer) fetchModelsData(ctx context.Context, origHeader http.Header, rawQuery string) (*cachedModelsResponse, error) {
	if (len(s.upstreams) == 1 && s.upstreams[0].Name == "") || (len(s.upstreams) == 0 && s.cfg.UpstreamURL != nil) {
		var target *UpstreamTarget
		if len(s.upstreams) > 0 {
			target = &s.upstreams[0]
		} else {
			target = &UpstreamTarget{URL: s.cfg.UpstreamURL}
		}
		return s.fetchSingleModelData(ctx, origHeader, rawQuery, target)
	}

	if len(s.upstreams) == 0 {
		return nil, fmt.Errorf("no upstream configured")
	}

	return s.fetchMultiModelsData(ctx, origHeader, rawQuery)
}

func (s *ProxyServer) fetchSingleModelData(ctx context.Context, origHeader http.Header, rawQuery string, target *UpstreamTarget) (*cachedModelsResponse, error) {
	destURL := *target.URL
	destURL.Path = singleJoiningSlash(destURL.Path, "/v1/models")
	destURL.RawQuery = rawQuery

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, destURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create upstream request: %w", err)
	}

	copyHeaders(req.Header, origHeader)
	req.Header.Set("User-Agent", "grok-shell")
	applyAuthHeader(req.Header, target.APIKey)

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("upstream error: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read upstream response: %w", err)
	}

	cType := resp.Header.Get("Content-Type")
	if resp.StatusCode == http.StatusOK && strings.Contains(cType, "application/json") {
		var root interface{}
		if json.Unmarshal(bodyBytes, &root) == nil {
			if injectContextLength(root) {
				if modified, encErr := json.Marshal(root); encErr == nil {
					bodyBytes = modified
				}
			}
		}
	}

	hdr := make(http.Header)
	copyHeaders(hdr, resp.Header)

	return &cachedModelsResponse{
		statusCode: resp.StatusCode,
		header:     hdr,
		body:       bodyBytes,
	}, nil
}

func (s *ProxyServer) fetchMultiModelsData(ctx context.Context, origHeader http.Header, rawQuery string) (*cachedModelsResponse, error) {
	type fetchResult struct {
		idx    int
		models []interface{}
		err    error
	}

	results := make([]fetchResult, len(s.upstreams))
	var wg sync.WaitGroup
	for i, target := range s.upstreams {
		wg.Add(1)
		go func(idx int, tgt UpstreamTarget) {
			defer wg.Done()
			mList, err := s.fetchUpstreamModels(ctx, origHeader, rawQuery, tgt)
			results[idx] = fetchResult{idx: idx, models: mList, err: err}
		}(i, target)
	}
	wg.Wait()

	var allModels []interface{}
	var lastErr error
	succeeded := 0
	for _, res := range results {
		if res.err != nil {
			lastErr = res.err
			continue
		}
		succeeded++
		allModels = append(allModels, res.models...)
	}

	if succeeded == 0 && lastErr != nil {
		return nil, fmt.Errorf("upstream error: %w", lastErr)
	}

	if allModels == nil {
		allModels = []interface{}{}
	}

	respMap := map[string]interface{}{
		"object": "list",
		"data":   allModels,
	}

	respBytes, err := json.Marshal(respMap)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal models: %w", err)
	}

	hdr := make(http.Header)
	hdr.Set("Content-Type", "application/json")

	return &cachedModelsResponse{
		statusCode: http.StatusOK,
		header:     hdr,
		body:       respBytes,
	}, nil
}

func (s *ProxyServer) fetchUpstreamModels(ctx context.Context, origHeader http.Header, rawQuery string, tgt UpstreamTarget) ([]interface{}, error) {
	destURL := *tgt.URL
	destURL.Path = singleJoiningSlash(destURL.Path, "/v1/models")
	destURL.RawQuery = rawQuery

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, destURL.String(), nil)
	if err != nil {
		return nil, err
	}

	copyHeaders(req.Header, origHeader)
	req.Header.Set("User-Agent", "grok-shell")
	applyAuthHeader(req.Header, tgt.APIKey)

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("upstream %s returned status %d", tgt.Name, resp.StatusCode)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var root interface{}
	if err := json.Unmarshal(bodyBytes, &root); err != nil {
		return nil, err
	}

	injectContextLength(root)

	var rawList []interface{}
	switch val := root.(type) {
	case map[string]interface{}:
		if data, ok := val["data"].([]interface{}); ok {
			rawList = data
		}
	case []interface{}:
		rawList = val
	}

	var resultList []interface{}
	for _, item := range rawList {
		if itemMap, ok := item.(map[string]interface{}); ok {
			if tgt.Name != "" {
				if idStr, ok := itemMap["id"].(string); ok {
					itemMap["id"] = tgt.Name + "/" + idStr
				}
				if nameStr, ok := itemMap["name"].(string); ok {
					itemMap["name"] = tgt.Name + "/" + nameStr
				}
			}
			resultList = append(resultList, itemMap)
		}
	}

	return resultList, nil
}

func injectContextLength(v interface{}) bool {
	modified := false
	switch val := v.(type) {
	case map[string]interface{}:
		if cw, exists := val["context_window"]; exists {
			val["context_length"] = cw
			modified = true
		}
		for _, item := range val {
			if injectContextLength(item) {
				modified = true
			}
		}
	case []interface{}:
		for _, item := range val {
			if injectContextLength(item) {
				modified = true
			}
		}
	}
	return modified
}

func (s *ProxyServer) transparentProxy(w http.ResponseWriter, r *http.Request, target *UpstreamTarget, preloadedBody []byte, sessionID string, model string, reqStartTime time.Time, origRequestBody []byte) {
	if target == nil {
		if len(s.upstreams) > 0 {
			target = &s.upstreams[0]
		} else if s.cfg.UpstreamURL != nil {
			target = &UpstreamTarget{URL: s.cfg.UpstreamURL}
		} else {
			http.Error(w, "no upstream configured", http.StatusBadGateway)
			return
		}
	}

	destURL := *target.URL
	destURL.Path = singleJoiningSlash(destURL.Path, r.URL.Path)
	destURL.RawQuery = r.URL.RawQuery

	var bodyReader io.Reader
	if preloadedBody != nil {
		bodyReader = bytes.NewReader(preloadedBody)
	} else if r.Body != nil {
		bodyReader = r.Body
		defer r.Body.Close()
	}

	req, err := http.NewRequestWithContext(r.Context(), r.Method, destURL.String(), bodyReader)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to create upstream request: %v", err), http.StatusInternalServerError)
		return
	}

	copyHeaders(req.Header, r.Header)
	applyAuthHeader(req.Header, target.APIKey)

	resp, err := s.client.Do(req)
	if err != nil {
		http.Error(w, fmt.Sprintf("upstream error: %v", err), http.StatusBadGateway)
		reqStr := string(origRequestBody)
		if reqStr == "" {
			reqStr = string(preloadedBody)
		}
		s.recordLastReq(&LastRequestRecord{
			Model:       model,
			TimestampMs: reqStartTime.UnixMilli(),
			DurationMs:  time.Since(reqStartTime).Milliseconds(),
			StatusCode:  http.StatusBadGateway,
			Stream:      false,
			Request:     reqStr,
			Response:    fmt.Sprintf("upstream error: %v", err),
		})
		return
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to read upstream response: %v", err), http.StatusBadGateway)
		return
	}

	if model != "" && strings.Contains(resp.Header.Get("Content-Type"), "application/json") {
		var rawMap map[string]json.RawMessage
		if err := json.Unmarshal(respBytes, &rawMap); err == nil {
			if _, ok := rawMap["model"]; ok {
				if modelJSON, err := json.Marshal(model); err == nil {
					rawMap["model"] = modelJSON
					if modified, err := json.Marshal(rawMap); err == nil {
						respBytes = modified
					}
				}
			}
		}
	}

	if sessionID != "" && s.monitorHub.HasSubscribers(model) {
		s.monitorHub.Broadcast(MonitorPacketEvent{
			SessionID:   sessionID,
			Model:       model,
			TimestampMs: time.Now().UnixMilli(),
			Direction:   DirectionDownstream,
			PacketType:  "response",
			Payload:     string(respBytes),
			Summary:     "Non-Streaming Response",
		})
		s.monitorHub.Broadcast(MonitorPacketEvent{
			SessionID:   sessionID,
			Model:       model,
			TimestampMs: time.Now().UnixMilli(),
			Direction:   DirectionDownstream,
			PacketType:  "session_end",
			Payload:     `{"status":"completed"}`,
			Summary:     "Connection Closed",
		})
	}

	copyHeaders(w.Header(), resp.Header)
	w.Header().Del("Content-Length")
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(respBytes)

	reqStr := string(origRequestBody)
	if reqStr == "" {
		reqStr = string(preloadedBody)
	}
	s.recordLastReq(&LastRequestRecord{
		Model:       model,
		TimestampMs: reqStartTime.UnixMilli(),
		DurationMs:  time.Since(reqStartTime).Milliseconds(),
		StatusCode:  resp.StatusCode,
		Stream:      false,
		Request:     reqStr,
		Response:    string(respBytes),
	})
}

func extractSSEPayload(data []byte) string {
	trimmed := bytes.TrimSpace(data)
	if idx := bytes.Index(trimmed, []byte("data:")); idx != -1 {
		after := trimmed[idx+len("data:"):]
		lines := bytes.Split(after, []byte("\n"))
		var buf bytes.Buffer
		for i, line := range lines {
			line = bytes.TrimSpace(line)
			if bytes.HasPrefix(line, []byte("data:")) {
				line = bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
			}
			if i > 0 && buf.Len() > 0 && len(line) > 0 {
				buf.WriteByte(' ')
			}
			buf.Write(line)
		}
		return buf.String()
	}
	return string(trimmed)
}

func (s *ProxyServer) handleMonitor(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodOptions {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming not supported", http.StatusInternalServerError)
		return
	}

	model := r.URL.Query().Get("model")

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ch, unsub := s.monitorHub.Subscribe(model)
	defer unsub()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return
			}
			data, err := json.Marshal(ev)
			if err != nil {
				continue
			}
			if _, writeErr := fmt.Fprintf(w, "data: %s\n\n", data); writeErr != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func copyHeaders(dst, src http.Header) {
	for k, vv := range src {
		if isHopByHop(k) {
			continue
		}
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

func isHopByHop(header string) bool {
	switch strings.ToLower(header) {
	case "connection",
		"keep-alive",
		"proxy-authenticate",
		"proxy-authorization",
		"te",
		"trailers",
		"transfer-encoding",
		"upgrade":
		return true
	default:
		return false
	}
}

func singleJoiningSlash(a, b string) string {
	aslash := strings.HasSuffix(a, "/")
	bslash := strings.HasPrefix(b, "/")
	switch {
	case aslash && bslash:
		return a + b[1:]
	case !aslash && !bslash:
		return a + "/" + b
	}
	return a + b
}

func ContextWithTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, d)
}
