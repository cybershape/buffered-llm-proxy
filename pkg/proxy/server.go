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
	"time"

	"buffered-proxy/pkg/aggregator"
	"buffered-proxy/pkg/compress"
	"buffered-proxy/pkg/metrics"
)

//go:embed dashboard.html
var dashboardHTMLTemplate string

var dashboardTmpl = template.Must(template.New("dashboard").Parse(dashboardHTMLTemplate))

type EffectiveConfig struct {
	UpstreamURL        string `json:"upstream_url"`
	HighWatermarkMB    int64  `json:"high_watermark_mb"`
	HighWatermarkBytes int64  `json:"high_watermark_bytes"`
	LowWatermarkMB     int64  `json:"low_watermark_mb"`
	LowWatermarkBytes  int64  `json:"low_watermark_bytes"`
	MinCoalesceWaitMs  int64  `json:"min_coalesce_wait_ms"`
	CompressionEnabled bool   `json:"compression_enabled"`
	MetricsAPIEnabled  bool   `json:"metrics_api_enabled"`
}

type ServerConfig struct {
	UpstreamURL        *url.URL
	BufferConfig       aggregator.BufferConfig
	HTTPClient         *http.Client
	AllowMetricsAPI    bool
	DisableCompression bool
}

type ProxyServer struct {
	cfg          ServerConfig
	client       *http.Client
	totalMetrics *metrics.StreamMetrics
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
	return &ProxyServer{
		cfg:          cfg,
		client:       cfg.HTTPClient,
		totalMetrics: &metrics.StreamMetrics{},
	}
}

func (s *ProxyServer) TotalMetrics() *metrics.StreamMetrics {
	return s.totalMetrics
}

func (s *ProxyServer) EffectiveConfig() EffectiveConfig {
	var upstreamStr string
	if s.cfg.UpstreamURL != nil {
		upstreamStr = s.cfg.UpstreamURL.String()
	}
	hw := s.cfg.BufferConfig.HighWatermark
	lw := s.cfg.BufferConfig.LowWatermark
	return EffectiveConfig{
		UpstreamURL:        upstreamStr,
		HighWatermarkMB:    hw / (1024 * 1024),
		HighWatermarkBytes: hw,
		LowWatermarkMB:     lw / (1024 * 1024),
		LowWatermarkBytes:  lw,
		MinCoalesceWaitMs:  s.cfg.BufferConfig.MinCoalesceWait.Milliseconds(),
		CompressionEnabled: !s.cfg.DisableCompression,
		MetricsAPIEnabled:  s.cfg.AllowMetricsAPI,
	}
}

func (s *ProxyServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !s.cfg.DisableCompression {
		cw, cleanup := compress.WrapResponseWriter(w, r, s.totalMetrics)
		defer cleanup()
		w = cw
	}

	cleanPath := strings.TrimSuffix(r.URL.Path, "/")
	if s.cfg.AllowMetricsAPI && (cleanPath == "/metrics" || cleanPath == "/dashboard") {
		if cleanPath == "/dashboard" {
			s.handleDashboard(w, r)
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

type streamCheckPayload struct {
	Stream bool   `json:"stream"`
	Model  string `json:"model"`
}

func (s *ProxyServer) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to read body: %v", err), http.StatusBadRequest)
		return
	}
	_ = r.Body.Close()

	var payload streamCheckPayload
	_ = json.Unmarshal(bodyBytes, &payload)

	if !payload.Stream {
		s.transparentProxy(w, r, bodyBytes)
		return
	}

	targetURL := *s.cfg.UpstreamURL
	targetURL.Path = singleJoiningSlash(targetURL.Path, r.URL.Path)
	targetURL.RawQuery = r.URL.RawQuery

	req, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL.String(), bytes.NewReader(bodyBytes))
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to create upstream request: %v", err), http.StatusInternalServerError)
		return
	}

	copyHeaders(req.Header, r.Header)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Del("Accept-Encoding")

	reqStartTime := time.Now()
	resp, err := s.client.Do(req)
	if err != nil {
		http.Error(w, fmt.Sprintf("upstream error: %v", err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	cType := resp.Header.Get("Content-Type")
	if resp.StatusCode != http.StatusOK || !strings.Contains(cType, "text/event-stream") {
		copyHeaders(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
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
	pipeline.SetRequestInfo(payload.Model, reqStartTime)

	_ = pipeline.ProcessStream(r.Context(), resp.Body, w)
}

func (s *ProxyServer) handleModels(w http.ResponseWriter, r *http.Request) {
	targetURL := *s.cfg.UpstreamURL
	targetURL.Path = singleJoiningSlash(targetURL.Path, r.URL.Path)
	targetURL.RawQuery = r.URL.RawQuery

	req, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL.String(), nil)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to create upstream request: %v", err), http.StatusInternalServerError)
		return
	}

	copyHeaders(req.Header, r.Header)
	req.Header.Set("User-Agent", "grok-shell")

	resp, err := s.client.Do(req)
	if err != nil {
		http.Error(w, fmt.Sprintf("upstream error: %v", err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to read upstream response: %v", err), http.StatusBadGateway)
		return
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

	copyHeaders(w.Header(), resp.Header)
	w.Header().Del("Content-Length")
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(bodyBytes)
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

func (s *ProxyServer) transparentProxy(w http.ResponseWriter, r *http.Request, preloadedBody []byte) {
	targetURL := *s.cfg.UpstreamURL
	targetURL.Path = singleJoiningSlash(targetURL.Path, r.URL.Path)
	targetURL.RawQuery = r.URL.RawQuery

	var bodyReader io.Reader
	if preloadedBody != nil {
		bodyReader = bytes.NewReader(preloadedBody)
	} else if r.Body != nil {
		bodyReader = r.Body
		defer r.Body.Close()
	}

	req, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL.String(), bodyReader)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to create upstream request: %v", err), http.StatusInternalServerError)
		return
	}

	copyHeaders(req.Header, r.Header)

	resp, err := s.client.Do(req)
	if err != nil {
		http.Error(w, fmt.Sprintf("upstream error: %v", err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
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
