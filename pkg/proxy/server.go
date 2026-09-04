package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"buffered-proxy/pkg/aggregator"
	"buffered-proxy/pkg/metrics"
)

type ServerConfig struct {
	UpstreamURL     *url.URL
	BufferConfig    aggregator.BufferConfig
	HTTPClient      *http.Client
	AllowMetricsAPI bool
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

func (s *ProxyServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.cfg.AllowMetricsAPI && (r.URL.Path == "/metrics" || r.URL.Path == "/debug/metrics") {
		s.handleMetrics(w, r)
		return
	}

	if r.Method == http.MethodPost && strings.TrimSuffix(r.URL.Path, "/") == "/v1/chat/completions" {
		s.handleChatCompletions(w, r)
		return
	}

	s.transparentProxy(w, r, nil)
}

func (s *ProxyServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	summary := map[string]interface{}{
		"upstream_sse_events":        s.totalMetrics.UpstreamSSEEvents,
		"downstream_sse_events":      s.totalMetrics.DownstreamSSEEvents,
		"overall_coalescing_ratio":   s.totalMetrics.OverallCoalescingRatio(),
		"reasoning_fragments_in":     s.totalMetrics.ReasoningFragmentsIn,
		"reasoning_events_out":       s.totalMetrics.ReasoningEventsOut,
		"reasoning_coalescing_ratio": s.totalMetrics.ReasoningCoalescingRatio(),
		"content_fragments_in":       s.totalMetrics.ContentFragmentsIn,
		"content_events_out":         s.totalMetrics.ContentEventsOut,
		"content_coalescing_ratio":   s.totalMetrics.ContentCoalescingRatio(),
		"tool_fragments_in":          s.totalMetrics.ToolArgumentFragmentsIn,
		"tool_events_out":            s.totalMetrics.ToolEventsOut,
		"tool_coalescing_ratio":      s.totalMetrics.ToolCoalescingRatio(),
		"upstream_bytes":             s.totalMetrics.UpstreamBytes,
		"downstream_bytes":           s.totalMetrics.DownstreamBytes,
		"pending_bytes_max":          s.totalMetrics.PendingBytesMax,
		"reader_pause_count":         s.totalMetrics.ReaderPauseCount,
		"reader_pause_duration_ns":   s.totalMetrics.ReaderPauseDurationNs,
		"downstream_write_ns":        s.totalMetrics.DownstreamWriteNs,
	}
	_ = json.NewEncoder(w).Encode(summary)
}

type streamCheckPayload struct {
	Stream bool `json:"stream"`
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

	_ = pipeline.ProcessStream(r.Context(), resp.Body, w)
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
