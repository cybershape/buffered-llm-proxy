package proxy

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"buffered-proxy/pkg/aggregator"
	"buffered-proxy/pkg/sse"

	"github.com/klauspost/compress/zstd"
)

func TestProxyModelsTransparent(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/models" {
			t.Errorf("unexpected upstream req: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"gpt-4o"}]}`))
	}))
	defer upstream.Close()

	uURL, _ := url.Parse(upstream.URL)
	proxySrv := NewProxyServer(ServerConfig{
		UpstreamURL:  uURL,
		BufferConfig: aggregator.DefaultBufferConfig(),
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	proxySrv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "gpt-4o") {
		t.Fatalf("expected gpt-4o in body, got: %s", body)
	}
}

func TestProxyModelsContextWindowAndUserAgent(t *testing.T) {
	var capturedUA string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedUA = r.Header.Get("User-Agent")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"model-1","context_window":128000},{"id":"model-2","extra":{"context_window":32768}}]}`))
	}))
	defer upstream.Close()

	uURL, _ := url.Parse(upstream.URL)
	proxySrv := NewProxyServer(ServerConfig{
		UpstreamURL:        uURL,
		BufferConfig:       aggregator.DefaultBufferConfig(),
		DisableCompression: true,
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	proxySrv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	if capturedUA != "grok-shell" {
		t.Fatalf("expected upstream User-Agent 'grok-shell', got %q", capturedUA)
	}

	var respMap map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &respMap); err != nil {
		t.Fatalf("failed to unmarshal models response: %v", err)
	}

	dataList := respMap["data"].([]interface{})
	m1 := dataList[0].(map[string]interface{})
	if m1["context_window"] != float64(128000) {
		t.Fatalf("expected context_window 128000, got %v", m1["context_window"])
	}
	if m1["context_length"] != float64(128000) {
		t.Fatalf("expected context_length 128000, got %v", m1["context_length"])
	}

	m2 := dataList[1].(map[string]interface{})
	extra := m2["extra"].(map[string]interface{})
	if extra["context_window"] != float64(32768) {
		t.Fatalf("expected extra.context_window 32768, got %v", extra["context_window"])
	}
	if extra["context_length"] != float64(32768) {
		t.Fatalf("expected extra.context_length 32768, got %v", extra["context_length"])
	}
}

func TestProxyCompletionsEndpointNotSupported(t *testing.T) {
	uURL, _ := url.Parse("http://127.0.0.1:8000")
	proxySrv := NewProxyServer(ServerConfig{
		UpstreamURL:  uURL,
		BufferConfig: aggregator.DefaultBufferConfig(),
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"prompt":"hi"}`))
	proxySrv.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected status 404 for /v1/completions, got %d", rec.Code)
	}
}

func TestProxyChatStreamFalseTransparent(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/chat/completions" {
			t.Errorf("unexpected upstream req: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"chat-1","choices":[{"message":{"role":"assistant","content":"direct reply"}}]}`))
	}))
	defer upstream.Close()

	uURL, _ := url.Parse(upstream.URL)
	proxySrv := NewProxyServer(ServerConfig{
		UpstreamURL:  uURL,
		BufferConfig: aggregator.DefaultBufferConfig(),
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"stream":false,"messages":[{"role":"user","content":"hi"}]}`))
	proxySrv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "direct reply") {
		t.Fatalf("expected direct reply, got: %s", body)
	}
}

func TestProxyChatStreamTrueAggregated(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)

		chunks := []string{
			"data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"}}]}\n\n",
			"data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"A\"}}]}\n\n",
			"data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"B\"}}]}\n\n",
			"data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"C\"}}]}\n\n",
			"data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
			"data: [DONE]\n\n",
		}
		for _, chunk := range chunks {
			_, _ = w.Write([]byte(chunk))
			flusher.Flush()
		}
	}))
	defer upstream.Close()

	uURL, _ := url.Parse(upstream.URL)
	proxySrv := NewProxyServer(ServerConfig{
		UpstreamURL:     uURL,
		BufferConfig:    aggregator.DefaultBufferConfig(),
		AllowMetricsAPI: true,
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	proxySrv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	res := rec.Body.Bytes()
	r := sse.NewReader(bytes.NewReader(res))
	var receivedContents []string
	var hasRole bool
	var hasStop bool
	var hasDone bool

	for {
		ev, err := r.ReadEvent()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read event err: %v", err)
		}
		str := string(ev.Data)
		if str == "[DONE]" {
			hasDone = true
			continue
		}
		if strings.Contains(str, `"role":"assistant"`) {
			hasRole = true
		}
		if strings.Contains(str, `"content":`) {
			receivedContents = append(receivedContents, str)
		}
		if strings.Contains(str, `"finish_reason":"stop"`) {
			hasStop = true
		}
	}

	if !hasRole {
		t.Fatalf("expected role assistant")
	}
	if !hasStop {
		t.Fatalf("expected finish_reason stop")
	}
	if !hasDone {
		t.Fatalf("expected [DONE]")
	}

	var concatenatedContent string
	for _, c := range receivedContents {
		var chunkMap map[string]interface{}
		clean := strings.TrimPrefix(c, "data: ")
		if json.Unmarshal([]byte(clean), &chunkMap) == nil {
			if choices, ok := chunkMap["choices"].([]interface{}); ok && len(choices) > 0 {
				first := choices[0].(map[string]interface{})
				if delta, ok := first["delta"].(map[string]interface{}); ok {
					if txt, ok := delta["content"].(string); ok {
						concatenatedContent += txt
					}
				}
			}
		}
	}
	if concatenatedContent != "ABC" {
		t.Fatalf("expected concatenated content 'ABC', got %q", concatenatedContent)
	}
}

func TestProxyModelsCompressionGzip(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"gpt-4o"}]}`))
	}))
	defer upstream.Close()

	uURL, _ := url.Parse(upstream.URL)
	proxySrv := NewProxyServer(ServerConfig{
		UpstreamURL:  uURL,
		BufferConfig: aggregator.DefaultBufferConfig(),
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	proxySrv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if rec.Header().Get("Content-Encoding") != "gzip" {
		t.Fatalf("expected Content-Encoding gzip, got %s", rec.Header().Get("Content-Encoding"))
	}

	gzReader, err := gzip.NewReader(rec.Body)
	if err != nil {
		t.Fatalf("failed to init gzip reader: %v", err)
	}
	defer gzReader.Close()

	body, _ := io.ReadAll(gzReader)
	if !strings.Contains(string(body), "gpt-4o") {
		t.Fatalf("expected gpt-4o in decompressed body, got: %s", string(body))
	}
}

func TestProxyModelsCompressionZstd(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"gpt-4o"}]}`))
	}))
	defer upstream.Close()

	uURL, _ := url.Parse(upstream.URL)
	proxySrv := NewProxyServer(ServerConfig{
		UpstreamURL:  uURL,
		BufferConfig: aggregator.DefaultBufferConfig(),
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Accept-Encoding", "zstd, gzip")
	proxySrv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if rec.Header().Get("Content-Encoding") != "zstd" {
		t.Fatalf("expected Content-Encoding zstd, got %s", rec.Header().Get("Content-Encoding"))
	}

	zstdReader, err := zstd.NewReader(rec.Body)
	if err != nil {
		t.Fatalf("failed to init zstd reader: %v", err)
	}
	defer zstdReader.Close()

	body, _ := io.ReadAll(zstdReader)
	if !strings.Contains(string(body), "gpt-4o") {
		t.Fatalf("expected gpt-4o in decompressed body, got: %s", string(body))
	}
}

func TestProxyChatStreamCompressionZstd(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)

		chunks := []string{
			"data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"}}]}\n\n",
			"data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello \"}}]}\n\n",
			"data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"World!\"}}]}\n\n",
			"data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
			"data: [DONE]\n\n",
		}
		for _, chunk := range chunks {
			_, _ = w.Write([]byte(chunk))
			flusher.Flush()
		}
	}))
	defer upstream.Close()

	uURL, _ := url.Parse(upstream.URL)
	proxySrv := NewProxyServer(ServerConfig{
		UpstreamURL:  uURL,
		BufferConfig: aggregator.DefaultBufferConfig(),
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Accept-Encoding", "zstd")
	proxySrv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if rec.Header().Get("Content-Encoding") != "zstd" {
		t.Fatalf("expected Content-Encoding zstd, got %s", rec.Header().Get("Content-Encoding"))
	}

	zstdReader, err := zstd.NewReader(rec.Body)
	if err != nil {
		t.Fatalf("failed to init zstd reader: %v", err)
	}
	defer zstdReader.Close()

	r := sse.NewReader(zstdReader)
	var contents []string
	for {
		ev, err := r.ReadEvent()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read event err: %v", err)
		}
		if string(ev.Data) == "[DONE]" {
			break
		}
		var chunkMap map[string]interface{}
		clean := strings.TrimPrefix(string(ev.Data), "data: ")
		if json.Unmarshal([]byte(clean), &chunkMap) == nil {
			if choices, ok := chunkMap["choices"].([]interface{}); ok && len(choices) > 0 {
				first := choices[0].(map[string]interface{})
				if delta, ok := first["delta"].(map[string]interface{}); ok {
					if txt, ok := delta["content"].(string); ok {
						contents = append(contents, txt)
					}
				}
			}
		}
	}

	joined := strings.Join(contents, "")
	if joined != "Hello World!" {
		t.Fatalf("expected 'Hello World!', got %q", joined)
	}
}

func TestProxyMetricsCompression(t *testing.T) {
	uURL, _ := url.Parse("http://127.0.0.1:8000")
	proxySrv := NewProxyServer(ServerConfig{
		UpstreamURL:     uURL,
		BufferConfig:    aggregator.DefaultBufferConfig(),
		AllowMetricsAPI: true,
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	proxySrv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if rec.Header().Get("Content-Encoding") != "gzip" {
		t.Fatalf("expected Content-Encoding gzip, got %s", rec.Header().Get("Content-Encoding"))
	}

	gzReader, err := gzip.NewReader(rec.Body)
	if err != nil {
		t.Fatalf("failed to init gzip reader: %v", err)
	}
	defer gzReader.Close()

	body, _ := io.ReadAll(gzReader)
	var m map[string]interface{}
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("failed to parse decompressed metrics json: %v", err)
	}
	if _, ok := m["downstream_sse_events"]; !ok {
		t.Fatalf("missing downstream_sse_events in metrics")
	}
	if _, ok := m["compression_ratio"]; !ok {
		t.Fatalf("missing compression_ratio in metrics")
	}
	if _, ok := m["compression_savings_ratio"]; !ok {
		t.Fatalf("missing compression_savings_ratio in metrics")
	}
	if _, ok := m["compression_uncompressed_bytes"]; !ok {
		t.Fatalf("missing compression_uncompressed_bytes in metrics")
	}
	if _, ok := m["compression_compressed_bytes"]; !ok {
		t.Fatalf("missing compression_compressed_bytes in metrics")
	}
	val, ok := m["compression_savings_ratio_percent"].(string)
	if !ok || !strings.HasSuffix(val, "%") {
		t.Fatalf("expected percentage string for compression_savings_ratio_percent, got: %v", m["compression_savings_ratio_percent"])
	}
}

func TestMetricsJSONFieldOrder(t *testing.T) {
	uURL, _ := url.Parse("http://127.0.0.1:8000")
	proxySrv := NewProxyServer(ServerConfig{
		UpstreamURL:        uURL,
		BufferConfig:       aggregator.DefaultBufferConfig(),
		AllowMetricsAPI:    true,
		DisableCompression: true,
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	proxySrv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	body := rec.Body.String()
	dec := json.NewDecoder(strings.NewReader(body))
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('{') {
		t.Fatalf("expected JSON object start")
	}

	var actualOrder []string
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			t.Fatalf("decode key failed: %v", err)
		}
		key, ok := keyTok.(string)
		if !ok {
			t.Fatalf("expected string key")
		}
		actualOrder = append(actualOrder, key)
		var dummy interface{}
		if err := dec.Decode(&dummy); err != nil {
			t.Fatalf("decode value failed: %v", err)
		}
	}

	expectedOrder := []string{
		"upstream_sse_events",
		"downstream_sse_events",
		"overall_coalescing_ratio",
		"reasoning_fragments_in",
		"reasoning_events_out",
		"reasoning_coalescing_ratio",
		"content_fragments_in",
		"content_events_out",
		"content_coalescing_ratio",
		"tool_fragments_in",
		"tool_events_out",
		"tool_coalescing_ratio",
		"upstream_bytes",
		"downstream_bytes",
		"compression_uncompressed_bytes",
		"compression_compressed_bytes",
		"compression_ratio",
		"compression_savings_ratio",
		"compression_savings_ratio_percent",
		"pending_bytes_max",
		"reader_pause_count",
		"reader_pause_duration_ns",
		"downstream_write_ns",
		"models",
	}

	if len(actualOrder) != len(expectedOrder) {
		t.Fatalf("field count mismatch: got %d, want %d", len(actualOrder), len(expectedOrder))
	}
	for i := range expectedOrder {
		if actualOrder[i] != expectedOrder[i] {
			t.Fatalf("field index %d mismatch: got %q, want %q", i, actualOrder[i], expectedOrder[i])
		}
	}
}

func TestProxyDebugMetricsNotFound(t *testing.T) {
	uURL, _ := url.Parse("http://127.0.0.1:8000")
	proxySrv := NewProxyServer(ServerConfig{
		UpstreamURL:     uURL,
		BufferConfig:    aggregator.DefaultBufferConfig(),
		AllowMetricsAPI: true,
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/debug/metrics", nil)
	proxySrv.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for /debug/metrics, got %d", rec.Code)
	}
}

func TestProxyDisableCompression(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"gpt-4o"}]}`))
	}))
	defer upstream.Close()

	uURL, _ := url.Parse(upstream.URL)
	proxySrv := NewProxyServer(ServerConfig{
		UpstreamURL:        uURL,
		BufferConfig:       aggregator.DefaultBufferConfig(),
		DisableCompression: true,
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Accept-Encoding", "zstd, gzip")
	proxySrv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if rec.Header().Get("Content-Encoding") != "" {
		t.Fatalf("expected no Content-Encoding when disabled, got %s", rec.Header().Get("Content-Encoding"))
	}
	if !strings.Contains(rec.Body.String(), "gpt-4o") {
		t.Fatalf("expected plaintext gpt-4o, got %s", rec.Body.String())
	}
}

func TestProxyDashboardEndpoint(t *testing.T) {
	uURL, _ := url.Parse("http://127.0.0.1:9999")
	proxySrv := NewProxyServer(ServerConfig{
		UpstreamURL:     uURL,
		BufferConfig:    aggregator.DefaultBufferConfig(),
		AllowMetricsAPI: true,
	})

	// GET /dashboard
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	proxySrv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	cType := rec.Header().Get("Content-Type")
	if !strings.Contains(cType, "text/html") {
		t.Fatalf("expected text/html content type, got: %s", cType)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "Buffered Proxy 监控仪表盘") {
		t.Errorf("missing dashboard title in html")
	}
	if !strings.Contains(body, "生效配置 (Effective Configurations)") {
		t.Errorf("missing effective configs section in html")
	}
	if !strings.Contains(body, "模型性能指标 (Model Performance: TTFT &amp; TPS)") && !strings.Contains(body, "模型性能指标") {
		t.Errorf("missing model performance section in html")
	}
	if !strings.Contains(body, "http://127.0.0.1:9999") {
		t.Errorf("expected upstream url injected into html")
	}
	if !strings.Contains(body, "fetch('/metrics')") {
		t.Errorf("expected fetch metrics in javascript")
	}
	if !strings.Contains(body, "5") {
		t.Errorf("expected 5 seconds refresh setting in html")
	}

	// HEAD /dashboard
	headRec := httptest.NewRecorder()
	headReq := httptest.NewRequest(http.MethodHead, "/dashboard", nil)
	proxySrv.ServeHTTP(headRec, headReq)
	if headRec.Code != http.StatusOK {
		t.Fatalf("expected 200 for HEAD /dashboard, got %d", headRec.Code)
	}
	if headRec.Body.Len() != 0 {
		t.Fatalf("expected empty body for HEAD, got %d bytes", headRec.Body.Len())
	}

	// POST /dashboard -> 405
	postRec := httptest.NewRecorder()
	postReq := httptest.NewRequest(http.MethodPost, "/dashboard", nil)
	proxySrv.ServeHTTP(postRec, postReq)
	if postRec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 for POST /dashboard, got %d", postRec.Code)
	}

	// Disabled metrics API -> 404
	disabledSrv := NewProxyServer(ServerConfig{
		UpstreamURL:     uURL,
		BufferConfig:    aggregator.DefaultBufferConfig(),
		AllowMetricsAPI: false,
	})
	disRec := httptest.NewRecorder()
	disReq := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	disabledSrv.ServeHTTP(disRec, disReq)
	if disRec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 when AllowMetricsAPI is false, got %d", disRec.Code)
	}
}

func TestProxyModelTTFTAndTPS(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)

		// Delay slightly to test TTFT
		time.Sleep(10 * time.Millisecond)
		_, _ = w.Write([]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"}}]}\n\n"))
		flusher.Flush()

		time.Sleep(10 * time.Millisecond)
		_, _ = w.Write([]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello \"}}]}\n\n"))
		flusher.Flush()

		time.Sleep(10 * time.Millisecond)
		_, _ = w.Write([]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"World!\"}}]}\n\n"))
		flusher.Flush()

		_, _ = w.Write([]byte("data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"completion_tokens\":2}}\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
		flusher.Flush()
	}))
	defer upstream.Close()

	uURL, _ := url.Parse(upstream.URL)
	proxySrv := NewProxyServer(ServerConfig{
		UpstreamURL:        uURL,
		BufferConfig:       aggregator.DefaultBufferConfig(),
		AllowMetricsAPI:    true,
		DisableCompression: true,
	})

	chatReq := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"custom-test-model","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	chatRec := httptest.NewRecorder()
	proxySrv.ServeHTTP(chatRec, chatReq)

	if chatRec.Code != http.StatusOK {
		t.Fatalf("chat request failed: %d", chatRec.Code)
	}

	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRec := httptest.NewRecorder()
	proxySrv.ServeHTTP(metricsRec, metricsReq)

	if metricsRec.Code != http.StatusOK {
		t.Fatalf("metrics request failed: %d", metricsRec.Code)
	}

	var resp MetricsResponse
	if err := json.Unmarshal(metricsRec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode metrics response: %v", err)
	}

	m, ok := resp.Models["custom-test-model"]
	if !ok {
		t.Fatalf("expected custom-test-model in models metrics, got: %+v", resp.Models)
	}

	if m.Requests != 1 {
		t.Errorf("expected 1 request, got %d", m.Requests)
	}
	if m.TotalTokens != 2 {
		t.Errorf("expected 2 total tokens from usage, got %d", m.TotalTokens)
	}
	if m.AvgTTFTMs <= 0 {
		t.Errorf("expected AvgTTFTMs > 0, got %f", m.AvgTTFTMs)
	}
	if m.LastTTFTMs <= 0 {
		t.Errorf("expected LastTTFTMs > 0, got %f", m.LastTTFTMs)
	}
	if m.TPS <= 0 {
		t.Errorf("expected TPS > 0, got %f", m.TPS)
	}
}

func TestMonitorHub(t *testing.T) {
	hub := NewMonitorHub()

	if hub.HasSubscribers("gpt-4o") {
		t.Fatalf("expected no subscribers initially")
	}

	ch1, unsub1 := hub.Subscribe("gpt-4o")
	if !hub.HasSubscribers("gpt-4o") {
		t.Fatalf("expected subscriber for gpt-4o")
	}
	if hub.HasSubscribers("claude-3") {
		t.Fatalf("expected no subscriber for claude-3")
	}

	ch2, unsub2 := hub.Subscribe("claude-3")

	hub.Broadcast(MonitorPacketEvent{
		SessionID:  "sess_1",
		Model:      "gpt-4o",
		Direction:  DirectionUpstream,
		PacketType: "request",
		Payload:    `{"model":"gpt-4o"}`,
	})

	select {
	case ev := <-ch1:
		if ev.Model != "gpt-4o" || ev.Direction != DirectionUpstream {
			t.Fatalf("unexpected ev received on ch1: %+v", ev)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for ch1 event")
	}

	select {
	case ev := <-ch2:
		t.Fatalf("ch2 should not receive gpt-4o event, got: %+v", ev)
	default:
	}

	unsub1()
	unsub2()

	if hub.HasSubscribers("gpt-4o") || hub.HasSubscribers("claude-3") {
		t.Fatalf("expected no subscribers after unsub")
	}

	_, ok := <-ch1
	if ok {
		t.Fatalf("ch1 should be closed after unsub")
	}
}

func TestProxyServerMonitorStreamChat(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, ok := w.(http.Flusher)
		if !ok {
			return
		}
		_, _ = w.Write([]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"}}]}\n\n"))
		flusher.Flush()
		_, _ = w.Write([]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"}}]}\n\n"))
		flusher.Flush()
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
		flusher.Flush()
	}))
	defer upstream.Close()

	uURL, _ := url.Parse(upstream.URL)
	proxySrv := NewProxyServer(ServerConfig{
		UpstreamURL:     uURL,
		BufferConfig:    aggregator.DefaultBufferConfig(),
		AllowMetricsAPI: true,
	})

	chGpt, unsubGpt := proxySrv.MonitorHub().Subscribe("gpt-4o")
	defer unsubGpt()

	chClaude, unsubClaude := proxySrv.MonitorHub().Subscribe("claude-3")
	defer unsubClaude()

	chatReqBody := `{"model":"gpt-4o","messages":[{"role":"user","content":"Hi"}],"stream":true}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(chatReqBody))
	rec := httptest.NewRecorder()

	proxySrv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var receivedEvents []MonitorPacketEvent
collectLoop:
	for {
		select {
		case ev := <-chGpt:
			receivedEvents = append(receivedEvents, ev)
			if ev.PacketType == "session_end" {
				break collectLoop
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out collecting events, collected %d events", len(receivedEvents))
		}
	}

	if len(receivedEvents) < 2 {
		t.Fatalf("expected at least request, downstream chunk and session_end, got %d events", len(receivedEvents))
	}

	reqEv := receivedEvents[0]
	if reqEv.Direction != DirectionUpstream || reqEv.PacketType != "request" {
		t.Errorf("expected first event to be upstream request, got: %+v", reqEv)
	}
	if !strings.Contains(reqEv.Payload, `"gpt-4o"`) {
		t.Errorf("expected payload to contain gpt-4o, got: %s", reqEv.Payload)
	}

	hasDownstream := false
	hasContent := false
	for _, ev := range receivedEvents[1:] {
		if ev.Direction == DirectionDownstream && ev.PacketType == "downstream_chunk" {
			hasDownstream = true
			if strings.Contains(ev.Payload, "content") {
				hasContent = true
			}
		}
	}
	if !hasDownstream {
		t.Errorf("expected at least one downstream chunk")
	}
	if !hasContent {
		t.Errorf("expected at least one downstream chunk containing content")
	}

	select {
	case ev := <-chClaude:
		t.Fatalf("chClaude should not have received any event, got: %+v", ev)
	default:
	}
}

func TestProxyServerMonitorHTTP_SSE(t *testing.T) {
	proxySrv := NewProxyServer(ServerConfig{
		AllowMetricsAPI: true,
	})

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/monitor?model=gpt-4o", nil).WithContext(ctx)
	rec := httptest.NewRecorder()

	doneCh := make(chan struct{})
	go func() {
		proxySrv.ServeHTTP(rec, req)
		close(doneCh)
	}()

	time.Sleep(20 * time.Millisecond)

	if !proxySrv.MonitorHub().HasSubscribers("gpt-4o") {
		t.Fatalf("expected gpt-4o subscriber registered via HTTP /monitor")
	}

	proxySrv.MonitorHub().Broadcast(MonitorPacketEvent{
		SessionID:   "sess_test_http",
		Model:       "gpt-4o",
		TimestampMs: time.Now().UnixMilli(),
		Direction:   DirectionUpstream,
		PacketType:  "request",
		Payload:     `{"hello":"world"}`,
	})

	time.Sleep(30 * time.Millisecond)

	cancel()

	select {
	case <-doneCh:
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for monitor handler to exit on context cancellation")
	}

	body := rec.Body.String()
	if !strings.Contains(body, "sess_test_http") || !strings.Contains(body, "upstream") {
		t.Fatalf("expected SSE output to contain sess_test_http, got: %s", body)
	}
}

func TestProxyResponsesMethodNotAllowed(t *testing.T) {
	uURL, _ := url.Parse("http://127.0.0.1:8000")
	proxySrv := NewProxyServer(ServerConfig{
		UpstreamURL:  uURL,
		BufferConfig: aggregator.DefaultBufferConfig(),
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
	proxySrv.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status 405 for GET /v1/responses, got %d", rec.Code)
	}
}

func TestProxyResponsesStreamFalseTransparent(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/responses" {
			t.Errorf("unexpected upstream req: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"resp-123","status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"responses sync reply"}]}]}`))
	}))
	defer upstream.Close()

	uURL, _ := url.Parse(upstream.URL)
	proxySrv := NewProxyServer(ServerConfig{
		UpstreamURL:  uURL,
		BufferConfig: aggregator.DefaultBufferConfig(),
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-4o","input":"hi","stream":false}`))
	proxySrv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "responses sync reply") {
		t.Fatalf("expected sync reply, got: %s", body)
	}
}

func TestProxyResponsesStreamTrueAggregated(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/responses" {
			t.Errorf("unexpected upstream req: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)

		chunks := []string{
			"event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp-stream\",\"model\":\"gpt-4o\",\"status\":\"in_progress\"}}\n\n",
			"event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"sequence_number\":1,\"output_index\":0,\"item\":{\"id\":\"msg-1\",\"type\":\"message\"}}\n\n",
			"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":2,\"item_id\":\"msg-1\",\"output_index\":0,\"content_index\":0,\"delta\":\"Alpha \"}\n\n",
			"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":3,\"item_id\":\"msg-1\",\"output_index\":0,\"content_index\":0,\"delta\":\"Beta \"}\n\n",
			"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":4,\"item_id\":\"msg-1\",\"output_index\":0,\"content_index\":0,\"delta\":\"Gamma\"}\n\n",
			"event: response.output_text.done\ndata: {\"type\":\"response.output_text.done\",\"sequence_number\":5,\"item_id\":\"msg-1\",\"output_index\":0,\"content_index\":0,\"text\":\"Alpha Beta Gamma\"}\n\n",
			"event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"sequence_number\":6,\"output_index\":0}\n\n",
			"event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":7,\"response\":{\"id\":\"resp-stream\",\"model\":\"gpt-4o\",\"status\":\"completed\",\"usage\":{\"input_tokens\":5,\"output_tokens\":3,\"total_tokens\":8}}}\n\n",
		}
		for _, chunk := range chunks {
			_, _ = w.Write([]byte(chunk))
			flusher.Flush()
		}
	}))
	defer upstream.Close()

	uURL, _ := url.Parse(upstream.URL)
	proxySrv := NewProxyServer(ServerConfig{
		UpstreamURL:     uURL,
		BufferConfig:    aggregator.DefaultBufferConfig(),
		AllowMetricsAPI: true,
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-4o","input":"hi","stream":true}`))
	proxySrv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	res := rec.Body.Bytes()
	r := sse.NewReader(bytes.NewReader(res))
	var receivedDeltas []string
	var hasCreated, hasDone, hasCompleted bool

	for {
		ev, err := r.ReadEvent()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read event err: %v", err)
		}
		switch ev.Type {
		case "response.created":
			hasCreated = true
		case "response.output_text.delta":
			var dMap map[string]interface{}
			if err := json.Unmarshal(ev.Data, &dMap); err != nil {
				t.Fatalf("invalid delta json: %v", err)
			}
			if d, ok := dMap["delta"].(string); ok {
				receivedDeltas = append(receivedDeltas, d)
			}
		case "response.output_text.done":
			hasDone = true
		case "response.completed":
			hasCompleted = true
		}
	}

	if !hasCreated {
		t.Fatalf("missing response.created event")
	}
	if !hasDone {
		t.Fatalf("missing response.output_text.done event")
	}
	if !hasCompleted {
		t.Fatalf("missing response.completed event")
	}

	joinedDelta := strings.Join(receivedDeltas, "")
	if joinedDelta != "Alpha Beta Gamma" {
		t.Fatalf("expected 'Alpha Beta Gamma', got %q", joinedDelta)
	}
}

func TestMultiUpstream_ListModels(t *testing.T) {
	upstream1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"object": "list",
			"data": []map[string]interface{}{
				{"id": "model1", "object": "model"},
				{"id": "model2", "object": "model"},
			},
		})
	}))
	defer upstream1.Close()

	upstream2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"object": "list",
			"data": []map[string]interface{}{
				{"id": "model3", "object": "model"},
			},
		})
	}))
	defer upstream2.Close()

	u1, _ := url.Parse(upstream1.URL)
	u2, _ := url.Parse(upstream2.URL)

	proxySrv := NewProxyServer(ServerConfig{
		Upstreams: []UpstreamTarget{
			{Name: "test", URL: u1},
			{Name: "prod", URL: u2},
		},
		BufferConfig:       aggregator.DefaultBufferConfig(),
		DisableCompression: true,
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	proxySrv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var respMap map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &respMap); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	dataList, ok := respMap["data"].([]interface{})
	if !ok || len(dataList) != 3 {
		t.Fatalf("expected 3 models in data, got: %+v", respMap)
	}

	ids := make([]string, len(dataList))
	for i, d := range dataList {
		m := d.(map[string]interface{})
		ids[i] = m["id"].(string)
	}

	expectedIDs := []string{"test/model1", "test/model2", "prod/model3"}
	for i, expected := range expectedIDs {
		if ids[i] != expected {
			t.Fatalf("expected id[%d] == %s, got %s", i, expected, ids[i])
		}
	}
}

func TestMultiUpstream_Routing_ChatCompletions_Streaming(t *testing.T) {
	var capturedModel string
	var upstream1Called bool

	upstream1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstream1Called = true
		bodyBytes, _ := io.ReadAll(r.Body)
		var reqMap map[string]interface{}
		_ = json.Unmarshal(bodyBytes, &reqMap)
		if m, ok := reqMap["model"].(string); ok {
			capturedModel = m
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()

		_, _ = w.Write([]byte("data: {\"id\":\"1\",\"object\":\"chat.completion.chunk\",\"created\":100,\"model\":\"model1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"},\"finish_reason\":null}]}\n\n"))
		w.(http.Flusher).Flush()
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
		w.(http.Flusher).Flush()
	}))
	defer upstream1.Close()

	var upstream2Called bool
	upstream2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstream2Called = true
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream2.Close()

	u1, _ := url.Parse(upstream1.URL)
	u2, _ := url.Parse(upstream2.URL)

	proxySrv := NewProxyServer(ServerConfig{
		Upstreams: []UpstreamTarget{
			{Name: "test", URL: u1},
			{Name: "prod", URL: u2},
		},
		BufferConfig:       aggregator.DefaultBufferConfig(),
		DisableCompression: true,
	})

	reqBody := `{"model":"test/model1","messages":[{"role":"user","content":"Hi"}],"stream":true}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	proxySrv.ServeHTTP(rec, req)

	if !upstream1Called {
		t.Fatalf("expected upstream 1 to be called")
	}
	if upstream2Called {
		t.Fatalf("upstream 2 should not be called")
	}
	if capturedModel != "model1" {
		t.Fatalf("expected upstream to receive model 'model1', got %q", capturedModel)
	}

	bodyStr := rec.Body.String()
	if !strings.Contains(bodyStr, "test/model1") {
		t.Fatalf("expected downstream response to contain model 'test/model1', got: %s", bodyStr)
	}
	if !strings.Contains(bodyStr, "Hello") {
		t.Fatalf("expected content 'Hello' in response, got: %s", bodyStr)
	}
}

func TestMultiUpstream_Routing_ChatCompletions_NonStreaming(t *testing.T) {
	var capturedModel string

	upstreamProd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bodyBytes, _ := io.ReadAll(r.Body)
		var reqMap map[string]interface{}
		_ = json.Unmarshal(bodyBytes, &reqMap)
		if m, ok := reqMap["model"].(string); ok {
			capturedModel = m
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"chatcmpl-1","object":"chat.completion","model":"modelA","choices":[{"index":0,"message":{"role":"assistant","content":"I am prod"}}]}`))
	}))
	defer upstreamProd.Close()

	uProd, _ := url.Parse(upstreamProd.URL)
	uTest, _ := url.Parse("http://127.0.0.1:9999")

	proxySrv := NewProxyServer(ServerConfig{
		Upstreams: []UpstreamTarget{
			{Name: "test", URL: uTest},
			{Name: "prod", URL: uProd},
		},
		BufferConfig:       aggregator.DefaultBufferConfig(),
		DisableCompression: true,
	})

	reqBody := `{"model":"prod/modelA","messages":[{"role":"user","content":"Hi"}],"stream":false}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	proxySrv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d, body: %s", rec.Code, rec.Body.String())
	}
	if capturedModel != "modelA" {
		t.Fatalf("expected upstream to receive 'modelA', got %q", capturedModel)
	}

	var respMap map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &respMap); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if respMap["model"] != "prod/modelA" {
		t.Fatalf("expected downstream model 'prod/modelA', got %v", respMap["model"])
	}
}

func TestMultiUpstream_Routing_Errors(t *testing.T) {
	u1, _ := url.Parse("http://127.0.0.1:8001")
	u2, _ := url.Parse("http://127.0.0.1:8002")

	proxySrv := NewProxyServer(ServerConfig{
		Upstreams: []UpstreamTarget{
			{Name: "test", URL: u1},
			{Name: "prod", URL: u2},
		},
		BufferConfig: aggregator.DefaultBufferConfig(),
	})

	// 1. Unknown upstream
	reqBody := `{"model":"unknown/model1","messages":[{"role":"user","content":"Hi"}],"stream":true}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(reqBody))
	rec := httptest.NewRecorder()
	proxySrv.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400 on unknown upstream, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "unknown") {
		t.Fatalf("expected error message to mention unknown upstream, got: %s", rec.Body.String())
	}

	// 2. Missing prefix when multiple upstreams exist
	reqBody = `{"model":"model1","messages":[{"role":"user","content":"Hi"}],"stream":true}`
	req = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(reqBody))
	rec = httptest.NewRecorder()
	proxySrv.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400 on missing prefix, got %d", rec.Code)
	}
}

func TestMultiUpstream_APIKey(t *testing.T) {
	var capturedAuthHeader string

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedAuthHeader = r.Header.Get("Authorization")
		if capturedAuthHeader != "Bearer secret-test-key" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
			return
		}
		if r.URL.Path == "/v1/models" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"object": "list",
				"data":   []map[string]interface{}{{"id": "model1"}},
			})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"id":    "cmpl",
			"model": "model1",
		})
	}))
	defer upstream.Close()

	u, _ := url.Parse(upstream.URL)
	proxySrv := NewProxyServer(ServerConfig{
		Upstreams: []UpstreamTarget{
			{Name: "auth-test", URL: u, APIKey: "secret-test-key"},
		},
		BufferConfig:       aggregator.DefaultBufferConfig(),
		DisableCompression: true,
	})

	// 1. Models call
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	proxySrv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if capturedAuthHeader != "Bearer secret-test-key" {
		t.Fatalf("expected 'Bearer secret-test-key', got %q", capturedAuthHeader)
	}

	// 2. Chat completions call
	capturedAuthHeader = ""
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"auth-test/model1","messages":[]}`))
	req.Header.Set("Content-Type", "application/json")
	proxySrv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if capturedAuthHeader != "Bearer secret-test-key" {
		t.Fatalf("expected 'Bearer secret-test-key', got %q", capturedAuthHeader)
	}
}

func TestProxyModels_MemoryCacheAndRevalidation(t *testing.T) {
	var (
		upstreamCalls atomic.Int32
		modelNameMu   sync.Mutex
		currentModel  = "model-v1"
		upstreamBlock = make(chan struct{})
		isBlocking    atomic.Bool
	)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			http.NotFound(w, r)
			return
		}
		upstreamCalls.Add(1)

		if isBlocking.Load() {
			<-upstreamBlock
		}

		modelNameMu.Lock()
		m := currentModel
		modelNameMu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"object": "list",
			"data":   []map[string]interface{}{{"id": m}},
		})
	}))
	defer upstream.Close()

	uURL, _ := url.Parse(upstream.URL)
	proxySrv := NewProxyServer(ServerConfig{
		UpstreamURL:        uURL,
		BufferConfig:       aggregator.DefaultBufferConfig(),
		DisableCompression: true,
	})

	// 1. First request: cache is empty, fetches synchronously
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	proxySrv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "model-v1") {
		t.Fatalf("expected model-v1, got %s", rec.Body.String())
	}
	if upstreamCalls.Load() != 1 {
		t.Fatalf("expected 1 upstream call, got %d", upstreamCalls.Load())
	}

	// Change upstream response to model-v2
	modelNameMu.Lock()
	currentModel = "model-v2"
	modelNameMu.Unlock()

	// 2. Second request: returns cached data (model-v1) immediately, triggers background update
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	proxySrv.ServeHTTP(rec, req)

	if !strings.Contains(rec.Body.String(), "model-v1") {
		t.Fatalf("expected cached model-v1 returned immediately, got %s", rec.Body.String())
	}

	// Wait for background update to finish
	deadline := time.Now().Add(2 * time.Second)
	for {
		if upstreamCalls.Load() == 2 && !proxySrv.modelsUpdating.Load() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for background update, calls=%d, updating=%v", upstreamCalls.Load(), proxySrv.modelsUpdating.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 3. Third request: upstream has finished updating, so it returns model-v2 immediately.
	// Now enable blocking on upstream to test in-flight suppression!
	isBlocking.Store(true)
	modelNameMu.Lock()
	currentModel = "model-v3"
	modelNameMu.Unlock()

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	proxySrv.ServeHTTP(rec, req)

	if !strings.Contains(rec.Body.String(), "model-v2") {
		t.Fatalf("expected model-v2, got %s", rec.Body.String())
	}

	// Wait until the 3rd upstream call has started and is blocked
	deadline = time.Now().Add(2 * time.Second)
	for {
		if upstreamCalls.Load() == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for 3rd upstream call to start")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 4. Fourth request arrives WHILE previous update is still running (blocked):
	// It should return memory data immediately (model-v2) and NOT trigger a new upstream request!
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	proxySrv.ServeHTTP(rec, req)

	if !strings.Contains(rec.Body.String(), "model-v2") {
		t.Fatalf("expected model-v2 from cache, got %s", rec.Body.String())
	}
	// Upstream calls MUST still be 3! No new request was sent!
	if upstreamCalls.Load() != 3 {
		t.Fatalf("expected upstreamCalls to remain 3, got %d", upstreamCalls.Load())
	}

	// 5. Unblock upstream and let update finish
	close(upstreamBlock)
	deadline = time.Now().Add(2 * time.Second)
	for {
		if !proxySrv.modelsUpdating.Load() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for update to finish")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 6. Next request: now that update finished, it returns model-v3 and triggers new update
	isBlocking.Store(false)
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	proxySrv.ServeHTTP(rec, req)

	if !strings.Contains(rec.Body.String(), "model-v3") {
		t.Fatalf("expected model-v3, got %s", rec.Body.String())
	}

	deadline = time.Now().Add(2 * time.Second)
	for {
		if upstreamCalls.Load() == 4 && !proxySrv.modelsUpdating.Load() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for 4th update to finish")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestProxyModels_ConcurrentAccess(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"object": "list",
			"data":   []map[string]interface{}{{"id": "gpt-4"}},
		})
	}))
	defer upstream.Close()

	uURL, _ := url.Parse(upstream.URL)
	proxySrv := NewProxyServer(ServerConfig{
		UpstreamURL:        uURL,
		BufferConfig:       aggregator.DefaultBufferConfig(),
		DisableCompression: true,
	})

	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
			proxySrv.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Errorf("expected 200, got %d", rec.Code)
			}
		}()
	}
	wg.Wait()
}
