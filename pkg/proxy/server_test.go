package proxy

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

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
	if m1["content_length"] != float64(128000) {
		t.Fatalf("expected content_length 128000, got %v", m1["content_length"])
	}

	m2 := dataList[1].(map[string]interface{})
	extra := m2["extra"].(map[string]interface{})
	if extra["context_window"] != float64(32768) {
		t.Fatalf("expected extra.context_window 32768, got %v", extra["context_window"])
	}
	if extra["content_length"] != float64(32768) {
		t.Fatalf("expected extra.content_length 32768, got %v", extra["content_length"])
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
