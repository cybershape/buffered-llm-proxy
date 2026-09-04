package proxy

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"buffered-proxy/pkg/aggregator"
	"buffered-proxy/pkg/sse"
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

func TestProxyCompletionsTransparent(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/completions" {
			t.Errorf("unexpected upstream req: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"cmpl-1","choices":[{"text":"hello completions"}]}`))
	}))
	defer upstream.Close()

	uURL, _ := url.Parse(upstream.URL)
	proxySrv := NewProxyServer(ServerConfig{
		UpstreamURL:  uURL,
		BufferConfig: aggregator.DefaultBufferConfig(),
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"prompt":"hi"}`))
	proxySrv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "hello completions") {
		t.Fatalf("expected completions in body, got: %s", body)
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
