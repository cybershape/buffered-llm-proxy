package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"buffered-proxy/pkg/aggregator"
	"buffered-proxy/pkg/proxy"
	"buffered-proxy/pkg/sse"
)

func TestEndToEndFullFlow(t *testing.T) {
	upstreamServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"mock-model-v1"}]}`))
		case "/v1/completions":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"cmpl-001","choices":[{"text":"completion result"}]}`))
		case "/v1/chat/completions":
			bodyBytes, _ := io.ReadAll(r.Body)
			var reqMap map[string]interface{}
			_ = json.Unmarshal(bodyBytes, &reqMap)

			streamVal, _ := reqMap["stream"].(bool)
			if !streamVal {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"id":"chat-sync","choices":[{"message":{"role":"assistant","content":"sync response"}}]}`))
				return
			}

			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			flusher := w.(http.Flusher)

			chunks := []string{
				"data: {\"id\":\"cmpl-stream\",\"model\":\"mock-r1\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"}}]}\n\n",
				"data: {\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\"reasoning-part1-\"}}]}\n\n",
				"data: {\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\"reasoning-part2-\"}}]}\n\n",
				"data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"content-part1-\"}}]}\n\n",
				"data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"content-part2-\"}}]}\n\n",
				"data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_9\",\"type\":\"function\",\"function\":{\"name\":\"calc\",\"arguments\":\"{\\\"val\\\":\"}}]}}]}\n\n",
				"data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"42}\"}}]}}]}\n\n",
				"data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
				"data: [DONE]\n\n",
			}
			for _, c := range chunks {
				_, _ = w.Write([]byte(c))
				flusher.Flush()
				time.Sleep(1 * time.Millisecond)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstreamServer.Close()

	uURL, err := url.Parse(upstreamServer.URL)
	if err != nil {
		t.Fatalf("parse upstream url failed: %v", err)
	}

	proxySrv := proxy.NewProxyServer(proxy.ServerConfig{
		UpstreamURL:     uURL,
		BufferConfig:    aggregator.DefaultBufferConfig(),
		AllowMetricsAPI: true,
	})

	proxyTestServer := httptest.NewServer(proxySrv)
	defer proxyTestServer.Close()

	client := proxyTestServer.Client()

	respModels, err := client.Get(proxyTestServer.URL + "/v1/models")
	if err != nil || respModels.StatusCode != http.StatusOK {
		t.Fatalf("failed models request: %v, code: %d", err, respModels.StatusCode)
	}
	bodyModels, _ := io.ReadAll(respModels.Body)
	_ = respModels.Body.Close()
	if !strings.Contains(string(bodyModels), "mock-model-v1") {
		t.Fatalf("unexpected models body: %s", string(bodyModels))
	}

	respCmpl, err := client.Post(proxyTestServer.URL+"/v1/completions", "application/json", strings.NewReader(`{"prompt":"test"}`))
	if err != nil || respCmpl.StatusCode != http.StatusOK {
		t.Fatalf("failed completions request: %v", err)
	}
	bodyCmpl, _ := io.ReadAll(respCmpl.Body)
	_ = respCmpl.Body.Close()
	if !strings.Contains(string(bodyCmpl), "completion result") {
		t.Fatalf("unexpected completions body: %s", string(bodyCmpl))
	}

	respSyncChat, err := client.Post(proxyTestServer.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"stream":false,"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil || respSyncChat.StatusCode != http.StatusOK {
		t.Fatalf("failed sync chat: %v", err)
	}
	bodySyncChat, _ := io.ReadAll(respSyncChat.Body)
	_ = respSyncChat.Body.Close()
	if !strings.Contains(string(bodySyncChat), "sync response") {
		t.Fatalf("unexpected sync chat body: %s", string(bodySyncChat))
	}

	respStreamChat, err := client.Post(proxyTestServer.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"stream":true,"messages":[{"role":"user","content":"stream test"}]}`))
	if err != nil || respStreamChat.StatusCode != http.StatusOK {
		t.Fatalf("failed stream chat: %v", err)
	}
	defer respStreamChat.Body.Close()

	if !strings.Contains(respStreamChat.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("expected text/event-stream content type, got: %s", respStreamChat.Header.Get("Content-Type"))
	}

	sseReader := sse.NewReader(respStreamChat.Body)
	var allStreamText strings.Builder
	var doneReceived bool

	for {
		ev, err := sseReader.ReadEvent()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read stream error: %v", err)
		}
		if string(ev.Data) == "[DONE]" {
			doneReceived = true
			continue
		}
		allStreamText.Write(ev.Data)
	}

	if !doneReceived {
		t.Fatalf("expected [DONE] event in stream")
	}

	streamOutput := allStreamText.String()
	if !strings.Contains(streamOutput, "reasoning-part1-") {
		t.Fatalf("missing reasoning part in stream")
	}
	if !strings.Contains(streamOutput, "content-part1-") {
		t.Fatalf("missing content part in stream")
	}
	if !strings.Contains(streamOutput, "42}") {
		t.Fatalf("missing coalesced arguments in stream: %s", streamOutput)
	}

	respMetrics, err := client.Get(proxyTestServer.URL + "/metrics")
	if err != nil || respMetrics.StatusCode != http.StatusOK {
		t.Fatalf("failed to get metrics: %v", err)
	}
	var metricsMap map[string]interface{}
	_ = json.NewDecoder(respMetrics.Body).Decode(&metricsMap)
	_ = respMetrics.Body.Close()

	if upEvents, ok := metricsMap["upstream_sse_events"].(float64); !ok || upEvents == 0 {
		t.Fatalf("expected non-zero upstream_sse_events in metrics: %v", metricsMap)
	}
	if downEvents, ok := metricsMap["downstream_sse_events"].(float64); !ok || downEvents == 0 {
		t.Fatalf("expected non-zero downstream_sse_events in metrics: %v", metricsMap)
	}
}

func TestUnknownEventPassthrough(t *testing.T) {
	var inSSE bytes.Buffer
	inSSE.WriteString("data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"}}]}\n\n")
	inSSE.WriteString("data: {\"custom_provider_event\":\"custom_data_payload\"}\n\n")
	inSSE.WriteString("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"}}]}\n\n")
	inSSE.WriteString("data: [DONE]\n\n")

	outBuf := &bytes.Buffer{}
	pipeline := aggregator.NewStreamPipeline(aggregator.DefaultBufferConfig(), nil)

	err := pipeline.ProcessStream(context.Background(), io.NopCloser(&inSSE), outBuf)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	outStr := outBuf.String()
	if !strings.Contains(outStr, "custom_provider_event") {
		t.Fatalf("expected unknown custom event to be safely passed through, got: %s", outStr)
	}
	if !strings.Contains(outStr, "custom_data_payload") {
		t.Fatalf("expected payload to be preserved, got: %s", outStr)
	}
}
