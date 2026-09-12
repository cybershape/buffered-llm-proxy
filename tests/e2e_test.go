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

	"github.com/klauspost/compress/zstd"
)

func TestEndToEndFullFlow(t *testing.T) {
	upstreamServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"mock-model-v1"}]}`))
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

	modelsMap, ok := metricsMap["models"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected models map in metrics: %v", metricsMap)
	}
	mockR1, ok := modelsMap["mock-r1"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected mock-r1 in models: %v", modelsMap)
	}
	if reqs, ok := mockR1["requests"].(float64); !ok || reqs != 1 {
		t.Errorf("expected 1 request for mock-r1, got: %v", mockR1["requests"])
	}
	if tps, ok := mockR1["tps"].(float64); !ok || tps <= 0 {
		t.Errorf("expected positive TPS for mock-r1, got: %v", mockR1["tps"])
	}
	if ttft, ok := mockR1["avg_ttft_ms"].(float64); !ok || ttft <= 0 {
		t.Errorf("expected positive Avg TTFT for mock-r1, got: %v", mockR1["avg_ttft_ms"])
	}

	respDashboard, err := client.Get(proxyTestServer.URL + "/dashboard")
	if err != nil || respDashboard.StatusCode != http.StatusOK {
		t.Fatalf("failed to get dashboard: %v", err)
	}
	bodyDash, _ := io.ReadAll(respDashboard.Body)
	_ = respDashboard.Body.Close()
	if !strings.Contains(string(bodyDash), "Effective Configurations") {
		t.Fatalf("missing effective configs in dashboard")
	}
	if !strings.Contains(string(bodyDash), "Model Performance") {
		t.Fatalf("missing model performance in dashboard")
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

func TestEndToEndZstdStreamingCompression(t *testing.T) {
	upstreamServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)

		chunks := []string{
			"data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"}}]}\n\n",
			"data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"chunk-1-\"}}]}\n\n",
			"data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"chunk-2-\"}}]}\n\n",
			"data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
			"data: [DONE]\n\n",
		}
		for _, c := range chunks {
			_, _ = w.Write([]byte(c))
			flusher.Flush()
			time.Sleep(1 * time.Millisecond)
		}
	}))
	defer upstreamServer.Close()

	uURL, err := url.Parse(upstreamServer.URL)
	if err != nil {
		t.Fatalf("parse upstream url failed: %v", err)
	}

	proxySrv := proxy.NewProxyServer(proxy.ServerConfig{
		UpstreamURL:  uURL,
		BufferConfig: aggregator.DefaultBufferConfig(),
	})

	proxyTestServer := httptest.NewServer(proxySrv)
	defer proxyTestServer.Close()

	req, err := http.NewRequest(http.MethodPost, proxyTestServer.URL+"/v1/chat/completions", strings.NewReader(`{"stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatalf("create request failed: %v", err)
	}
	req.Header.Set("Accept-Encoding", "zstd, gzip")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Encoding") != "zstd" {
		t.Fatalf("expected Content-Encoding zstd, got %s", resp.Header.Get("Content-Encoding"))
	}

	zstdReader, err := zstd.NewReader(resp.Body)
	if err != nil {
		t.Fatalf("init zstd reader failed: %v", err)
	}
	defer zstdReader.Close()

	r := sse.NewReader(zstdReader)
	var eventsReceived int
	for {
		ev, err := r.ReadEvent()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read event failed: %v", err)
		}
		if string(ev.Data) == "[DONE]" {
			break
		}
		eventsReceived++
	}

	if eventsReceived == 0 {
		t.Fatalf("expected to receive events over zstd compressed stream")
	}
}

func TestEndToEndResponsesFlow(t *testing.T) {
	upstreamServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/responses" {
			http.NotFound(w, r)
			return
		}
		bodyBytes, _ := io.ReadAll(r.Body)
		var reqMap map[string]interface{}
		_ = json.Unmarshal(bodyBytes, &reqMap)

		streamVal, _ := reqMap["stream"].(bool)
		if !streamVal {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"resp-sync","status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"responses sync body"}]}]}`))
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)

		chunks := []string{
			"event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp-stream\",\"model\":\"responses-test-model\",\"status\":\"in_progress\"}}\n\n",
			"event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"sequence_number\":1,\"output_index\":0,\"item\":{\"id\":\"msg_1\",\"type\":\"message\"}}\n\n",
			"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":2,\"item_id\":\"msg_1\",\"output_index\":0,\"content_index\":0,\"delta\":\"chunk-1-\"}\n\n",
			"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":3,\"item_id\":\"msg_1\",\"output_index\":0,\"content_index\":0,\"delta\":\"chunk-2-\"}\n\n",
			"event: response.output_text.done\ndata: {\"type\":\"response.output_text.done\",\"sequence_number\":4,\"item_id\":\"msg_1\",\"output_index\":0,\"content_index\":0,\"text\":\"chunk-1-chunk-2-\"}\n\n",
			"event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"sequence_number\":5,\"output_index\":0}\n\n",
			"event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":6,\"response\":{\"id\":\"resp-stream\",\"model\":\"responses-test-model\",\"status\":\"completed\",\"usage\":{\"input_tokens\":10,\"output_tokens\":2,\"total_tokens\":12}}}\n\n",
		}
		for _, c := range chunks {
			_, _ = w.Write([]byte(c))
			flusher.Flush()
			time.Sleep(1 * time.Millisecond)
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

	// 1. Non-streaming
	respSync, err := client.Post(proxyTestServer.URL+"/v1/responses", "application/json", strings.NewReader(`{"model":"responses-test-model","input":"hi","stream":false}`))
	if err != nil || respSync.StatusCode != http.StatusOK {
		t.Fatalf("failed sync responses: %v, code: %d", err, respSync.StatusCode)
	}
	bodySync, _ := io.ReadAll(respSync.Body)
	_ = respSync.Body.Close()
	if !strings.Contains(string(bodySync), "responses sync body") {
		t.Fatalf("unexpected sync body: %s", string(bodySync))
	}

	// 2. Streaming
	respStream, err := client.Post(proxyTestServer.URL+"/v1/responses", "application/json", strings.NewReader(`{"model":"responses-test-model","input":"stream test","stream":true}`))
	if err != nil || respStream.StatusCode != http.StatusOK {
		t.Fatalf("failed stream responses: %v", err)
	}
	defer respStream.Body.Close()

	if !strings.Contains(respStream.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("expected text/event-stream content type, got: %s", respStream.Header.Get("Content-Type"))
	}

	sseReader := sse.NewReader(respStream.Body)
	var allDeltaText strings.Builder
	var completedReceived bool

	for {
		ev, err := sseReader.ReadEvent()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read stream error: %v", err)
		}
		if ev.Type == "response.completed" {
			completedReceived = true
		}
		if ev.Type == "response.output_text.delta" {
			var dMap map[string]interface{}
			if json.Unmarshal(ev.Data, &dMap) == nil {
				if d, ok := dMap["delta"].(string); ok {
					allDeltaText.WriteString(d)
				}
			}
		}
	}

	if !completedReceived {
		t.Fatalf("expected response.completed in stream")
	}
	if allDeltaText.String() != "chunk-1-chunk-2-" {
		t.Fatalf("expected 'chunk-1-chunk-2-', got %q", allDeltaText.String())
	}

	// 3. Metrics
	respMetrics, err := client.Get(proxyTestServer.URL + "/metrics")
	if err != nil || respMetrics.StatusCode != http.StatusOK {
		t.Fatalf("failed to get metrics: %v", err)
	}
	var metricsMap map[string]interface{}
	_ = json.NewDecoder(respMetrics.Body).Decode(&metricsMap)
	_ = respMetrics.Body.Close()

	modelsMap, ok := metricsMap["models"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected models in metrics: %v", metricsMap)
	}
	modelMetric, ok := modelsMap["responses-test-model"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected responses-test-model in models: %v", modelsMap)
	}
	if reqs, ok := modelMetric["requests"].(float64); !ok || reqs != 1 {
		t.Errorf("expected 1 request, got %v", modelMetric["requests"])
	}
	if tps, ok := modelMetric["tps"].(float64); !ok || tps <= 0 {
		t.Errorf("expected positive TPS, got %v", modelMetric["tps"])
	}
	if ttft, ok := modelMetric["avg_ttft_ms"].(float64); !ok || ttft <= 0 {
		t.Errorf("expected positive Avg TTFT, got %v", modelMetric["avg_ttft_ms"])
	}
}

func TestEndToEndMultiUpstream(t *testing.T) {
	var upstream1ReceivedModel string
	var upstream2ReceivedModel string

	upstream1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"object": "list",
				"data": []map[string]interface{}{
					{"id": "model1"},
				},
			})
		case "/v1/chat/completions":
			bodyBytes, _ := io.ReadAll(r.Body)
			var reqMap map[string]interface{}
			_ = json.Unmarshal(bodyBytes, &reqMap)
			upstream1ReceivedModel = reqMap["model"].(string)

			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			flusher := w.(http.Flusher)
			_, _ = w.Write([]byte("data: {\"id\":\"cmpl-1\",\"model\":\"model1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"upstream1-reply\"}}]}\n\n"))
			flusher.Flush()
			_, _ = w.Write([]byte("data: [DONE]\n\n"))
			flusher.Flush()
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream1.Close()

	upstream2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"object": "list",
				"data": []map[string]interface{}{
					{"id": "modelA"},
				},
			})
		case "/v1/chat/completions":
			bodyBytes, _ := io.ReadAll(r.Body)
			var reqMap map[string]interface{}
			_ = json.Unmarshal(bodyBytes, &reqMap)
			upstream2ReceivedModel = reqMap["model"].(string)

			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			flusher := w.(http.Flusher)
			_, _ = w.Write([]byte("data: {\"id\":\"cmpl-2\",\"model\":\"modelA\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"upstream2-reply\"}}]}\n\n"))
			flusher.Flush()
			_, _ = w.Write([]byte("data: [DONE]\n\n"))
			flusher.Flush()
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream2.Close()

	u1, _ := url.Parse(upstream1.URL)
	u2, _ := url.Parse(upstream2.URL)

	proxySrv := proxy.NewProxyServer(proxy.ServerConfig{
		Upstreams: []proxy.UpstreamTarget{
			{Name: "test", URL: u1},
			{Name: "prod", URL: u2},
		},
		BufferConfig:       aggregator.DefaultBufferConfig(),
		DisableCompression: true,
	})

	proxyTestServer := httptest.NewServer(proxySrv)
	defer proxyTestServer.Close()

	client := proxyTestServer.Client()

	// 1. Check /v1/models
	resp, err := client.Get(proxyTestServer.URL + "/v1/models")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to get models: %v", err)
	}
	var modelsResp map[string]interface{}
	_ = json.NewDecoder(resp.Body).Decode(&modelsResp)
	_ = resp.Body.Close()

	dataList := modelsResp["data"].([]interface{})
	if len(dataList) != 2 {
		t.Fatalf("expected 2 models, got %d", len(dataList))
	}
	m0 := dataList[0].(map[string]interface{})["id"].(string)
	m1 := dataList[1].(map[string]interface{})["id"].(string)
	if m0 != "test/model1" || m1 != "prod/modelA" {
		t.Fatalf("expected test/model1 and prod/modelA, got %s and %s", m0, m1)
	}

	// 2. Request test/model1
	reqBody1 := `{"model":"test/model1","messages":[{"role":"user","content":"hello"}],"stream":true}`
	resp1, err := client.Post(proxyTestServer.URL+"/v1/chat/completions", "application/json", strings.NewReader(reqBody1))
	if err != nil || resp1.StatusCode != http.StatusOK {
		t.Fatalf("failed post to test/model1: %v", err)
	}
	body1Bytes, _ := io.ReadAll(resp1.Body)
	_ = resp1.Body.Close()

	if upstream1ReceivedModel != "model1" {
		t.Fatalf("expected upstream 1 to receive 'model1', got %q", upstream1ReceivedModel)
	}
	if !strings.Contains(string(body1Bytes), "test/model1") {
		t.Fatalf("expected downstream response to have model 'test/model1', got: %s", string(body1Bytes))
	}
	if !strings.Contains(string(body1Bytes), "upstream1-reply") {
		t.Fatalf("expected downstream response to have 'upstream1-reply', got: %s", string(body1Bytes))
	}

	// 3. Request prod/modelA
	reqBody2 := `{"model":"prod/modelA","messages":[{"role":"user","content":"hello"}],"stream":true}`
	resp2, err := client.Post(proxyTestServer.URL+"/v1/chat/completions", "application/json", strings.NewReader(reqBody2))
	if err != nil || resp2.StatusCode != http.StatusOK {
		t.Fatalf("failed post to prod/modelA: %v", err)
	}
	body2Bytes, _ := io.ReadAll(resp2.Body)
	_ = resp2.Body.Close()

	if upstream2ReceivedModel != "modelA" {
		t.Fatalf("expected upstream 2 to receive 'modelA', got %q", upstream2ReceivedModel)
	}
	if !strings.Contains(string(body2Bytes), "prod/modelA") {
		t.Fatalf("expected downstream response to have model 'prod/modelA', got: %s", string(body2Bytes))
	}
	if !strings.Contains(string(body2Bytes), "upstream2-reply") {
		t.Fatalf("expected downstream response to have 'upstream2-reply', got: %s", string(body2Bytes))
	}
}
