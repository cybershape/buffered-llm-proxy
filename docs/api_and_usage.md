# 接口指南与运行配置

## 1. 构建与运行

### 构建二进制
```bash
go build -o buffered-proxy ./cmd/proxy
```

### 启动代理服务
```bash
./buffered-proxy \
  -host 0.0.0.0 \
  -port 8080 \
  -upstream http://127.0.0.1:8000 \
  -max-buffer-mb 32 \
  -low-water-mb 24
```

---

## 2. 命令行参数详解

| 参数名 | 默认值 | 说明 |
| :--- | :--- | :--- |
| `-host` | `0.0.0.0` | 代理服务绑定的监听 IP / Host |
| `-port` | `8080` | 代理服务监听端口 |
| `-upstream` | `http://127.0.0.1:8000` | 上游 AI Provider 或 CLIProxyAPI 地址 |
| `-max-buffer-mb` | `32` | 单请求内存 Buffer 高水位阈值（MB），超过触发反压暂停 Upstream Reader |
| `-low-water-mb` | `24` | 单请求内存 Buffer 低水位阈值（MB），缓冲区回落至此值以下恢复 Upstream Reader |
| `-min-coalesce-ms` | `0` | 可选微延迟汇聚等待时间（毫秒），默认为 0（纯下游完成驱动） |
| `-enable-metrics` | `true` | 是否开放 `/metrics` 监控端点 |

---

## 3. 支持端点与路由行为

### 3.1 `POST /v1/chat/completions` (流式聚合)
当请求体包含 `"stream": true` 时触发流式语义聚合机制：
- 自动建立上游 Reader 协程与下游 Writer 写入流程。
- Reasoning（思维链）、Content（正文增量）、Tool Calls（工具调用参数）按语义规则安全聚合。
- 重复 `role` 自动忽略，不构成 barrier。
- 顺序与边界（Reasoning -> Content -> Tool Calls -> Content -> Finish）严格保序。
- 上游输出错误或异常非 200 响应时，自动透传对应状态码与错误内容。

#### 示例请求
```bash
curl -N -X POST http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{
    "model": "deepseek-r1",
    "stream": true,
    "messages": [{"role": "user", "content": "你好"}]
  }'
```

---

### 3.2 `POST /v1/chat/completions` (`stream: false` 透明透传)
当请求体未设置 `stream` 或 `"stream": false` 时，执行无损反向代理透明透传。

---

### 3.3 `GET /v1/models` (透明透传)
直接代理上游模型列表接口，保留原始 Headers 及 JSON 响应体。

#### 示例请求
```bash
curl http://localhost:8080/v1/models \
  -H "Authorization: Bearer $API_KEY"
```

---

### 3.4 `GET /metrics` (监控与统计)
返回代理服务自启动以来的累积统计与聚合比率指标：

```json
{
  "upstream_sse_events": 1420,
  "downstream_sse_events": 118,
  "overall_coalescing_ratio": 12.03,
  "reasoning_fragments_in": 600,
  "reasoning_events_out": 42,
  "reasoning_coalescing_ratio": 14.28,
  "content_fragments_in": 520,
  "content_events_out": 40,
  "content_coalescing_ratio": 13.00,
  "tool_fragments_in": 300,
  "tool_events_out": 36,
  "tool_coalescing_ratio": 8.33,
  "upstream_bytes": 1048576,
  "downstream_bytes": 985120,
  "pending_bytes_max": 204800,
  "reader_pause_count": 0,
  "reader_pause_duration_ns": 0,
  "downstream_write_ns": 125000000
}
```
