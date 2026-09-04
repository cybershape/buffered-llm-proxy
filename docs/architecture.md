# 架构设计与聚合引擎机制

## 1. 系统总体架构

```text
+-------------+         +------------------+         +--------------------+         +------------+
| AI Provider | ------> | Upstream Reader  | ------> | PendingBuffer      | ------> | Downstream | ------> Client
| (Upstream)  | (SSE)   | (Fast Ingestion) | (Mutex) | (High/Low Water)   | (Swap)  | Writer     | (SSE)
+-------------+         +------------------+         +--------------------+         +------------+
```

系统核心目标是消除传统基于固定定时器（如 50ms timer）带来的延迟开销或过度细碎的网络写入，建立基于**下游完成驱动（Downstream-Driven Snapshot）**的自适应弹性聚合机制。

---

## 2. 核心并发解耦模型

- **Upstream Reader**:
  - 启动独立协程，以网络最快速度消费 Upstream 返回的 HTTP 响应体。
  - 通过 `sse.Reader` 逐帧还原 SSE 事件（容忍半包、多包、行前导空格及注释行）。
  - 将 SSE JSON 解析提取出标准化的 `semantic.Segment`，调用 `PendingBuffer.Append()` 存入。
  - 在下游发送数据的耗时窗口期内，Reader 依然并发无阻地摄入并聚合上游推送的后续增量。

- **Downstream Writer**:
  - 采用基于下游完成触发的循环：等待 PendingBuffer 中出现数据。
  - 执行 `PendingBuffer.Swap()` 原子取走当前快照（Snapshot），并将缓冲区置空。
  - 将快照中的各语义分段序列化为标准 OpenAI SSE 格式并下发，调用 Flush 刷入网络。
  - 发送完毕后立即发起下一轮 Swap。
  - **自适应特性**：下游网络或客户端处理越慢，每次发送耗时越长，Reader 累积并聚合的数据越多，下一批合并粒度自动变大；当下游极快时，立即取走，延迟接近零。

---

## 3. 内存有界缓冲与反压（Backpressure）

为了避免极端慢客户端导致内存无限膨胀，系统引入了高低水位反压机制：

- `high_watermark`（默认 32MB）：
  - 当 `currentBytes >= high_watermark` 时，Upstream Reader 进入等待状态（`sync.Cond.Wait`），暂停从上游 Socket 读取。
  - TCP 接收窗口填满后自然向上游服务传导反压，防止服务端爆内存。
- `low_watermark`（默认 24MB）：
  - 当 Downstream Writer 消费并 Swap 提取数据后，缓冲区占用降至低水位以下，触发广播唤醒（`sync.Cond.Broadcast`），Upstream Reader 恢复高速读取。
- 完整指标记录 Reader 暂停次数与累计挂起耗时。

---

## 4. 语义分段与合并规则

### 4.1 核心分段类别
1. `REASONING_DELTA`：思维链 token（支持 `reasoning_content`、`reasoning`、`reasoning_text`、`thought` 等字段，输出保持原字段名）。
2. `CONTENT_DELTA`：正文增量（`delta.content`）。
3. `TOOL_CALL_DELTA`：工具调用（按 `choice.index` 与 `tool_call.index` 双重隔离）。
4. `ROLE`：初始角色声明。
5. `FINISH`：终止状态（`finish_reason`）。
6. `USAGE`：用量统计。
7. `ERROR`：错误事件。
8. `DONE`：`[DONE]` 控制信号。
9. `UNKNOWN`：未知或自定义扩展事件。

### 4.2 合并规则与 Barrier 原则
- **Role 幂等与非 Barrier 特性**：
  - 同一 choice 中首次出现的 `role`（如 `"role": "assistant"`）输出一次。
  - 上游后续 chunk 重复出现的相同 `role` 自动忽略，不生成额外分段，**绝不成为 barrier**，确保携带重复 role 的 content/reasoning 能够无缝连续聚合。
  - 相邻的相同 `RoleSegment` 天然允许合并吸收。
- **同质合并**：
  - 相邻 `ReasoningSegment`（相同 choice 与相同字段）文本自动追加。
  - 相邻 `ContentSegment`（相同 choice）文本自动追加。
  - 相邻 `ToolCallSegment`（相同 choice）的参数片段（arguments）逐字节拼接。严禁在中途对参数做 JSON parse/stringify，避免破坏未闭合 JSON 及空格转义。
- **异质 Barrier**：
  - `Reasoning -> Content` 构成 Barrier，禁止混合。
  - `Content -> Tool Call` 构成 Barrier，严格保持前后发生顺序。
  - `Tool Call -> Content` 构成 Barrier。
  - `Finish`、`Usage`、`[DONE]`、`Error` 为严格 Barrier，确保此前累积的分段全部发出后才下发。
  - 未知事件（`UNKNOWN`）作为 Barrier 原样安全透传。
