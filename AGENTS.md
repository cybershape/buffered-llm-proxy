# AGENTS.md

## 项目说明

本项目为高性能、基于语义分段聚合的 OpenAI 兼容 Chat Completions 流式聚合代理（Buffered Streaming Proxy），采用 Go 编写。

## 架构与核心原则

1. **并发解耦模型**：
   - Upstream Reader 与 Downstream Writer 完全并发解耦。
   - Reader 持续以网络最快速度读取上游 SSE 数据，解析为语义分段（Semantic Segments）写入有界缓冲区。
   - Writer 由下游网络完成驱动（Downstream-completion-driven），每次写入完毕后通过原子 Swap 提取当前累积的 snapshot 并下发。
   - 下游越慢，批次自动越大；下游越快，输出越接近实时。

2. **严格语义顺序与 Barrier**：
   - 保持 upstream 语义事件顺序，严禁 cross-segment 乱序。
   - Reasoning -> Content 为 Barrier，禁止跨边界混合。
   - Content -> Tool Call 为 Barrier。
   - Tool Call -> Content 为 Barrier。
   - `finish_reason`、`usage`、`[DONE]`、`error` 为控制 Barrier，保证之前数据全部下发后再发出。

3. **协议与格式兼容**：
   - Reasoning 字段保留原始名称（如 `reasoning_content`、`reasoning` 等），不自行转换 schema。
   - Tool Call arguments 采用逐字节拼接，严禁中途 parse/stringify JSON。
   - 保留公共 Metadata（id、model、system_fingerprint 等），透传未知扩展字段。
   - `/v1/models`、`POST /v1/completions` 以及 `stream=false` 请求全透明透传。

## 工程规范

- 代码风格遵循 Go 官方标准（`gofmt`），无冗余注释。
- 提交前必须通过 `-race` 竞态检测：`go test -race ./...`。
- 修改聚合规则时需同步维护 `fixtures/` 与 `tests/` 测试用例。
