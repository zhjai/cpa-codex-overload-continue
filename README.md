# CPA Codex overload recovery

这是一个 CLIProxyAPI 动态插件，用于识别 Codex Responses 的容量错误。设计参考了
Sub2API 的处理方式：容量错误可能藏在 HTTP 200 的 SSE `response.failed` 事件里，不能只
按 HTTP 503 判断。

本版本（0.2.0）已经删除“伪造 assistant 文本再发送 Continue”的续写机制。那种做法无法
可靠保留 Responses 的 response lineage、`previous_response_id`、encrypted reasoning、工具
调用和副作用边界。

## 行为

- 识别 `server_is_overloaded`、`server_overloaded`、`slow_down`、
  `service_unavailable_error`，以及 `Selected model is at capacity`、
  `Our servers are currently overloaded` 等明确容量文案。
- `response.output_text.delta`、reasoning、encrypted output、图像/音频等语义输出，以及任意
  function/custom/MCP/shell/computer/apply_patch/tool 调用都会阻止安全重放；工具事件采取保守
  处理。
- 输出前的重试默认关闭（`max_pre_output_retries: 0`）。CPA 原生的
  `stream-bootstrap-buffering` 和 credential failover 应优先负责这一阶段。插件最多允许显式
  开启一次 plugin-level 重试，避免重复请求失控。
- 输出后的默认行为是 `fail_closed`：保留原始容量错误，不重放请求。
- 实验性 `text_only` 模式只在已经有语义文本/推理输出、且没有任何工具事件时，将错误副本
  的 code/type 改为 `server_error`。它保留 SSE event、response id、sequence 和其他字段，
  让客户端自行决定是否退避重试。是否触发目标 Codex CLI 的内置 retry，必须用真实 CLI 验证，
  本仓库没有对此作已验证声明。
- 原生 WebSocket passthrough 的错误事件不能由当前插件 API 安全修改；插件 executor 接管
  请求会失去原生 WebSocket steering。要同时覆盖 HTTP 和原生 WebSocket 并保留 steering，
  应在 CPA host 层实现同样的统一错误处理。

## 配置

启用时必须提供模型 allowlist，避免插件无意接管所有 Responses 请求：

```yaml
enabled: true
provider: codex
models:
  - gpt-6-astra
post_output_capacity: fail_closed
max_pre_output_retries: 0
backoff_base_ms: 250
backoff_max_ms: 4000
```

`post_output_capacity: text_only` 是实验开关，建议先在隔离 CPA 实例和无工具请求中验证。
旧版 `max_pre_commit_retries`、`max_continuations`、`continuation_mode`、`continue_prompt`、
`rewrite_response_id` 会被明确拒绝，避免旧配置静默启用不安全的合成续写。

## 构建与测试

CPA 当前 SDK 要求 Go 1.26。模块位于 `go/`：

```bash
cd go
/path/to/go1.26/bin/go test ./...
/path/to/go1.26/bin/go test -race ./...
/path/to/go1.26/bin/go build -buildvcs=false -buildmode=c-shared \
  -o ../dist/codex-overload-continue.so .
```

自动测试覆盖跨 chunk SSE、嵌套 `response.failed`、无 code 的容量文案、`slow_down`、非容量
错误、reasoning/encrypted output、工具调用检测、fail-closed 和 `server_error` 改写形状。
它们不能替代真实 CPA host、真实上游、取消请求、账号切换或 Codex CLI retry 的集成测试。

## Arena 依据

本设计经过两轮独立 Arena：199 服务器上的 Claude Code Opus 与本地 Codex 子代理。两方都
认为 Sub2API 风格优于合成续写，但指出 CPA 当前 SDK 对 stream error 和原生 WebSocket
的可变性不足，因此 v0.2 只做保守插件边界；完整 HTTP + WebSocket 覆盖应提交 CPA host
层修复。
