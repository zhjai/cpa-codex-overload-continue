# CPA Codex overload recovery

这是一个 CLIProxyAPI 动态插件，用于识别 Codex Responses 的容量错误。设计参考了
Sub2API 的处理方式：容量错误可能藏在 HTTP 200 的 SSE `response.failed` 事件里，不能只
按 HTTP 503 判断。

本版本（0.2.1）已经删除“伪造 assistant 文本再发送 Continue”的续写机制。那种做法无法
可靠保留 Responses 的 response lineage、`previous_response_id`、encrypted reasoning、工具
调用和副作用边界。

## 行为

- 识别 `server_is_overloaded`、`server_overloaded`、`slow_down`、
  `service_unavailable_error`，以及 `Selected model is at capacity`、
  `Our servers are currently overloaded` 等明确容量文案。
- `response.output_text.delta`、reasoning、encrypted output、refusal、转录、图像/音频/视频等语义输出，以及任意
  function/custom/MCP/shell/computer/apply_patch/tool 调用都会阻止安全重放；工具事件采取保守
  处理。
- 插件层不重放请求。`max_pre_output_retries` 仅为兼容保留且必须为 `0`；嵌套执行不会继承
  外层 `AuthID`，输出前故障由 CPA 原生 `stream-bootstrap-buffering` 和 credential failover
  处理，避免锁死单一凭据或绕过账号选择。
- 要处理“HTTP 200 已发送 `response.created`，随后才收到 `response.failed`”这一容量场景，
  CPA 必须同时开启 `codex.stream-bootstrap-buffering: true`；仅设置
  `streaming.bootstrap-retries` 不够。建议给 buffering 设置有限的
  `stream-bootstrap-timeout`，防止代理长时间等待首个生成事件：

  ```yaml
  codex:
    stream-bootstrap-buffering: true
    stream-bootstrap-timeout: "20s"
  ```

  这项由 CPA host 执行凭据切换，插件本身不重放请求。启用 Home 控制面时，CPA 会禁用本地
  插件 executor 路由；当前插件不绕过这个限制。
- 输出后的默认行为是 `fail_closed`：保留原始容量错误，不重放请求。
- 实验性 `text_only` 模式只在已经产生纯文本输出、且没有 reasoning、encrypted、图像、音频或
  工具输出时，将容量错误的 code/type 改为 `server_error`；缺少 code/type 的明确容量错误会补齐
  这两个字段。插件边界会保留结构化错误的其他字段，但 CPA Responses handler 会在发给客户端前
  重建终止错误帧，通常只保留 `sequence_number` 和清理后的 error 节点。纯文本容量错误会被规范化
  为 error JSON，但不会伪造 response lineage。是否触发目标 Codex CLI 的内置 retry，必须用真实
  CLI 验证，本仓库没有对此作已验证声明。
- 原生 WebSocket passthrough 的错误事件不能由当前插件 API 安全修改；插件 executor 接管
  请求会失去原生 WebSocket steering。要同时覆盖 HTTP 和原生 WebSocket 并保留 steering，
  应在 CPA host 层实现同样的统一错误处理。
- CPA 插件 API 没有嵌套模型的 token-count callback；插件对 `CountTokens` 返回明确的 501，
  不再伪报 `input_tokens: 0`。异步 stream 握手也只能先返回 `Content-Type`，上游响应头不会透传。

## 配置

启用插件时必须填写非空 `models` allowlist，避免接管其他 Responses 路由：

```yaml
enabled: true
provider: codex
models:
  - gpt-6-astra
post_output_capacity: fail_closed
max_pre_output_retries: 0
```

`post_output_capacity: text_only` 是实验开关，建议先在隔离 CPA 实例和无工具请求中验证。
旧版 `max_pre_commit_retries`、`max_continuations`、`continuation_mode`、`continue_prompt`、
`rewrite_response_id`、`backoff_base_ms`、`backoff_max_ms` 会被明确拒绝。其他未知字段也会导致
配置失败，避免拼写错误或旧设置被静默忽略。

## 构建与测试

CPA 当前 SDK 要求 Go 1.26。模块位于 `go/`：

```bash
cd go
/path/to/go1.26/bin/go test ./...
/path/to/go1.26/bin/go test -race ./...
/path/to/go1.26/bin/go build -buildvcs=false -buildmode=c-shared \
  -o ../dist/codex-overload-continue.so .
```

自动测试覆盖标准和 CPA 逐行 chunk SSE、多行 data、嵌套 `response.failed`、无 code 的容量文案、
明确容量错误识别、非容量 429/503 保留、reasoning/encrypted output、工具调用检测、空流错误、
RPC read 错误、内外 stream ID 隔离和单次嵌套执行。
它们不能替代真实 CPA host、真实上游、取消请求、账号切换或 Codex CLI retry 的集成测试。

## Arena 依据

本设计经过 Codex 子代理和 Claude Code Opus 的多轮独立审查。前期远端 Claude 审查完成；后续
远端 SSH 不可用时，按用户授权改用本地 Claude 复审当前修复。两方都认为 Sub2API 风格优于
合成续写，但指出 CPA 当前 SDK 对 stream error 和原生 WebSocket 的可变性不足，因此 v0.2
只做保守插件边界；完整 HTTP + WebSocket 覆盖应提交 CPA host 层修复。
