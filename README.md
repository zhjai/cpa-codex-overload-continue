# CPA Codex overload recovery

这是一个 CLIProxyAPI 动态插件原型，用于处理 Codex Responses 流在上游返回
`server_overloaded` 时中断的问题。

它采用两个独立审查意见的交集：199 服务器上的 Claude 认为必须由
`ModelRouter + Executor` 接管完整响应流；Codex `gpt-6-astra` 子代理检查了实现，
指出编译、分片 SSE、凭据固定、工具调用识别和 response lineage 的风险。插件因此
默认只做安全的早期 overload retry，文本续写默认关闭。

## 行为边界

- overload 在任何输出提交前发生时，插件最多按配置重试，并使用退避。
- 只有检测到纯文本输出、没有任何工具或副作用调用时，才允许显式开启一次文本续写。
- `function_call`、`custom_tool_call`、`tool_search_call`、`computer_call`、`shell_call`、
  `mcp_call`、`apply_patch` 等调用会 fail closed，不会重放。
- 插件不能在已经终止的原生 Codex WebSocket turn 中注入用户消息；它只能在请求仍由
  插件托管时把续写事件拼接到同一条 HTTP/SSE 响应中。
- 接管请求会失去 CPA 原生 WebSocket passthrough 和 mid-turn steering。
- 当前仓库没有真实 CPA host + Codex CLI 集成验证，因此不能把文本续写当作已验证功能。

## 构建

CPA 当前 SDK 要求 Go 1.26。示例使用隔离工具链：

```bash
cd go
/path/to/go1.26/bin/go mod tidy
/path/to/go1.26/bin/go test ./...
/path/to/go1.26/bin/go test -race ./...
/path/to/go1.26/bin/go build -buildvcs=false -buildmode=c-shared \
  -o ../dist/codex-overload-continue.so .
```

`dist/codex-overload-continue.so` 是构建产物；不要把它直接复制到正在运行的 CPA，
先在隔离的 CPA 实例中完成 host callback 和真实 Codex CLI 集成测试。

## 配置示例

```yaml
enabled: true
provider: codex
models:
  - gpt-5.5
max_pre_commit_retries: 2
backoff_base_ms: 250
backoff_max_ms: 4000

# 最安全的默认值。确认真实客户端接受拼接后的 SSE 后再改为 text。
continuation_mode: disabled
max_continuations: 0
rewrite_response_id: true
continue_prompt: "Continue exactly where you stopped. Do not repeat text already produced; finish the response."
```

实验性文本续写配置：

```yaml
continuation_mode: text
max_continuations: 1
```

启用前必须验证：拆分 SSE 帧、同一 response id、取消请求、encrypted reasoning、
Responses `previous_response_id`/输入历史以及真实 Codex CLI 对续写事件的处理。当前实现
只携带已观察到的文本作为 assistant message，无法恢复完整的原生 Responses item lineage。

## 审查与验证状态

199 服务器 Claude 的独立设计审查结果保存在任务归档中；Codex 子代理对本仓库的审查
结论是“修复编译和安全边界后可作为 opt-in 原型发布，未通过 live integration 前不得部署”。
本仓库的自动检查目前覆盖配置路由、SSE 跨 chunk、overload 分类、文本状态、工具调用
变体和 response id 重写；没有声称覆盖真实 CPA host、上游副作用或 Codex CLI。
