# agent-trace-gateway

会话级 agent 轨迹网关：Pingora 薄 filter，部署在用户与 sub2api 之间，解包 Anthropic Messages / OpenAI Responses / OpenAI chat completions 三种协议的请求与 SSE/WS 流式响应，重组为会话级轨迹导出至 Langfuse。

## 开工决定记录

- 仓库归属：**Vitus213**（用户决定，不进 Alle-Group）；
- 规格与压力检验：见 sub2api 仓库 `openspec/changes/extract-agent-trace-gateway/`（proposal/specs/design/tasks，CLI 严格校验通过）；
- 开工决定：远端仓库由 Vitus213 名下创建（tasks 0.2 授权门禁已满足）；
- fixtures：`xtask/harness/fixtures/` 为真实抓包样本（去凭据，版本库卫生要求；运行时轨迹按 D4 原样记录，两者互不影响）。

## 目录

- `src/`：网关实现
- `xtask/harness/`：回归 harness（协议 fixture 上游 + 测试驱动客户端）与真实样本 fixtures
- `tests/`：行为测试（TDD 主战场）
