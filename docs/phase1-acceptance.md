# Phase 1 验收记录

本文记录 2026-08-31 在本机 Docker 环境中的可重复验收结果。它只证明 Phase 1 的
live 采集链路能工作，不代表牛客历史全量，也不评价正文解析质量。

## 已完成证据

- `ruff check .` 通过；`pytest -q` 为 14 passed，其中 11 条单元测试、3 条 MySQL
  集成测试。
- 面经中心匿名请求第 1 页成功，识别 20 条 feed/discussion；sitemap 根索引递归到
  `sitemap1.xml`，在 100 URL 安全上限内识别 98 条 discussion。
- 重复运行 Scheduler 时，MySQL 以 `(page_type, external_id)` 合并页面；
  `page_sources` 保留 center/sitemap 来源。未成功页面允许被重复发布。
- 真实 discussion 页面成功完成 HTTP 200、gzip 原子落盘、MySQL success commit、
  ACK。集成测试还在 ACK 回调中从独立数据库会话验证了 success 和 gzip 已可见。
- ACK 前崩溃实验使用 `FAILPOINT_AFTER_SUCCESS_COMMIT=true`：page 3 已写 gzip 并提交
  success，Worker 随后以状态 1 退出；消息从 unacked 回到 ready。恢复 Worker 收到
  redelivered 消息后只执行 `duplicate_success_ack`，`fetch_attempts` 仍只有一次 HTTP。
- RabbitMQ 持久性实验：`fetch.ready` 的 136 条 persistent 消息及隔离测试队列的
  1 条消息在 RabbitMQ 容器重启后均保留。隔离队列随后已删除。

## 自动测试覆盖

单元测试覆盖 feed/discussion identity、canonical URL、center 连续旧页停止、HTTP
错误分类、最多三次进程内 retry、永久错误不重试、风控模板判定和 gzip 原子替换。

MySQL 集成测试覆盖：重复发现最终只有一个 page 和一条对应 lineage；Scheduler 只发布
pending 与 retryable failed；成功 ACK 之前 gzip 和 MySQL success 必须已经提交。

## Smoke test

状态：通过。2026-08-31 00:11（Asia/Shanghai）启动 `smoke-worker-1` 和
`smoke-worker-2`，启动核验时 RabbitMQ 为 2 consumers、481 ready、4 unacked。
Worker 持续消费至约 00:28:50，之后保持连接空闲；从启动到人工检查超过半小时。

最终结果：

- `fetch.ready` 为 0 ready、0 unacked；检查完成后两个空闲 Worker 已停止，consumer 为 0；
- 358 个 page 全部 success，358 个 identity、canonical URL 和规范 gzip 均唯一；
- success 页面均有 gzip path 与 SHA256，`data/raw` 有 358 个 gzip、0 个临时文件；
- `smoke-worker-1` 完成 178 次请求，`smoke-worker-2` 完成 179 次请求；
- 两个 smoke 日志均为 0 次 429、0 blocked、0 ERROR；
- 三次 Scheduler run 均为 success；center 有 60 条 lineage，sitemap 有 298 条，
  合计 358 条 lineage 且唯一；
- 最终 `ruff check .` 通过，完整测试为 14 passed。

本轮只完成 Phase 1 smoke，不自动启动下一轮 Scheduler。后续增量采集必须在人工确认后
另行启动。复查时可同时看 Worker 日志、RabbitMQ 队列、MySQL 聚合和 `data/raw`。
