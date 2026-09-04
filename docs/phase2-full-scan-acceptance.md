# Phase 2 完整 live 抓取验收报告

本文记录 2026-09-03 至 2026-09-04 在本机环境中执行的一次完整 live 抓取。
本次运行处理的是 Experience API 配置窗口与当次 sitemap 暴露的 feed/discussion
页面，不代表牛客历史全量，也不包含 Common Crawl、Wayback、正文解析或 NLP 分类。

## 验收结论

系统采集链路验收通过，数据批次有条件通过。

25,599 条页面任务已经全部离开 RabbitMQ 队列，其中 25,588 页完成 HTTP 200、gzip
落盘、MySQL success commit 和 ACK，成功率为 99.96%。剩余 11 页均返回 HTTP 200，
经逐页复查确认是正常公开帖子，被过宽的“安全验证”全文关键词误判为 blocked，并非
真实的 403、429、验证码或访问限制。

误判逻辑已在 `ede9e65 fix: avoid blocking pages mentioning security validation` 中修复。
这 11 页尚未恢复为 pending 并补抓，因此本批数据完整性仍有一项明确收尾工作。

## 运行环境与参数

- Windows 宿主机，MySQL 8.0 与 RabbitMQ Management 通过 Docker Compose 运行；
- RabbitMQ 队列为 durable `fetch.ready`，消息持久化，Worker 手动 ACK；
- 4 个 Worker 共同消费，每个 Worker `prefetch=2`、HTTP concurrency=1；
- 每个 Worker 使用 5 秒基础间隔和 0–2 秒 jitter；
- 单页面临时错误最多请求 3 次；
- 原始 HTML 使用 gzip 临时文件和 `os.replace()` 写入固定页面路径；
- HTTP 请求继承系统 `HTTP_PROXY`/`HTTPS_PROXY`，本轮实际经过
  `http://127.0.0.1:7890`；
- Scheduler 使用 `full-scan`，先发布已有 backlog，再运行 Experience API 与 sitemap
  discovery，并在每批 MySQL commit 后发布新页面。

## 时间线

所有时间均为 Asia/Shanghai。

| 时间 | 事件 |
| --- | --- |
| 2026-09-03 19:08 | 启动一次性 `full-scan` Scheduler 和首批 4 个 Worker |
| 2026-09-03 19:13 | Scheduler 完成 backlog 发布和本轮 discovery，crawl run 为 success |
| 2026-09-03 20:31 | Codex 关闭导致 Worker 进程退出；RabbitMQ 将未 ACK 消息恢复为 Ready |
| 2026-09-03 21:05 | 队列确认有 22,268 Ready、0 Unacked、0 Consumer，重新启动 4 个 Worker |
| 2026-09-04 06:22 | 最后一条页面处理完成，队列清空 |
| 2026-09-04 10:30 后 | 确认 0 Ready、0 Unacked；人工停止 4 个空闲 Worker，Consumer 归零 |

## 最终结果

| 指标 | 结果 |
| --- | ---: |
| pages 总数 | 25,599 |
| success | 25,588 |
| failed / blocked | 11 |
| pending | 0 |
| 成功率 | 99.96% |
| HTTP 200 success attempts | 25,588 |
| HTTP 200 blocked attempts | 11 |
| 临时网络 retry | 1 |
| HTTP 403 | 0 |
| HTTP 429 | 0 |
| RabbitMQ Ready / Unacked / Consumers（收尾后） | 0 / 0 / 0 |
| 数据库引用的唯一 gzip path | 25,588 |
| 数据库引用的唯一 body SHA256 | 25,588 |
| 磁盘 gzip 总体积 | 约 0.62 GiB |

稳定运行阶段 4 个 Worker 合计约 40 页/分钟，即约 2,400 页/小时。首次进程退出前已
完成 3,330 页；恢复后的 4 个 Worker 完成剩余 22,258 个 success，并产生 10 个
blocked 误判。各 Worker 请求量接近，RabbitMQ 消费分配没有出现明显倾斜。

## 关键语义实测

### 完整 discovery 与发布

Scheduler 先恢复数据库中已有 pending backlog，再执行 Experience API 与 sitemap
discovery。crawl run 最终为 success，新增页面在 batch commit 后进入 `fetch.ready`。
Worker 消费期间 Scheduler 已退出，证明发现、发布和长时间抓取不要求 Scheduler 常驻。

### 多 Worker 与 prefetch

4 个独立 Worker 同时连接同一个 durable queue。RabbitMQ 运行中稳定显示 4 consumers、
8 unacked，符合每个 Worker `prefetch=2`；每个 Worker 内仍按 concurrency=1 串行发起
HTTP 请求。

### ACK 与崩溃恢复

Codex 关闭后所有 Worker 进程退出。当时队列恢复为 22,268 Ready、0 Unacked、0
Consumer，没有任务停留在未确认状态。重新启动 Worker 后继续从原队列消费，未重新运行
Scheduler，也未调用 `publish-pending`，最终队列正常清空。这次真实中断验证了 ACK 前
消息由 RabbitMQ 自动 redelivery 的路径。

### 进程内 retry

全程只有 1 次无 HTTP 状态的临时网络错误。该请求进入 Worker 内 retry，随后对应页面
成功完成，没有产生 retryable failed backlog。全程没有 HTTP 429 或 403。

### 幂等与存储

25,588 个 success 页面分别对应 25,588 个唯一 gzip path 和 25,588 个唯一 SHA256。
页面状态、固定文件路径与消费前 success 检查共同保证重复投递不会产生重复页面记录或
重复快照。

磁盘实际存在 25,888 个 gzip，比当前数据库引用多 300 个。这 300 个文件来自此前测试、
smoke run 或数据库重建后遗留的 orphan blob。Phase 2 明确不实现 GC，因此该差异属于
已知设计结果，不影响本轮 25,588 个数据库引用文件的完整性。

## 11 个 blocked 误判

失败记录包含 4 个 discussion 和 7 个 feed。逐页重新匿名请求时，11 页均返回 HTTP 200、
最终 URL 与页面身份匹配，并具有正常帖子标题。9 页可以直接定位到正文里的“功能安全验证”
或“方案安全验证”；另外 2 页复查时不再出现 blocked 关键词，但页面标题和身份仍正常，
原始失败响应也有 266–400 KiB，排除风控模板或空壳响应。

根因是 Worker 曾将宽泛的“安全验证”作为全文 blocked marker。正常的芯片、汽车功能安全、
Agent 和软件工程面经频繁使用这个词，单个子串不足以证明页面被拦截。修复只删除该宽泛
marker，仍保留“访问过于频繁”“请登录后继续访问”和 `verifycenter` 等更明确特征。

## 自动测试

长跑开始前，隔离的 `nowcoder_test` 数据库完整测试为 43 passed，Ruff 通过。误判修复后，
Fetcher 相关单元测试为 6 passed，Ruff 通过，并新增“正常正文包含功能安全验证不应 blocked”
的回归用例。

2026-09-04 恢复 Docker Desktop 后，MySQL 与 RabbitMQ 均为 healthy。使用独立的
`nowcoder_test` 数据库重新运行完整测试，结果为 44 passed，Ruff 通过。测试过程中未连接
或重建正式采集数据库 `nowcoder`。

## 待完成事项

1. 将 11 个误判页面恢复为 pending，重新发布并抓取；
2. 核对 11 个页面全部 success、RabbitMQ 再次归零；
3. 完成以上两项后，将本报告结论更新为数据批次完全通过。

