# 牛客公开帖子采集系统 Phase 2 Spec

状态：已实现并完成验收（2026-09-03）
分支：`phase2-live-discovery`
范围：当前 live 来源的完整发现流程，不做历史回填和正文分类

## 1. Phase 2 要解决的问题

Phase 1 已经完成 Scheduler、RabbitMQ、双 Worker、手动 ACK、进程内 retry、MySQL 幂等和 gzip 原始页面保存。Phase 2 不重做这些能力，重点修正 discovery 的语义和执行方式。

本阶段的目标是：在一次运行中，执行两个配置好的 live discovery 范围，将发现的 feed/discussion URL 持续写入 MySQL。每个 discovery batch 在事务提交后立即把本批新产生的待抓取页面发布给现有 Worker，不再等待全部来源结束。

两个来源是：

- 牛客 sitemap；
- 牛客面经广泛列表 API。

这里的“完整”不表示牛客历史全量，也不表示已经穷尽 API 的全部深页。Sitemap 要遍历当次根入口列出的全部文档；Experience API 只扫描配置的前 20 页窗口，或在更早遇到空页、短页时结束。未被这两个配置范围暴露的页面，未来可以由其他查询或历史数据源补充，但不属于本系统的 Phase 2。

Discovery 只确认目标是合法的 feed/discussion 页面，不负责判断帖子内容是否真的是面经。API 是一个面经相关、高相关度但返回窗口有限的发现入口；sitemap 是覆盖范围更广的入口。内容分类和手撕题提取留给后续 NLP 流程。

## 2. 非目标

Phase 2 不实现：

- 公司、岗位或公司×岗位查询分区；
- 公司名 suggest 枚举或 company ID 暴力遍历；
- 登录、Cookie 刷新、验证码或风控绕过；
- Common Crawl、Wayback 和其他历史回填；
- 正文解析、面经分类和手撕题提取；
- Worker 并发扩容或严格跨进程限流；
- 新的 RabbitMQ queue、retry queue、DLQ 或 outbox；
- 新的数据表、Alembic 或异步 MySQL 驱动；
- snapshot 历史、内容寻址和 GC；
- 为每个 crawl run 建立独立任务关联；
- Prometheus、管理后台、Redis、Kafka 或 Kubernetes。

现有 Worker、gzip 路径、四张表和 RabbitMQ 消息协议保持不变。

## 3. 总体架构

```text
                              SchedulerWorkflow
                                      │
                 full-scan 启动时发布已有 pending/retryable
                                      │
                         ┌────────────┴────────────┐
                         │                         │
               ExperienceApiDiscoverer     SitemapDiscoverer
                  同一查询串行翻页           sitemap 文档串行
                         │                         │
                         └──── DiscoveredPage ────┘
                                      │
                         asyncio.Queue(maxsize=1000)
                                      │
                              DiscoveryWriter
                            批内聚合，最多 200 条
                                      │
                         asyncio.to_thread(write_batch)
                                      │
                       MySQL commit pages / page_sources
                                      │
                 CommittedBatchResult(dispatchable_page_ids)
                                      │
                         Scheduler / DiscoveryService
                                      │
                              RabbitPublisher
                                      │
                      RabbitMQ fetch.ready → 原有 Worker
```

职责边界：

- `ExperienceApiDiscoverer` 只处理广泛列表 API 的请求、解析、翻页和停止判断；
- `SitemapDiscoverer` 只处理 sitemap 入口递归和 URL 解析；
- `DiscoveryService` 负责两个 producer 的并发、队列生命周期、取消、已提交 batch 的分发和结果汇总；
- `DiscoveryWriter` 是 discovery 唯一的 MySQL 写入出口，负责批量事务和计数，并在 commit 成功后返回 `CommittedBatchResult`；
- `DiscoveryWriter` 和数据库层不导入、不持有也不调用 RabbitMQ Publisher；
- `SchedulerWorkflow` 只编排 backlog publish、discovery 和实时 post-commit dispatch，不包含具体翻页或 SQL；
- `RabbitPublisher` 和 Worker 继续沿用 Phase 1 行为。

## 4. 统一发现对象

两个 discoverer 都输出 `DiscoveredPage`，而不是直接操作数据库：

```text
identity.page_type       feed / discussion
identity.external_id     feed UUID / discussion 数字 ID
identity.canonical_url   去 query 和 fragment 后的规范 URL
source_type              持久化兼容值 center / sitemap
source_key               具体来源键
source_modified_at       API editTime 或 sitemap lastmod
company_ids              API 固定为空元组
job_id                   API 固定为 -1
job_level                API 固定为 1
source_page              API 页码；sitemap 为 null
```

代码和 CLI 将原来的“center”概念改称 `experience-api`，对应类名为 `ExperienceApiDiscoverer`，文件为 `discovery/experience_api.py`。为了兼容已有 `page_sources`，数据库继续保存 Phase 1 的稳定标识：

```text
source_type = center
source_key  = center::-1:1
```

这里的 `center` 只是历史持久化协议名，不代表系统已经把页面内容分类为面经。sitemap 继续使用直接父级 sitemap URL 构造 `source_key`。

## 5. Experience API discovery

### 5.1 固定广泛查询

使用匿名接口：

```text
POST https://gw-c.nowcoder.com/api/sparta/job-experience/experience/job/list
```

固定业务参数：

```json
{
  "companyList": [],
  "jobId": -1,
  "level": 1,
  "order": 3,
  "page": 1,
  "isNewJob": true
}
```

Phase 2 只改变 `page`，不生成公司和岗位组合。接口内部并发为 1，必须按 1、2、3……串行翻页，因为下一页是否继续取决于本分区此前出现的结果。

身份解析保持 Phase 1 规则：

```text
contentType=74  → 使用 momentData.uuid 生成 feed identity
contentType=250 → 使用外层 contentId 生成 discussion identity
```

不支持的记录类型忽略，不进入数据库。API 是否继续翻页只取决于原始 `records` 数量和配置页数，不取决于成功解析出的 feed/discussion 数量。

### 5.2 配置窗口

Experience API 默认扫描第 1–20 页：

```text
page=1, 2, 3, ... EXPERIENCE_API_MAX_PAGES
```

Phase 2 不计算页面 fingerprint，也不尝试判断服务端是否开始重复尾页。当前实测中前 20 页可以得到不同结果，而更深分页可能重复；本阶段主动接受固定窗口可能减少返回规模，以换取更简单、清楚的 discovery 行为。

`EXPERIENCE_API_MAX_PAGES` 是业务选择的覆盖窗口，不是“已经穷尽上游”的证据。完整请求完配置页数表示本次 API producer 成功完成，但 run report 必须明确记录上游是否已自然结束。

接口返回的 `totalPage`、`total` 和 `current` 只写入日志或 run report，不控制翻页。数据库中页面是否已经存在也不参与停止判断。

### 5.3 停止规则

以下情况都表示 API producer 成功完成配置范围：

1. `records` 为空，停止原因 `empty_page`；
2. 当前页非空但小于接口声明的正常 page size，先输出当前页，再以 `short_page` 结束；
3. 完整处理 `EXPERIENCE_API_MAX_PAGES`，以 `configured_page_limit` 结束。

空页或短页意味着 `upstream_exhausted=true`。到达配置页数时仍然 `complete=true`，但必须记录 `upstream_exhausted=false`，不能宣称已经扫完 API 的全部可返回内容。

Phase 1 的连续三页 stale early-stop 从正式 full scan 路径删除。API 只有请求最终失败、响应无效或被阻断时才标记为 incomplete。

### 5.4 请求节奏与错误

两次 API 请求之间默认等待 2 秒，再增加 0–1 秒 jitter。并发 discovery 不能改变 API 自身并发为 1 的约束。

每个页面请求最多尝试 3 次：

```text
网络错误、timeout、408、429、5xx
→ 约 2 秒、5 秒加 jitter 后重试

429
→ 优先遵守合法 Retry-After

404/410、其他 4xx、风控/访问限制、重试耗尽
→ API 来源 incomplete
```

Discovery 的请求尝试不写入 `fetch_attempts`。该表仍只记录 Worker 对详情页的真实抓取尝试。Discovery 错误写标准日志，并汇总到当前 `crawl_runs.sources_json`。

## 6. Sitemap discovery

每次从 `SITEMAP_ROOT_URLS` 配置的根入口开始，根据当次文档内容递归处理子 sitemap，不写死 `sitemap1.xml`、`sitemap2.xml` 或子文档数量。

保持以下规则：

- 只访问允许的牛客域名和合法 sitemap 路径；
- 文档 URL 去重，防止递归环；
- 只接受 feed/discussion 规范路径；
- 页面 identity 在本轮 sitemap producer 内去重；
- 保留可解析的 `lastmod`；
- 根文档即使使用非标准 `urlset` 指向子 sitemap，也按 URL 形态继续递归。

Sitemap 文档继续串行请求。当前子文档很少，为此增加并发不会显著改善总运行时间，反而会增加错误汇总和取消逻辑。

队列中的 sitemap URL 数量达到 `SITEMAP_MAX_URLS`，或文档数量达到 `SITEMAP_MAX_DOCUMENTS` 时，如果仍有未处理内容，该来源必须标记为 incomplete。安全上限不是成功条件。

HTTP 请求沿用第 5.4 节的三次短暂错误重试原则。任一必须读取的 sitemap 文档最终失败，会让 sitemap 来源 incomplete；已经解析并送入 writer 的页面仍然保留。

## 7. 并发、队列和背压

`DiscoveryService` 同时启动两个 producer：

```text
experience-api producer
sitemap producer
```

两个 producer 可以并行等待网络，但各自内部保持串行。它们逐个向同一个 `asyncio.Queue[DiscoveredPage]` 写入，默认 `maxsize=1000`。

当 MySQL 写入速度跟不上时，队列填满，producer 会自然阻塞在 `queue.put()`；这就是 Phase 2 的背压。系统不把完整 sitemap 或全部 API 结果堆积到内存后再统一入库。

来源失败时的行为：

- 某个 producer 失败或 incomplete，不取消另一个 producer；
- 另一个 producer 继续完成自己的配置范围；
- writer 将已经进入队列的数据全部写完；
- 此前已经 commit 和发布的页面不撤回；
- 后续成功 batch 仍然 commit；Publisher 健康时仍按 post-commit 规则发布；
- 最终 crawl run 记为 failed，并在 `sources_json` 中记录每个来源的具体结果。

Publisher 失败时不取消 producer，也不停止 writer。Service 记录首次发布错误，并停止本轮后续的实时发布；后面的 discovery 结果继续写入数据库并保持 pending，最终 crawl run 记为 failed。遗留页面由之后的 `publish-pending` 恢复。

Writer 失败时的行为不同：

- writer 是唯一持久化出口，失败属于系统性故障；
- 立即取消仍在运行的 producer；
- 不再继续请求牛客；
- crawl run 记为 failed；
- 此前已经 commit 或发布的 batch 不撤回；
- 已 commit 的 batch 保留，未提交内容由下次 full scan 重新发现。

Service 必须在所有 producer 结束后通知 writer 刷新最后不足一个 batch 的对象，并等待 writer 正常退出。不得因为 producer 提前失败而遗留未消费队列。

## 8. 单 Writer 与批量数据库事务

`DiscoveryWriter` 最多聚合 `DISCOVERY_DB_BATCH_SIZE=200` 条对象。达到 batch size 或收到 discovery 完成信号时，通过：

```python
await asyncio.to_thread(write_batch, batch)
```

执行同步 SQLAlchemy/PyMySQL 事务。`write_batch()` 必须在线程内部创建并关闭自己的 Session，禁止把 Session 跨线程传递。

批量事务不能简单复用 Phase 1 的逐条 `SELECT page → SELECT page_source`，否则 25,000 条 sitemap URL 会产生大量 N+1 SQL。访问数据库前，writer 必须先在内存中完成批内聚合：

```python
pages_by_identity = {}
sources_by_key = {}

for item in batch:
    page_key = (item.identity.page_type, item.identity.external_id)
    pages_by_identity[page_key] = merge_page(pages_by_identity.get(page_key), item)

    lineage_key = (page_key, item.source_type, item.source_key)
    sources_by_key[lineage_key] = merge_source(
        sources_by_key.get(lineage_key), item
    )
```

`merge_page()` 使用 identity 生成的规范 URL，`source_modified_at` 取批内最大值。`merge_source()` 的 `source_modified_at` 取最大值；新 lineage 的 `first_seen_page` 保留批内第一次观察位置，`last_seen_page` 保留批内最后一次观察位置。

聚合后再执行数据库事务：

1. 一次批量查询 `pages_by_identity` 中的唯一 page identity；
2. 每个缺失 page 只插入一次并 flush，取得 page ID；
3. 一次批量查询 `sources_by_key` 涉及的已有 page_sources；
4. 集中插入或更新聚合后的 lineage；
5. 更新页面的 last_seen、source_modified_at 和必要的 pending 状态；
6. 在同一事务内 commit；
7. commit 成功后返回本批新增、更新、URL 计数和 `dispatchable_page_ids`。

`CommittedBatchResult` 只能在事务成功提交后产生。它至少包含本批统计和去重后的 `dispatchable_page_ids`；Service 收到结果后才可以调用 Publisher。Writer 不等待 RabbitMQ，也不根据发布结果修改数据库。

`dispatchable_page_ids` 只包括：

- 本批新建且状态为 pending 的页面；
- 原 success 页面因为明确更新证据而转回 pending 的页面。

以下页面不进入本批 `dispatchable_page_ids`：

- 本来已经是 pending、又被同一或另一来源命中的页面；
- 已经是 failed/retryable 的 backlog；
- failed/permanent、failed/blocked 或未发生明确更新的 success 页面。

failed/retryable 仍由 `full-scan` 启动时的 backlog publish 或独立 `publish-pending` 处理。这样，API 与 sitemap 在正常运行中先后发现同一页面时，只会在首次创建 pending 页面时实时发布一次，第二次发现只补充 lineage。

同一 identity、同一来源在一个 batch 中出现多次，最终只能产生一条 page 和一条 page_source；同一 identity 分别来自 API 和 sitemap 时，最终产生一条 page 和两条 page_sources。唯一约束是最终保护，不能用触发唯一约束并回滚事务代替正常的批内去重。

单 writer 是进程内的写入串行化手段，不引入分布式锁。数据库唯一约束继续作为最终幂等边界：

```text
pages          UNIQUE(page_type, external_id)
pages          UNIQUE(canonical_url)
page_sources   UNIQUE(page_id, source_type, source_key)
```

Batch 默认 200，不追求越大越好。过大的 batch 会增加单次 SQL、事务持有时间和整批回滚成本；该参数允许在真实运行后调整。

## 9. 页面状态与增量语义

页面 upsert 继续使用 Phase 1 规则：

```text
新页面
→ pending，并进入本批 dispatchable_page_ids

已有 pending
→ 保持 pending，更新 last_seen 和 lineage，不再次实时发布

已有 failed/retryable
→ 保持可重新发布，但不进入本批 dispatchable_page_ids

已有 failed/permanent 或 failed/blocked
→ 不自动恢复

已有 success，来源 editTime/lastmod 明确晚于 last_fetched_at
→ pending，并进入本批 dispatchable_page_ids

已有 success，来源没有更新证据或时间未变
→ 保持 success，只更新 last_seen 和 lineage
```

同一页面可能同时由 API 和 sitemap 发现。最终仍只有一条 `pages`，但 `page_sources` 分别保存两个来源。多个 producer 的重复发现不能触发多个页面记录。

这里的“增量”只描述数据库和抓取行为：已经成功且没有明确更新证据的页面不重新抓取。Discovery 本身每次都要把所选 live 来源扫到当次边界，不再因为数据库里连续出现旧页面而提前停止。

## 10. Discovery report 和 crawl run

不增加表或字段。现有 `crawl_runs.sources_json` 从 Phase 1 的简单数组扩展为带版本的 JSON 对象；旧记录仍然合法，不做数据迁移。

建议结构：

```json
{
  "schema_version": 2,
  "mode": "full-scan",
  "requested": ["experience-api", "sitemap"],
  "config": {
    "experience_api_max_pages": 20,
    "sitemap_max_documents": 20,
    "sitemap_max_urls": 50000
  },
  "results": {
    "experience-api": {
      "complete": true,
      "stop_reason": "configured_page_limit",
      "upstream_exhausted": false,
      "documents_or_pages_seen": 20,
      "urls_seen": 400,
      "error": null
    },
    "sitemap": {
      "complete": true,
      "stop_reason": "queue_exhausted",
      "upstream_exhausted": true,
      "documents_or_pages_seen": 3,
      "urls_seen": 25194,
      "error": null
    }
  },
  "dispatch": {
    "enabled": true,
    "backlog_published": 12,
    "batch_published": 388,
    "failed": false,
    "error": null
  }
}
```

示例数字只用于解释字段，不是固定预期。`complete` 表示来源成功完成本次配置范围；`upstream_exhausted` 表示是否观察到了上游自然结束。Experience API 到达 20 页配置窗口时前者为 true、后者为 false，仍然允许自动发布。

`crawl_runs.status` 继续只有：

```text
running / success / failed
```

任一情况使 run 失败：

- 任一所选来源 incomplete；
- writer 失败；
- backlog publish 或任一 post-commit dispatch 失败；
- Scheduler 自身发生未处理错误。

`crawl_runs.status=failed` 表示本轮至少一个阶段没有完成，不表示本轮没有成功落库或发布任何页面。来源失败、Publisher 失败和 Writer 失败都不能回滚此前已经提交的 batch，也不能撤回已经 confirm 的 RabbitMQ 消息。

保留现有统计列。`center_pages_seen` 继续记录 Experience API 请求页数，它和数据库中的 `center` lineage 一样是 Phase 1 的兼容字段。`sitemap_docs_seen`、`urls_seen`、`pages_inserted`、`pages_updated` 和 `messages_published` 继续更新。

## 11. Post-commit dispatch 与积压恢复

每个 batch 的固定顺序是：

```text
batch aggregate
→ MySQL commit
→ 返回 CommittedBatchResult
→ Scheduler / DiscoveryService 发布 dispatchable_page_ids
→ Worker 消费、保存 gzip、更新 MySQL
→ Worker ACK
```

不存在“等所有来源完成后再发布”的全局 barrier。一次来源后续失败，不会撤回另一来源或此前 batch 已经发布的消息。MySQL commit 是发布本批消息的硬前置条件；若进程在 commit 后、publish 前崩溃，页面自然保留为 pending。

`full-scan` 启动时先查询并发布全库已有 backlog：

```text
status=pending
OR status=failed AND last_error_type=retryable
```

这一步只处理命令启动前已经存在的积压，随后启动 discovery。先恢复旧 backlog，可以避免本轮刚创建的 pending 页面同时被 backlog 查询和实时 post-commit dispatch 重复选中。failed/retryable 在发布前按 Phase 1 规则恢复为 pending；permanent/blocked 不发布。

如果 backlog publish 或某个 batch 的实时发布失败，Publisher 停止本轮后续实时发布，但 producer 和 Writer 继续。尚未发布及之后新发现的页面保持 pending，等待未来的 `publish-pending` 或下一次 `full-scan` 恢复。`discover-only` 没有 Publisher，因此 committed result 中的页面有意留作 pending，不算发布失败。

RabbitMQ 继续使用单个 `fetch.ready` durable queue、persistent message 和 publisher confirm。消息仍为：

```json
{"schema_version": 1, "page_id": 123}
```

Publisher 逐条异步发布并等待 confirm。发布中途出现第一个错误时停止当前发布序列，并禁用本轮后续实时发布；run 记 failed，已经 confirm 的消息不撤回。之后执行 `publish-pending` 可能重复发布已经进入 RabbitMQ 但尚未被 Worker 改成 success 的页面，这是 Phase 1 已接受的 at-least-once 行为，由 Worker 幂等吸收。

Phase 2 不记录逐消息 `published_at`，也不增加 outbox、任务表或 exactly-once 机制。

## 12. CLI

Scheduler 改为三个明确子命令：

```powershell
# 先恢复已有积压，再运行 experience-api + sitemap；每批 commit 后实时发布
uv run nowcoder-crawler scheduler full-scan

# 只发现和写库，不连接 RabbitMQ
uv run nowcoder-crawler scheduler discover-only

# 不访问牛客，直接发布全库 pending/retryable
uv run nowcoder-crawler scheduler publish-pending
```

`full-scan` 和 `discover-only` 支持选择来源，默认两个来源都执行：

```powershell
--sources experience-api sitemap
```

正式的 live full scan 使用默认两个来源。调试时可以只运行一个来源，但日志和 `sources_json.requested` 必须清楚记录实际范围。

`full-scan` 和 `discover-only` 都创建 crawl run：

```text
full-scan
→ running
→ 发布命令启动前已有 backlog
→ discovery batch commit 后实时 publish
→ 所选来源、writer 和 Publisher 都完成后 success

discover-only
→ running
→ discovery batch 持续 commit，不连接或调用 Publisher
→ success
```

`publish-pending` 不创建 crawl run，因为 `crawl_runs` 仍表示一次 discovery 运行。它不访问牛客，只发布全库 pending 和 failed/retryable；遇到发布错误即停止，通过下次执行继续恢复。该命令通过标准日志输出候选数、成功发布数和错误；RabbitMQ UI 用于观察最终 queue 状态。

## 13. 配置

新增配置：

```dotenv
EXPERIENCE_API_MAX_PAGES=20
EXPERIENCE_API_INTERVAL_SECONDS=2
EXPERIENCE_API_JITTER_SECONDS=1
DISCOVERY_QUEUE_MAXSIZE=1000
DISCOVERY_DB_BATCH_SIZE=200
```

兼容规则：

- `EXPERIENCE_API_MAX_PAGES` 未设置时，读取旧的 `CENTER_MAX_PAGES`；
- 两者都未设置时默认 20；
- `CENTER_STALE_PAGES` 不再使用，并从 `.env.example` 删除；
- 现有 Worker 配置完全不变；
- 现有 sitemap 上限配置继续使用。

所有容量参数必须为正数。Phase 2 不增加动态并发或自适应吞吐配置。

## 14. 代码目录

```text
src/nowcoder_crawler/
├── scheduler.py                  # SchedulerWorkflow 和三个命令的应用逻辑
├── rabbit.py                     # 现有 Publisher 和消息协议
└── discovery/
    ├── __init__.py               # DiscoveredPage 和公共结果类型
    ├── experience_api.py         # 固定广泛查询、串行扫描配置窗口
    ├── sitemap.py                # sitemap 递归与 URL 解析
    ├── service.py                # producer、queue、取消、结果汇总
    └── writer.py                 # 单 writer、batch、同步 DB 线程隔离
```

不增加 repository/domain/application 等通用分层。数据库批量操作归 `discovery/writer.py` 所有；通用 `Database` 仍只负责 engine、Session factory 和 `create_all()`。

## 15. 测试

### 15.1 自动测试

Phase 2 新增或调整的测试覆盖：

1. Experience API 空页正常结束；
2. 短页会先输出再结束；
3. 第 20 页会被处理，到达配置页数时 `complete=true` 且 `upstream_exhausted=false`；
4. 不支持的记录不输出，但不会改变基于原始 records 数量的翻页行为；
5. `totalPage` 不会错误截断或延长扫描；
6. 两个 producer 能同时处于运行状态；
7. writer 达到 200 条时提交，并在结束时刷新不足一批的数据；
8. 同 identity、同来源的批内重复最终只有一个 page 和一个 page_source；
9. 同 identity 来自 API 和 sitemap 时只有一个 page，并保留两个 lineage；
10. 批量 upsert 不产生逐页面 N+1 查询；
11. MySQL commit 完成后才调用 Publisher；
12. 同一页面被 API 和 sitemap 重复发现时，正常运行只实时发布一次，并保留两条 lineage；
13. 一个 producer 后续失败时另一个继续，此前已经 commit 和发布的结果不撤回；
14. commit 后、publish 前失败会留下 pending；
15. Publisher 失败会停止后续实时发布，但不阻止后续 discovery 入库；
16. writer 失败时取消 producer；
17. `publish-pending` 能恢复 pending 和 failed/retryable 积压，且不发布 permanent/blocked；
18. 未被 Phase 2 明确替代的 Phase 1 identity、Worker retry、gzip、ACK/redelivery 和幂等测试继续通过。

原先“incomplete discovery 不发布任何消息”和“完整 discovery 后才发布”的 barrier 测试删除，不保留废弃行为来制造兼容性通过。

HTTP 测试使用本地 fixture 或 mock transport，不在常规测试中请求真实牛客。MySQL/RabbitMQ 行为继续放在 integration tests；纯停止规则和编排使用单元测试。

每个行为的实现和对应测试进入同一 commit。测试必须表达本 spec 的真实语义，不能通过弱化断言、跳过错误路径或增加另一个 feature 来掩盖实现 bug。

### 15.2 真实验收

2026-09-03 在本地 compose 的 MySQL 8.0 与 RabbitMQ 3 环境执行：

```powershell
uv run nowcoder-crawler scheduler discover-only
```

本次运行约 2 分 47 秒，实际结果为：

```text
crawl_run.status         success
experience API           20 页，400 个 URL
API stop_reason          configured_page_limit
API upstream_exhausted   false
sitemap                  3 个文档，25,535 个 URL
sitemap stop_reason      queue_exhausted
sitemap upstream_exhausted true
总 observations          25,935
pages_inserted           25,591
pages_updated            344
messages_published       0
```

验收结束时数据库共有 25,592 条 page，其中 discussion 9,570 条、feed 16,022 条；运行前已有 1 条测试 page，所以本轮新增数与最终总数相差 1。`page_sources` 中本轮产生 400 次 API lineage 观察和 25,535 次 sitemap lineage 观察，两个来源的重叠由 page identity 正常合并。

`sources_json` 中两个来源均为 `complete=true`。API 明确记录只到配置窗口而非上游穷尽；sitemap 明确记录队列自然结束。dispatch 报告为 `enabled=false`、发布数为 0，RabbitMQ `fetch.ready` 验收结束时为 0 ready、0 unacked、0 consumer。

自动化验收使用隔离的 `nowcoder_test` 数据库，`ruff` 通过，完整测试结果为 41 passed。测试覆盖批内去重、两次集合查询、producer 并发与取消、commit 后发布、跨来源只实时发布一次、Publisher 失败后继续入库、来源晚失败不撤回消息，以及 backlog 恢复。

Phase 2 验收不要求两个低速 Worker 下载全部两万多个页面，也不在真实验收中向正式 RabbitMQ 批量灌入任务。完整发布和 Worker 长时间消费可以在验收后由操作员明确启动，不作为本阶段完成门槛。

## 16. Phase 2 验收标准

- 只通过当前匿名广泛 Experience API 和动态 sitemap 发现 feed/discussion；
- API 不做公司/岗位分区，且串行完成配置的前 20 页窗口；
- sitemap 从根入口动态递归，不写死当前子文档；
- 数据库页面新旧不再造成 discovery early-stop；
- API 与 sitemap 两个 producer 可并发运行；
- 有界队列能够向 producer 施加背压；
- 同步 PyMySQL 写入不会直接阻塞 asyncio 事件循环；
- 单 writer 按 batch 批量查询和写入，不产生逐页面 N+1；
- 每个 batch 只能在 MySQL commit 后发布本批 `dispatchable_page_ids`；
- 新页面和明确更新后转为 pending 的 success 页面会实时发布，已有 pending 的重复发现只补充 lineage；
- 来源失败保留已提交和已发布结果，另一来源继续，最终 run 失败；
- Publisher 失败后 discovery 和数据库写入继续，后续结果保持 pending，最终 run 失败；
- writer 失败会取消 producer；
- `full-scan` 和 `publish-pending` 能恢复 pending、failed/retryable 积压；
- `discover-only`、`full-scan`、`publish-pending` 三个入口语义清楚；
- 继续使用现有四张表、消息协议和 Worker；
- 所有未被 Phase 2 明确替代的 Phase 1 自动测试继续通过；
- 完成一次真实 `discover-only` 验收并记录结果。

## 17. Commit plan

一个 spec 对应一个独立分支。本 spec 和全部 Phase 2 实现都位于：

```text
phase2-live-discovery
```

Commit 以可以观察和回退的行为变化为边界，不为了提交数量拆分文件移动、注释或其他琐碎改动。每笔 feature commit 同时包含与该行为直接相关的测试、配置和文档调整，并且在提交前通过完整测试。

计划如下：

```text
1. feat: scan configured experience API window
   重命名代码概念；实现固定广泛查询、20 页配置窗口、空页/短页停止、
   请求节奏和错误重试；删除 stale-stop 及其旧测试，同时提交新的行为测试和配置。

2. docs: replace discovery barrier with post-commit dispatch
   只修改本 spec，把全局 publication barrier 替换为 batch commit 后实时发布，
   并冻结失败、积压恢复、测试和验收语义。

3. feat: stream discovery through batched database writer
   加入两个 producer、有界队列、背压、单 writer、to_thread 和批量事务；
   Writer 返回 CommittedBatchResult，但不依赖 RabbitMQ；同时提交并发、flush、
   幂等、lineage、dispatchable_page_ids 和 writer 失败测试。

4. feat: publish committed discovery batches
   加入三个 Scheduler 子命令、source report、启动时积压发布和 batch commit 后
   实时发布；实现 Publisher 失败后继续入库及 publish-pending 恢复，并提交工作流测试。

5. docs: record phase 2 acceptance
   只在自动测试和真实 discover-only 验收完成后记录实际结果。
```

更早的 `docs: define phase 2 live discovery` 和 `docs: tighten phase 2 discovery invariants` 已作为设计历史保留，不 amend；以上列表是从第一笔行为 commit 开始的当前执行计划。

如果实现过程中发现上述某个 commit 同时包含两个可以独立观察、独立测试和独立回退的行为，可以在不制造无用中间状态的前提下再拆分。反过来，强相关的实现与测试不得为了增加 commit 数量而分开。

本项目不是生产系统，不增加没有直接服务 Phase 2 验收目标的防御机制。发现真实 bug 时修复根因；不得为了让测试变绿而修改 feature 语义、绕过失败路径或以虚假结果掩盖问题。每个 commit 都应当是可运行、测试通过、可以直接回退的学习节点。

## 18. 实现顺序

严格按照 Commit plan 推进。本次设计修订必须先独立 commit 并完成一致性复核，之后才能继续后续代码实现。每完成一个后续行为 commit，都应报告：

- 该 commit 改变了什么可观察行为；
- 对应测试证明了什么；
- 完整测试是否通过；
- 下一笔 commit 的边界。

Phase 2 完成后再讨论 Worker 吞吐、正文 NLP 分类或历史回填，不提前加入本分支。
