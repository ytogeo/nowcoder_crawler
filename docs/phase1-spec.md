# 牛客公开面经持续增量采集系统 Phase 1 Spec

状态：设计冻结，Phase 1 已实现并通过验收
项目目录：`D:\Desktop\nowcoder_crawler`
范围：Phase 1，只处理 live 公开页面

## 1. 目标

本项目首先是一个采集架构练习，不追求尽快爬完牛客，也不宣称历史全量。Phase 1 要亲手实践：

- 一次性 Scheduler 与 Fetch Worker 的职责拆分；
- RabbitMQ producer、consumer、prefetch 和手动 ACK；
- Worker 异常退出后的 unacked message redelivery；
- 两个 Worker 共同消费一个队列；
- Worker 内短暂错误重试；
- MySQL 状态持久化和 discovery lineage；
- 重复发现、重复发布和重复消费下的结果幂等；
- 增量发现；
- 原始 HTML 与未来正文解析解耦。

业务输出是牛客公开面经 feed 和 discussion 页面的当前原始 HTML gzip。Phase 1 不解析正文，不抽取手撕题。

## 2. 非目标

Phase 1 不实现：

- Common Crawl、Wayback 或历史回填；
- 历史全量承诺；
- 常驻 Scheduler；
- 独立 `fetch_tasks` 表；
- transactional outbox、running lease 或分布式锁；
- retry queue、TTL、DLX、DLQ；
- snapshot 历史、内容寻址和 GC；
- 严格的跨进程全局限流；
- 正文解析和手撕题提取；
- Redis、Kafka、FastAPI、Prometheus、Grafana、Kubernetes；
- Alembic、管理后台或自研队列 UI；
- 登录 Cookie、验证码处理或受限内容绕过。

## 3. 架构

```text
                         一次性 Scheduler CLI
                    ┌──────────────┴──────────────┐
                    │                             │
              面经中心 discoverer          sitemap discoverer
                    │                             │
                    └──────── URL 规范化 ─────────┘
                                  │
                    upsert pages / page_sources
                                  │
                   publish {schema_version,page_id}
                                  │
                         RabbitMQ fetch.ready
                         durable + persistent
                          ┌───────┴───────┐
                          │               │
                       Worker 1        Worker 2
                      prefetch=2       prefetch=2
                     concurrency=1    concurrency=1
                          │               │
                          └──── HTTP GET ─┘
                                  │
                     gzip 原子落盘 + MySQL commit
                                  │
                              manual ACK
```

职责边界：

- Scheduler 只负责发现、规范化、数据库 upsert 和发布，不下载帖子正文；
- Worker 只消费页面任务、抓取公开 HTML、保存 gzip、更新数据库和 ACK；
- RabbitMQ 负责消息传递和保存 unacked 状态，不是业务状态数据库；
- MySQL 保存页面身份、简单状态、发现血缘和抓取尝试；
- gzip 是未来 parser 的输入，parser 是否成功不影响抓取状态。

## 4. 页面身份

系统只接受两类规范页面。

```text
feed:
  page_type   = feed
  external_id = 32 位十六进制 UUID
  canonical   = https://www.nowcoder.com/feed/main/detail/{uuid}

discussion:
  page_type   = discussion
  external_id = 数字 contentId
  canonical   = https://www.nowcoder.com/discuss/{contentId}
```

规范化时删除 query 和 fragment，包括 `urlSource`、`sourceSSR` 等参数。不识别的路径不进入队列。

数据库唯一约束：

```text
UNIQUE(page_type, external_id)
UNIQUE(canonical_url)
```

## 5. Scheduler

### 5.1 CLI 和 crawl run

```powershell
uv run nowcoder-crawler scheduler `
  --sources center sitemap `
  --max-pages 10
```

Scheduler 执行一次后退出。持续增量通过人工、PowerShell 或系统任务计划程序重复执行命令实现，应用内部不实现定时守护进程。

每次执行创建一条 `crawl_runs`。它只表示一次 discovery 运行，不表示 Worker 消费生命周期。完成发现和当轮发布后即可记为 success。

### 5.2 面经中心 discoverer

默认全站查询：

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

每轮从最新第 1 页开始。满足任一条件停止：

- 达到 `--max-pages`，默认 10；
- 连续 3 页既无新页面，也无更晚 `editTime`；
- 返回空页；
- 到达接口声明的最后一页。

不能从上次页码继续，因为最新排序会移动，容易漏掉插入到前面的内容。

身份解析：

- `contentType=74`：使用 `momentData.uuid` 生成 feed；
- `contentType=250`：使用外层 `contentId` 生成 discussion。

lineage 保存当前查询的 `companyList`、`jobId`、`level` 和页面位置。Phase 1 默认查询全站，但字段保留给后续公司、岗位分片使用。

### 5.3 Sitemap discoverer

从配置的牛客 sitemap 入口递归读取 XML 或文本 sitemap：

- 只递归 `nowcoder.com` 下的 sitemap；
- sitemap 文档数不超过 `SITEMAP_MAX_DOCUMENTS`；
- 本轮 URL 数不超过 `SITEMAP_MAX_URLS`；
- 已访问 sitemap URL 去重，防止环；
- 只接受 feed/discussion 规范路径；
- 保留可解析的 `lastmod`。

牛客可能用普通 `urlset` 指向其他 sitemap 文件，因此不能只根据 `sitemapindex` 标签判断是否递归。

### 5.4 增量发布

```text
新页面
→ status=pending，发布

已有 pending
→ 允许重复发布

已有 failed/retryable
→ 允许重复发布

已有 failed/permanent 或 failed/blocked
→ 不自动发布

已有 success，来源 editTime/lastmod 明确晚于 last_fetched_at
→ status=pending，发布

已有 success，来源无更新时间或时间未变化
→ 只更新 last_seen 和 lineage，不发布
```

Scheduler 允许重复发布，这是不做 outbox 和 lease 的明确取舍。

### 5.5 RabbitMQ 发布

只有一个业务队列：

```text
fetch.ready
```

```text
durable queue = true
delivery_mode = persistent
publisher confirm = 开启，但不增加独立 publisher 架构
```

消息体：

```json
{"schema_version": 1, "page_id": 123}
```

消息不携带 URL 或状态，Worker 从 MySQL 读取权威页面数据。写库后、发布前崩溃的 pending 页面由下次 Scheduler 补发；发布后的崩溃可能产生重复消息，由消费端结果幂等吸收。

## 6. Worker

### 6.1 启动和消费

```powershell
uv run nowcoder-crawler worker --worker-id worker-1
uv run nowcoder-crawler worker --worker-id worker-2
```

两个 Worker 共同消费 `fetch.ready`：

```text
prefetch_count = 2
HTTP concurrency = 1
manual ACK
```

每个 Worker 必须串行处理 HTTP；prefetch 只用于观察 RabbitMQ 预取和 unacked 行为。杀死 Worker 后，它持有的正在处理及已预取消息都由 RabbitMQ redeliver。

收到消息后：

```text
page 不存在
→ 记录错误并 ACK，避免毒消息永久占队列

status=success
→ 视为重复消息，直接 ACK

status=pending/failed
→ 抓取
```

Phase 1 没有 `running` 状态，也没有消费前 claim。两份相同消息可能被两个 Worker 同时请求。

### 6.2 请求节奏

每个 Worker 独立限速：

```text
base delay = 5 秒
jitter = 0–2 秒
HTTP concurrency = 1
```

这不是严格全局限流。两个 Worker 可能短时碰撞，但 Phase 1 接受这一边界。

默认 HTTP 行为：

```text
匿名，不携带登录 Cookie
普通浏览器 User-Agent
follow redirects = true
max redirects = 5
connect timeout = 10 秒
read/total timeout = 30 秒
```

### 6.3 成功校验

Phase 1 不解析标题或正文 selector。成功只要求：

- 状态为 200；
- `Content-Type` 合理表示 HTML；
- 最终域名仍是牛客；
- 最终路径仍对应预期 feed UUID 或 discussion ID；
- 响应中能够确认目标页面身份；
- 不是登录、验证码、风控或访问限制模板；
- body 不是明显异常的小响应。

确认获得目标公开帖子 HTML 即为抓取成功。未来 parser 失败不能触发重新抓取。

### 6.4 错误分类和进程内重试

每条消息最多 3 次 HTTP 请求：首次加两次重试。

```text
retryable:
  网络错误、DNS、timeout、408、429、500–599

permanent:
  400–499，排除 408、429、401、403
  典型为 404、410
  页面身份不匹配或不支持的页面类型

blocked:
  401、403、验证码、风控、登录或访问限制模板
```

普通 retryable 等待：

```text
第 1 次失败后：约 2 秒 + jitter
第 2 次失败后：约 5 秒 + jitter
第 3 次失败：停止本消息重试
```

429 优先遵守 `Retry-After`；缺失时使用保守默认等待，并设置合理等待上限，避免单条消息无限占用 Worker。每个 Worker维护自己的连续 429 计数，成功的非 429 响应会清零。

当前 Worker 连续遇到 3 次 429 后：

```text
当前页面写 failed/blocked
MySQL commit
ACK 当前消息
Worker 非零退出
```

另一 Worker 不自动停止，由操作员根据日志和 RabbitMQ UI处理。

### 6.5 失败与 ACK

普通 retryable 耗尽 3 次：

```text
pages.status=failed
last_error_type=retryable
写 fetch_attempts
commit
ACK
```

permanent/blocked 不做进程内重试：

```text
pages.status=failed
last_error_type=permanent/blocked
写 fetch_attempts
commit
ACK
```

所有最终失败都 ACK，避免单队列热循环。只有 retryable failed 会被下一次 Scheduler 自动发布。Worker 在处理或等待 retry 时崩溃，消息保持 unacked 并被 redeliver；新进程的 attempt 从 1 重新开始。

## 7. 原始文件

固定规范路径：

```text
data/raw/feed/{uuid}.html.gz
data/raw/discussion/{contentId}.html.gz
```

成功流程：

```text
1. 获得原始 body bytes
2. 计算未压缩 body SHA-256
3. 在目标目录创建带随机后缀的临时文件
4. 写 gzip 并 close
5. os.replace(temp, final)
6. 更新 pages 的路径、hash 和 HTTP 元数据
7. MySQL commit
8. ACK
```

Phase 1 不要求 `fsync`。两个 Worker 必须使用不同临时文件名，不能共享 `{id}.tmp`。

崩溃边界：

```text
文件完成前崩溃
→ 无 ACK，redelivery

os.replace 后、DB commit 前崩溃
→ redelivery 后重写同一路径

DB success 后、ACK 前崩溃
→ redelivery；新 Worker 看到 success 后直接 ACK
```

这构成 Phase 1 的 at-least-once delivery + idempotent result。系统允许重复 HTTP，不承诺 exactly-once execution。

## 8. MySQL 四张表

使用 SQLAlchemy `create_all()` 初始化固定 schema，不使用 Alembic。所有时间使用 UTC `DATETIME(6)`；状态使用 `VARCHAR`，不使用 MySQL ENUM。

### 8.1 crawl_runs

一次 Scheduler discovery 运行。

```text
id                  BIGINT PK AUTO_INCREMENT
status              VARCHAR(16)   running/success/failed
sources_json        JSON
started_at          DATETIME(6)
finished_at         DATETIME(6) NULL
center_pages_seen   INT DEFAULT 0
sitemap_docs_seen   INT DEFAULT 0
urls_seen           INT DEFAULT 0
pages_inserted      INT DEFAULT 0
pages_updated       INT DEFAULT 0
messages_published  INT DEFAULT 0
error_message       TEXT NULL
```

### 8.2 pages

页面身份、简单状态和当前 gzip 元数据。

```text
id                    BIGINT PK AUTO_INCREMENT
page_type             VARCHAR(16) NOT NULL
external_id           VARCHAR(64) NOT NULL
canonical_url         VARCHAR(512) NOT NULL
status                VARCHAR(16) NOT NULL DEFAULT 'pending'

first_seen_at         DATETIME(6) NOT NULL
last_seen_at          DATETIME(6) NOT NULL
source_modified_at    DATETIME(6) NULL
last_fetched_at       DATETIME(6) NULL

gzip_path             VARCHAR(1024) NULL
body_sha256           CHAR(64) NULL
http_status           SMALLINT NULL
response_content_type VARCHAR(255) NULL
response_bytes        BIGINT NULL
final_url             VARCHAR(512) NULL

last_error_type       VARCHAR(16) NULL
last_error_message    TEXT NULL
created_at            DATETIME(6) NOT NULL
updated_at            DATETIME(6) NOT NULL

UNIQUE(page_type, external_id)
UNIQUE(canonical_url)
```

允许的状态：`pending / success / failed`。允许的错误类型：`retryable / permanent / blocked`。成功后清空错误字段。

### 8.3 page_sources

记录 discovery lineage。

```text
id                   BIGINT PK AUTO_INCREMENT
page_id              BIGINT NOT NULL FK pages(id)
source_type          VARCHAR(32) NOT NULL
source_key           VARCHAR(255) NOT NULL
company_ids_json     JSON NULL
job_id               INT NULL
job_level            SMALLINT NULL
first_seen_page      INT NULL
last_seen_page       INT NULL
source_modified_at   DATETIME(6) NULL
first_seen_at        DATETIME(6) NOT NULL
last_seen_at         DATETIME(6) NOT NULL
first_crawl_run_id   BIGINT NOT NULL FK crawl_runs(id)
last_crawl_run_id    BIGINT NOT NULL FK crawl_runs(id)

UNIQUE(page_id, source_type, source_key)
```

`source_key`：

```text
center:{规范化company_ids}:{job_id}:{level}
sitemap:{入口或直接父级sitemap URL}
```

分页位置不进入唯一键；同一查询中位置变化只更新 `last_seen_page`。

### 8.4 fetch_attempts

每一次真实 HTTP 请求一行，包括同一消息内的重试。

```text
id                BIGINT PK AUTO_INCREMENT
page_id           BIGINT NOT NULL FK pages(id)
worker_id         VARCHAR(64) NOT NULL
attempt_no        SMALLINT NOT NULL
started_at        DATETIME(6) NOT NULL
finished_at       DATETIME(6) NOT NULL
outcome           VARCHAR(16) NOT NULL
http_status       SMALLINT NULL
final_url         VARCHAR(512) NULL
response_bytes    BIGINT NULL
elapsed_ms        INT NULL
retry_after       VARCHAR(128) NULL
error_type        VARCHAR(16) NULL
error_message     TEXT NULL
```

`attempt_no` 是本次消息处理进程内的 1–3，Worker crash 后 redelivery 可以重新从 1 开始。

## 9. ACK、Retry 与幂等分别解决什么

### ACK

ACK 表示 Worker 已经把消息转换成可靠结果，不再需要 broker 保留它。

成功必须在 gzip 已 `os.replace` 且 `pages=success` 已 commit 后 ACK。失败必须在错误状态和尝试记录 commit 后 ACK。禁止收到消息立即 ACK，也不能仅凭 HTTP 200 ACK。

### Retry

进程内 retry 只处理秒级短暂故障：

```text
短暂恢复 → 同一 Worker 内最多 3 次
仍失败   → retryable failed + ACK
跨运行恢复 → 下一次 Scheduler 再发布 retryable failed
```

permanent/blocked 不跨运行自动重试。

### 幂等

Phase 1 不是“任务绝不执行两次”，而是“重复执行不产生重复最终页面”：

- `pages` 唯一约束吸收重复发现；
- `page_sources` 唯一约束吸收重复 lineage；
- success 页面收到重复消息直接 ACK；
- 同一页面始终写同一个规范 gzip；
- 数据库只保留当前路径和 hash。

## 10. 配置和依赖

使用 dataclass 从环境变量读取配置；必要配置缺失时启动失败。Phase 1 不接受 Cookie 配置。

`.env.example`：

```dotenv
MYSQL_DSN=mysql+pymysql://nowcoder:nowcoder@127.0.0.1:3306/nowcoder
RABBITMQ_URL=amqp://guest:guest@127.0.0.1:5672/
RAW_DATA_DIR=./data/raw

FETCH_QUEUE=fetch.ready
WORKER_PREFETCH=2
FETCH_CONNECT_TIMEOUT_SECONDS=10
FETCH_TIMEOUT_SECONDS=30
FETCH_MAX_ATTEMPTS=3
FETCH_BASE_DELAY_SECONDS=5
FETCH_JITTER_SECONDS=2
FETCH_RETRY_AFTER_DEFAULT_SECONDS=60
FETCH_RETRY_AFTER_MAX_SECONDS=300

CENTER_MAX_PAGES=10
CENTER_STALE_PAGES=3
SITEMAP_MAX_DOCUMENTS=20
SITEMAP_MAX_URLS=50000
```

核心依赖：

```text
aio-pika
httpx
SQLAlchemy
PyMySQL
```

开发依赖：

```text
pytest
pytest-asyncio
ruff
```

使用 Python 标准 `logging`，不建立独立 logging 模块。

## 11. 目录结构

```text
nowcoder_crawler/
├── pyproject.toml
├── compose.yaml
├── .env.example
├── .gitignore
├── README.md
├── docs/
│   └── phase1-spec.md
├── src/nowcoder_crawler/
│   ├── __init__.py
│   ├── cli.py
│   ├── config.py
│   ├── models.py
│   ├── database.py
│   ├── rabbit.py
│   ├── identity.py
│   ├── scheduler.py
│   ├── worker.py
│   ├── fetcher.py
│   ├── storage.py
│   └── discovery/
│       ├── __init__.py
│       ├── center.py
│       └── sitemap.py
├── tests/
│   ├── unit/
│   └── integration/
└── data/raw/
```

不增加 repository/service/domain 等分层。

## 12. Compose 和运行

`compose.yaml` 只包含 MySQL 8 和 RabbitMQ management：

- 两者使用 named volume；
- RabbitMQ Management UI 暴露本地端口；
- 两个服务有 healthcheck；
- Scheduler/Worker 在宿主机通过 uv 运行。

Scheduler 和 Worker 启动时调用 SQLAlchemy `create_all()`，空数据库可自动初始化。

基本运行方式：

```powershell
docker compose up -d
uv sync

uv run nowcoder-crawler scheduler --sources center sitemap --max-pages 10

# 终端 1
uv run nowcoder-crawler worker --worker-id worker-1

# 终端 2
uv run nowcoder-crawler worker --worker-id worker-2
```

## 13. 可观察性

使用标准 logging 输出以下上下文：

```text
component, event, crawl_run_id, page_id, page_type, external_id,
worker_id, attempt_no, http_status, error_type, elapsed_ms
```

观察入口：

- Scheduler/Worker 终端日志；
- MySQL 四张表；
- RabbitMQ Management UI 的 ready/unacked、consumer 和 redelivery；
- `data/raw` gzip 文件。

## 14. 测试

### 14.1 单元测试

1. feed/discussion identity 和 canonical URL；
2. center 连续 3 个旧页停止；
3. HTTP 错误分类和进程内 retry；
4. gzip 临时文件与 `os.replace`。

单元测试不请求真实牛客。

### 14.2 集成和验收

1. duplicate discovery 最终只有一个 page；
2. Worker 在 ACK 前被 kill 后消息 redelivery；
3. 成功必须完成 gzip + MySQL success 后才 ACK；
4. retryable 错误会进程内重试；
5. retryable failed 会被下一次 Scheduler 再发布；
6. permanent/blocked 不会被 Scheduler 自动发布。

本地 fixture HTTP server 用于自动测试。RabbitMQ 重启后的 persistent message 恢复作为手工实验，不要求自动化测试。

## 15. 手工可靠性实验

### ACK 前 kill

1. 启动基础设施和两个 Worker；
2. 确保某 Worker 持有 unacked message；
3. 强制结束该 Worker；
4. 在 RabbitMQ UI观察另一个 Worker 收到 redelivered message；
5. 验证最终只有一个 page 和一个规范 gzip。

### DB success 后、ACK 前 kill

开发测试可使用仅由测试环境变量启用的 failpoint，或使用调试器：

1. 完成 gzip 和 MySQL success；
2. ACK 前结束 Worker；
3. 消息 redelivery；
4. 新 Worker 看到 success 后直接 ACK；
5. 不产生第二个页面记录。

failpoint 只服务验收，不成为运行机制。

### RabbitMQ 重启

1. 发布 persistent 消息但不启动 Worker；
2. 重启 RabbitMQ 容器；
3. 确认 durable queue 和消息仍在；
4. 启动 Worker 完成消费。

## 16. Phase 1 验收标准

- 面经中心和 sitemap 可重复增量发现 feed/discussion；
- 两个 Worker 共同消费一个 RabbitMQ queue；
- 每个 Worker `prefetch=2`、HTTP concurrency=1；
- gzip 落盘和 MySQL success commit 后才 ACK；
- Worker crash 后 unacked message redelivery；
- 重复发现和消息不产生重复页面记录或多个规范文件；
- retryable 错误在 Worker 内最多 3 次；
- retryable failed 可由下一次 Scheduler 再发布；
- permanent/blocked 不自动再发布；
- 重启组件后，可通过重跑 Scheduler 和 Worker继续工作；
- 完成一次 20–30 分钟真实低速 smoke test；
- 不依赖正文解析完成以上验收。

## 17. 编码里程碑

### M1：骨架与数据层

- pyproject、Compose、配置；
- 四张 SQLAlchemy 表和 `create_all()`；
- identity/canonical URL；
- 基础单元测试。

### M2：发现和 Scheduler

- 面经中心、sitemap discoverer；
- 增量 upsert 和 lineage；
- durable queue、persistent publish；
- Scheduler CLI。

### M3：Worker 和 raw 存储

- 双 Worker、prefetch 2、concurrency 1；
- HTTP 校验和错误分类；
- 进程内 retry；
- gzip + `os.replace`；
- commit 后 manual ACK。

### M4：可靠性验收

- 精简测试；
- kill/redelivery 实验；
- RabbitMQ 持久消息实验；
- 20–30 分钟真实低速 smoke test；
- README 和验收记录。

## 18. Phase 1 后再评估

只有真实运行证明需要时，才讨论 retry queue/DLQ、task 表和 lease、outbox、严格全局限流、snapshot、对象存储、正文解析、历史补全和长期监控。这些不属于本 spec。
