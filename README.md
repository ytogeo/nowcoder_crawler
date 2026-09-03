# nowcoder-crawler

面向牛客公开 `feed` 和 `discussion` 页面的持续增量采集器。项目从面经列表 API 与 sitemap 发现页面，通过 MySQL 持久化采集状态、RabbitMQ 分发任务，并将原始 HTML 原子保存为 gzip 文件，供后续解析、检索或数据分析使用。

项目只采集无需登录即可访问的公开页面，不包含 Cookie 管理、验证码绕过、历史网页回填和正文分类。

## 功能特性

- 同时接入面经列表 API 与 sitemap，并统一规范化 feed UUID 和 discussion ID；
- 使用有界异步队列、单 Writer 和批量事务处理 discovery 数据；
- `full-scan` 在每批数据完成 MySQL commit 后立即发布，数据库写入失败时不会产生悬空消息；
- RabbitMQ 使用 durable queue、persistent message、publisher confirm 和手动 ACK；
- Worker 支持请求节奏控制、临时错误重试和成功消费后的幂等处理；
- 原始 HTML 通过临时文件和 `os.replace()` 写入固定 gzip 路径；
- 通过标准日志、MySQL 状态和 RabbitMQ Management UI 观察运行情况。

## 架构

```mermaid
flowchart LR
    API[Experience API] --> Discovery[Discovery Service]
    Sitemap[Sitemap] --> Discovery
    Discovery --> Queue[Bounded asyncio.Queue]
    Queue --> Writer[Batch Writer]
    Writer -->|transaction| MySQL[(MySQL)]
    MySQL -->|commit succeeds| Result[Committed Batch Result]
    Result --> Orchestrator[Scheduler / Discovery Service]
    Orchestrator -->|dispatchable page IDs| Publisher[RabbitMQ Publisher]
    Publisher --> Rabbit[(fetch.ready)]
    Rabbit --> W1[Fetch Worker 1]
    Rabbit --> W2[Fetch Worker 2]
    W1 --> Raw[(gzip HTML)]
    W2 --> Raw
    W1 --> MySQL
    W2 --> MySQL
    W1 -->|ACK after file + DB commit| Rabbit
    W2 -->|ACK after file + DB commit| Rabbit
```

Scheduler 是一次性任务，不包含常驻定时器。需要持续增量采集时，可由 cron、任务计划程序或其他外部调度系统定期调用。

## 环境要求

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- Docker 与 Docker Compose

## 快速开始

克隆项目并启动依赖：

```powershell
git clone https://github.com/ytogeo/nowcoder_crawler.git
Set-Location nowcoder_crawler
uv sync
docker compose up -d
```

CLI 直接读取进程环境变量，不会自动加载 `.env`。PowerShell 可以这样配置本地开发环境：

```powershell
$env:MYSQL_DSN = 'mysql+pymysql://nowcoder:nowcoder@127.0.0.1:3307/nowcoder'
$env:RABBITMQ_URL = 'amqp://guest:guest@127.0.0.1:5672/'
$env:RAW_DATA_DIR = (Resolve-Path './data/raw').Path
```

建议第一次运行时先只发现和写库，不向 RabbitMQ 发布：

```powershell
uv run nowcoder-crawler scheduler discover-only
```

检查页面数量和来源分布后，在两个终端中启动 Worker：

```powershell
uv run nowcoder-crawler worker --worker-id worker-1
uv run nowcoder-crawler worker --worker-id worker-2
```

然后发布数据库中已有的 `pending` 和 `failed/retryable` 页面：

```powershell
uv run nowcoder-crawler scheduler publish-pending
```

也可以使用 `full-scan` 在一次命令中先恢复已有积压，再执行 discovery，并在每批 MySQL commit 后实时发布新页面：

```powershell
uv run nowcoder-crawler scheduler full-scan
```

默认 Worker 每次请求间隔约 5–7 秒，两个 Worker 不共享全局限流器。请根据目标站点响应和实际网络环境保守调整速率。

## CLI

| 命令 | 作用 |
| --- | --- |
| `scheduler discover-only` | 扫描所选 discovery 来源并写入 MySQL，不连接 RabbitMQ |
| `scheduler publish-pending` | 不访问牛客，只发布全库 pending 和 failed/retryable 积压 |
| `scheduler full-scan` | 先发布已有积压，再扫描来源并按 batch 实时发布 |
| `worker --worker-id NAME` | 启动一个 concurrency=1 的 Fetch Worker |

`full-scan` 和 `discover-only` 默认同时扫描 `experience-api` 与 `sitemap`，可按需调整：

```powershell
uv run nowcoder-crawler scheduler discover-only --sources sitemap
uv run nowcoder-crawler scheduler full-scan --sources experience-api --max-pages 10
```

全局调试日志开关需要写在子命令之前：

```powershell
uv run nowcoder-crawler --verbose scheduler discover-only
```

## 配置

完整配置及默认值见 [`.env.example`](.env.example)。常用变量如下：

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `MYSQL_DSN` | 必填 | SQLAlchemy MySQL DSN |
| `RABBITMQ_URL` | 必填 | RabbitMQ 连接地址 |
| `RAW_DATA_DIR` | `./data/raw` | gzip 原始页面目录 |
| `FETCH_QUEUE` | `fetch.ready` | Worker 消费队列 |
| `WORKER_PREFETCH` | `2` | 每个 Worker 的未 ACK 消息上限 |
| `FETCH_MAX_ATTEMPTS` | `3` | 单页面最大 HTTP 请求次数 |
| `FETCH_BASE_DELAY_SECONDS` | `5` | 单 Worker 基础请求间隔 |
| `FETCH_JITTER_SECONDS` | `2` | 请求间隔随机抖动上限 |
| `EXPERIENCE_API_MAX_PAGES` | `20` | 每轮 API discovery 的配置窗口 |
| `DISCOVERY_QUEUE_MAXSIZE` | `1000` | discovery 内存队列容量 |
| `DISCOVERY_DB_BATCH_SIZE` | `200` | Writer 单批最大观察数 |
| `SITEMAP_MAX_DOCUMENTS` | `20` | sitemap 文档安全上限 |
| `SITEMAP_MAX_URLS` | `50000` | sitemap URL 安全上限 |

Compose 将 MySQL 暴露在宿主机 `3307`，RabbitMQ AMQP 与 Management UI 分别位于 `5672` 和 `15672`。本地默认 Management UI 账号为 `guest/guest`。

## 数据与可靠性语义

MySQL 使用四张表：

- `crawl_runs`：一次 discovery 运行及各来源结果；
- `pages`：页面身份、抓取状态和当前 gzip 元数据；
- `page_sources`：API/sitemap 的发现来源与查询血缘；
- `fetch_attempts`：Worker 的每次真实 HTTP 尝试。

原始页面保存在：

```text
data/raw/feed/<uuid>.html.gz
data/raw/discussion/<id>.html.gz
```

系统提供的是 at-least-once，而不是 exactly-once：RabbitMQ 消息可能重复，两个 Worker 也可能对同一页面产生重复 HTTP 请求。数据库唯一约束、页面状态检查和固定文件路径保证最终结果幂等。

Worker 只在 gzip 写入成功且 MySQL 状态提交后 ACK。进程在 ACK 前退出时，RabbitMQ 会重新投递未确认消息。Scheduler 在 MySQL commit 后、RabbitMQ publish 前退出时，页面会保留为 pending，可由 `publish-pending` 恢复。

队列仍有积压时不要反复执行 `publish-pending` 或 `full-scan`，否则 pending 页面会被再次发布。重复消息不会产生重复页面，但会增加无效消费和潜在的重复请求。

## 运行观察

RabbitMQ Management UI：<http://127.0.0.1:15672>

查看队列状态：

```powershell
docker compose exec -T rabbitmq rabbitmqctl list_queues name messages_ready messages_unacknowledged consumers
```

查看页面和抓取尝试分布：

```powershell
docker compose exec -T mysql mysql -unowcoder -pnowcoder nowcoder -e "SELECT status,last_error_type,COUNT(*) FROM pages GROUP BY status,last_error_type; SELECT worker_id,outcome,COUNT(*) FROM fetch_attempts GROUP BY worker_id,outcome;"
```

查看原始文件数量：

```powershell
Get-ChildItem ./data/raw -Recurse -Filter *.html.gz | Measure-Object
```

## 测试

单元测试不需要外部服务：

```powershell
uv run ruff check .
uv run pytest tests/unit -q
```

集成测试会重建目标数据库中的表。请创建独立测试库，绝不要把 `TEST_MYSQL_DSN` 指向正在使用的采集数据库：

```powershell
docker compose exec -T mysql mysql -uroot -proot -e "CREATE DATABASE IF NOT EXISTS nowcoder_test"
$env:TEST_MYSQL_DSN = 'mysql+pymysql://root:root@127.0.0.1:3307/nowcoder_test'
uv run pytest -q
```

## 项目边界

- discovery 覆盖当前 API 配置窗口与当次 sitemap 暴露的页面，不代表牛客历史全量；
- 不使用 Common Crawl 或 Wayback Machine 回填历史页面；
- 不解析正文，也不判断页面是否属于面经或包含手撕题；
- 不实现登录态维护、验证码识别或访问限制绕过；
- 不包含跨 Worker 的严格全局限流和 exactly-once 投递。

实现与可靠性约束见 [Phase 1 Spec](docs/phase1-spec.md) 和 [Phase 2 Spec](docs/phase2-spec.md)，实际验收记录见 [Phase 1 验收记录](docs/phase1-acceptance.md) 及 Phase 2 Spec 的验收章节。

## 使用说明

使用者应自行确认并遵守目标站点的服务条款、robots 规则及适用法律。请维持保守请求频率，并在出现连续 429、403、验证码或其他访问限制时及时停止采集和人工检查。
