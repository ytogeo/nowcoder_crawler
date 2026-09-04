# nowcoder-crawler

面向牛客网公开页面的增量式网页采集工具。

系统从 **面经 API** 与 **Sitemap** 发现页面，通过 **MySQL** 记录抓取状态与来源血缘，使用 **RabbitMQ** 进行任务异步分发，并由多个 **Worker** 将页面 HTML 原生原子压缩保存为 `.html.gz` 文件，与后续的正文抽取和数据分析流程彻底解耦。

---

## 核心特性

- **多源增量发现**：支持 Sitemap 递归遍历与面经 API 翻页发现，自动记录发现血缘（`page_sources`）。
- **队列解耦与分发**：基于 RabbitMQ 实现任务分发与负载均衡，支持多个 Worker 并行消费。
- **原子压缩落盘**：原始 HTML 直接存为 `.html.gz`，采用临时文件 + 原子替换，防止产生残缺文件并大幅节约存储。
- **平滑限速与风控处理**：各 Worker 独立配置基础延迟与随机抖动（Jitter）；403 或验证码页面标记为 blocked，单个 Worker 连续 3 次收到 429 时非零退出。
- **可靠性与幂等设计**：
  - **手动 ACK**：数据成功落盘且 MySQL 状态更新后才确认消息，异常退出时未 ACK 任务自动重投递。
  - **断点自愈**：调度器在数据库事务提交后才向队列发消息，中断时可通过 `publish-pending` 随时恢复。
  - **全局幂等**：依靠数据库唯一约束与前置状态检查，无惧重复消息与重试。

---

## 环境要求

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)（推荐包管理器）
- Docker & Docker Compose

---

## 快速开始

### 1. 安装依赖

克隆仓库并使用 `uv` 安装项目依赖：

```bash
git clone https://github.com/ytogeo/nowcoder_crawler.git
cd nowcoder_crawler
uv sync
```

### 2. 启动基础服务

使用项目自带的 `compose.yaml` 启动本地 MySQL 8.0 和 RabbitMQ：

```bash
docker compose up -d
```

> **服务默认地址**：
> - MySQL：`127.0.0.1:3307`（账号/密码：`nowcoder`/`nowcoder`）
> - RabbitMQ AMQP：`127.0.0.1:5672`
> - RabbitMQ Web 控制台：`http://127.0.0.1:15672`（默认账号：`guest`/`guest`）

### 3. 配置环境变量

程序直接读取系统环境变量。可通过以下方式配置本地环境：

**PowerShell (Windows)**:
```powershell
$env:MYSQL_DSN = 'mysql+pymysql://nowcoder:nowcoder@127.0.0.1:3307/nowcoder'
$env:RABBITMQ_URL = 'amqp://guest:guest@127.0.0.1:5672/'
$env:RAW_DATA_DIR = (Resolve-Path './data/raw').Path
```

**Bash / Zsh (Linux / macOS)**:
```bash
export MYSQL_DSN="mysql+pymysql://nowcoder:nowcoder@127.0.0.1:3307/nowcoder"
export RABBITMQ_URL="amqp://guest:guest@127.0.0.1:5672/"
export RAW_DATA_DIR="$(pwd)/data/raw"
```

完整配置项可参考 [`.env.example`](.env.example)。

### 4. 运行流程

#### 方式 A：分步执行（推荐首次使用）

1. **执行页面发现并入库**（仅扫描并写库，不发消息到队列）：
   ```bash
   uv run nowcoder-crawler scheduler discover-only
   ```
2. **启动抓取 Worker**（可分别在两个终端中运行）：
   ```bash
   uv run nowcoder-crawler worker --worker-id worker-1
   uv run nowcoder-crawler worker --worker-id worker-2
   ```
3. **发布待抓取任务**（将库中未抓取的页面推入队列）：
   ```bash
   uv run nowcoder-crawler scheduler publish-pending
   ```

#### 方式 B：一键全流程扫描（Full-Scan）

先发布已有积压，再执行 discovery，并在每批数据提交后实时向 Worker 分发任务：

```bash
uv run nowcoder-crawler scheduler full-scan
```

---

## CLI 命令速查

CLI 入口统一为 `nowcoder-crawler`：

| 子命令 | 作用说明 | 常见场景 |
| :--- | :--- | :--- |
| `scheduler discover-only` | 扫描指定来源并将 URL 写入 MySQL，不连接 RabbitMQ | 首次排查数据源、单独更新待抓取库 |
| `scheduler publish-pending` | 不请求牛客，仅把数据库中现存的 pending/retryable 页面发布到队列 | 恢复断点积压、配合 discover-only 使用 |
| `scheduler full-scan` | 先发布旧积压，再扫描来源并在事务 commit 后实时推送新任务 | 日常自动化或全流程采集 |
| `worker --worker-id <ID>` | 启动一个 Fetch Worker 节点（单进程内 concurrency=1） | 消费队列并下载页面 |

### 常用参数示例

```bash
# 仅扫描 sitemap 来源
uv run nowcoder-crawler scheduler discover-only --sources sitemap

# 仅扫描面经 API，且限制最大翻页数为 10 页
uv run nowcoder-crawler scheduler full-scan --sources experience-api --max-pages 10

# 开启 DEBUG 级别日志输出（--verbose 需放在子命令前）
uv run nowcoder-crawler --verbose scheduler discover-only
```

---

## 常用配置项

常用环境变量如下，更多高级参数请查看 [`.env.example`](.env.example)：

| 环境变量 | 默认值 | 说明 |
| :--- | :--- | :--- |
| `MYSQL_DSN` | *(必填)* | MySQL 数据库连接串 |
| `RABBITMQ_URL` | *(必填)* | RabbitMQ 连接地址 |
| `RAW_DATA_DIR` | `./data/raw` | 原始 gzip 文件的保存根目录 |
| `FETCH_QUEUE` | `fetch.ready` | Worker 消费的任务队列名称 |
| `WORKER_PREFETCH` | `2` | 单个 Worker 的未 ACK 消息拉取上限 |
| `FETCH_MAX_ATTEMPTS` | `3` | 单页面最大 HTTP 请求次数（包含首次请求） |
| `FETCH_BASE_DELAY_SECONDS` | `5` | 单 Worker 每次请求的基础等待时间（秒） |
| `FETCH_JITTER_SECONDS` | `2` | 请求等待的随机抖动上限（秒），实际间隔为 base + [0, jitter] |
| `EXPERIENCE_API_MAX_PAGES` | `20` | 每轮面经 API 发现的最大翻页深度 |
| `DISCOVERY_DB_BATCH_SIZE` | `200` | Discovery 批量写库的批次大小 |

---

## 数据存储与可靠性

### 1. 原始文件存储结构

原始 HTML 采用 gzip 格式归档，存储结构如下：

```text
data/raw/
├── feed/
│   └── <uuid>.html.gz          # 动态详情页（如 /feed/main/detail/<uuid>）
└── discussion/
    └── <contentId>.html.gz     # 帖子/讨论详情页（如 /discuss/<contentId>）
```

### 2. 数据库设计（MySQL）

系统包含 4 张核心表：
- `crawl_runs`：记录单次 Scheduler 的发现运行状态与统计；
- `pages`：页面的规范化身份、当前抓取状态及 gzip 路径、SHA-256 校验和；
- `page_sources`：记录页面在 API 或 Sitemap 中的来源详情与发现血缘；
- `fetch_attempts`：记录 Worker 的每次真实 HTTP 请求尝试与审计信息。

### 3. 可靠性机制

- **At-Least-Once 与幂等**：Worker 仅在 gzip 写入成功且 MySQL 状态更新提交后才会发送 ACK。即使节点异常退出，RabbitMQ 也会重新投递任务；消费端通过数据库唯一索引与状态检查保证幂等。
- **断点自愈**：如果 Scheduler 在数据写入数据库后、发送到队列前异常退出，数据仍为 `pending` 状态，后续随时可以通过 `publish-pending` 恢复发布。

---

## 开发与测试

集成测试会重建 `TEST_MYSQL_DSN` 指向数据库中的表。该变量必须指向独立测试库，不能指向正在使用的采集数据库。

```bash
# 运行单元测试与集成测试
uv run pytest

# 代码风格与质量检查
uv run ruff check .

# 自动格式化代码
uv run ruff format .
```

---

## 注意事项

1. 本项目仅供技术研究与学习交流，请勿用于非法用途或商业恶意抓取。
2. 使用时请严格遵守目标站点的服务条款与 robots 规则。
3. 请保持低频、友好的请求间隔（默认单 Worker 间隔 5~7 秒）。403 或验证码页面会被标记为 blocked；单个 Worker 连续 3 次收到 429 时会非零退出，此时应及时人工检查。
