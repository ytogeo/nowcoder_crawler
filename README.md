# nowcoder-crawler

一个用于学习采集架构的牛客公开面经增量采集器。Phase 1 只处理 live feed/discussion 原始 HTML，不宣称历史全量，也不做正文解析。

设计边界和验收标准见 [Phase 1 Spec](docs/phase1-spec.md)。

## 本地启动

```powershell
Copy-Item .env.example .env
docker compose up -d
uv sync
```

宿主机 CLI 读取环境变量，不自动加载 `.env`。PowerShell 开发环境可执行：

```powershell
$env:MYSQL_DSN = 'mysql+pymysql://nowcoder:nowcoder@127.0.0.1:3307/nowcoder'
$env:RABBITMQ_URL = 'amqp://guest:guest@127.0.0.1:5672/'
$env:RAW_DATA_DIR = (Resolve-Path './data/raw').Path
```

运行一次发现：

```powershell
uv run nowcoder-crawler scheduler --sources center sitemap --max-pages 10
```

两个终端分别启动 Worker：

```powershell
uv run nowcoder-crawler worker --worker-id worker-1
uv run nowcoder-crawler worker --worker-id worker-2
```

RabbitMQ Management UI：<http://127.0.0.1:15672>，本地默认账号 `guest/guest`。

本项目把 MySQL 暴露到宿主机 `3307`，避免与常见的本机 `3306` 冲突。容器内部仍使用 `3306`。

## 测试

```powershell
uv run ruff check .
uv run pytest tests/unit -q
```

设置 `TEST_MYSQL_DSN` 后可以运行包含 MySQL 的全部测试：

```powershell
$env:TEST_MYSQL_DSN = 'mysql+pymysql://root:root@127.0.0.1:3307/nowcoder_test'
uv run pytest -q
```

## 查看 smoke test

后台 smoke Worker 的日志分别写入 `logs/smoke-worker-1.err.log` 和
`logs/smoke-worker-2.err.log`。实时查看：

```powershell
Get-Content .\logs\smoke-worker-1.err.log -Wait -Tail 30
Get-Content .\logs\smoke-worker-2.err.log -Wait -Tail 30
```

RabbitMQ Management UI 的 **Queues and Streams → fetch.ready** 可以看到 Ready、
Unacked 和 Consumers。也可以在终端查看：

```powershell
docker compose exec -T rabbitmq rabbitmqctl list_queues name messages_ready messages_unacknowledged consumers
```

MySQL 中的抓取结果：

```powershell
docker compose exec -T mysql mysql -unowcoder -pnowcoder nowcoder -e "SELECT status,last_error_type,COUNT(*) FROM pages GROUP BY status,last_error_type; SELECT worker_id,outcome,COUNT(*) FROM fetch_attempts GROUP BY worker_id,outcome;"
```

原始文件数量：

```powershell
Get-ChildItem .\data\raw -Recurse -Filter *.html.gz | Measure-Object
```

真实 smoke test 和 ACK/redelivery 实验的结果见
[Phase 1 验收记录](docs/phase1-acceptance.md)，设计依据见
[Phase 1 Spec](docs/phase1-spec.md)。
