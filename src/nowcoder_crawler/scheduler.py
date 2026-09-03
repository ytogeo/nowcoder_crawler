from __future__ import annotations

import logging
from dataclasses import dataclass

import httpx
from sqlalchemy import or_, select

from .config import Settings
from .database import Database
from .discovery.service import DiscoveryService
from .models import CrawlRun, Page, utc_now
from .rabbit import RabbitPublisher

LOGGER = logging.getLogger("nowcoder_crawler.scheduler")


# 记录单次调度运行（crawl_run）的统计计数器
@dataclass
class RunCounters:
    center_pages_seen: int = 0
    sitemap_docs_seen: int = 0
    urls_seen: int = 0
    pages_inserted: int = 0
    pages_updated: int = 0
    messages_published: int = 0


async def _publish_candidates(
    database: Database, publisher: RabbitPublisher, counters: RunCounters
) -> None:
    """筛选待抓取的页面（pending 或可重试的 failed），发布到 RabbitMQ 队列。"""
    with database.session() as session:
        candidates = list(
            session.scalars(
                select(Page).where(
                    or_(
                        Page.status == "pending",
                        (Page.status == "failed") & (Page.last_error_type == "retryable"),
                    )
                )
            )
        )
    for page in candidates:
        await publisher.publish_page(page.id)
        counters.messages_published += 1
        # 发布后将 retryable 的失败状态重置回 pending
        if page.status == "failed":
            with database.session() as session:
                current = session.get(Page, page.id)
                if (
                    current is not None
                    and current.status == "failed"
                    and current.last_error_type == "retryable"
                ):
                    current.status = "pending"
        LOGGER.info("message_published page_id=%s", page.id)


async def run_scheduler(settings: Settings, *, sources: tuple[str, ...], max_pages: int) -> int:
    """调度器主入口：一次性执行发现、Upsert 数据库和发布抓取任务。"""
    invalid = set(sources) - {"experience-api", "sitemap"}
    if invalid:
        raise ValueError(f"unsupported sources: {sorted(invalid)}")
    database = Database(settings.mysql_dsn)
    database.create_schema()
    counters = RunCounters()

    # 1. 创建本次 crawl_runs 运行记录
    with database.session() as session:
        run = CrawlRun(status="running", sources_json=list(sources), started_at=utc_now())
        session.add(run)
        session.flush()
        run_id = run.id

    publisher: RabbitPublisher | None = None
    try:
        timeout = httpx.Timeout(
            settings.fetch_timeout_seconds,
            connect=settings.fetch_connect_timeout_seconds,
        )
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            discovery = await DiscoveryService(
                database,
                run_id,
                settings,
                sources=sources,
                experience_api_max_pages=max_pages,
            ).run(client)

        counters.urls_seen = discovery.writes.observations
        counters.pages_inserted = discovery.writes.pages_inserted
        counters.pages_updated = discovery.writes.pages_updated
        if api_result := discovery.sources.get("experience-api"):
            counters.center_pages_seen = api_result.units_seen
        if sitemap_result := discovery.sources.get("sitemap"):
            counters.sitemap_docs_seen = sitemap_result.units_seen
        if not discovery.complete:
            failed = [
                f"{name}: {result.error or result.stop_reason}"
                for name, result in discovery.sources.items()
                if not result.complete
            ]
            raise RuntimeError("discovery incomplete: " + "; ".join(failed))

        # 3. 连接 RabbitMQ 并将待抓取候选页面推入队列
        publisher = await RabbitPublisher.connect(settings.rabbitmq_url, settings.fetch_queue)
        await _publish_candidates(database, publisher, counters)

        # 4. 统计指标并标记本次运行为 success
        with database.session() as session:
            run = session.get(CrawlRun, run_id)
            assert run is not None
            run.status = "success"
            run.finished_at = utc_now()
            run.center_pages_seen = counters.center_pages_seen
            run.sitemap_docs_seen = counters.sitemap_docs_seen
            run.urls_seen = counters.urls_seen
            run.pages_inserted = counters.pages_inserted
            run.pages_updated = counters.pages_updated
            run.messages_published = counters.messages_published
        LOGGER.info(
            "crawl_run_success crawl_run_id=%s urls=%s inserted=%s updated=%s published=%s",
            run_id,
            counters.urls_seen,
            counters.pages_inserted,
            counters.pages_updated,
            counters.messages_published,
        )
        return run_id
    except Exception as exc:
        # 异常时记录错误信息并将 crawl_runs 标记为 failed
        with database.session() as session:
            run = session.get(CrawlRun, run_id)
            if run is not None:
                run.status = "failed"
                run.finished_at = utc_now()
                run.error_message = str(exc)[:4000]
        LOGGER.exception("crawl_run_failed crawl_run_id=%s", run_id)
        raise
    finally:
        if publisher is not None:
            await publisher.close()
        database.close()
