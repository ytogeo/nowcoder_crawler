from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime

import httpx
from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from .config import Settings
from .database import Database
from .discovery import DiscoveredPage
from .discovery.center import CenterQuery, discover_center
from .discovery.sitemap import discover_sitemaps
from .models import CrawlRun, Page, PageSource, utc_now
from .rabbit import RabbitPublisher

LOGGER = logging.getLogger("nowcoder_crawler.scheduler")


@dataclass
class RunCounters:
    center_pages_seen: int = 0
    sitemap_docs_seen: int = 0
    urls_seen: int = 0
    pages_inserted: int = 0
    pages_updated: int = 0
    messages_published: int = 0


def _later(left: datetime | None, right: datetime | None) -> datetime | None:
    if left is None:
        return right
    if right is None:
        return left
    return max(left, right)


def upsert_discovered_pages(
    session: Session,
    run_id: int,
    discovered: list[DiscoveredPage],
    counters: RunCounters,
) -> int:
    now = utc_now()
    activity = 0
    for item in discovered:
        identity = item.identity
        page = session.scalar(
            select(Page).where(
                Page.page_type == identity.page_type,
                Page.external_id == identity.external_id,
            )
        )
        is_new = page is None
        if page is None:
            page = Page(
                page_type=identity.page_type,
                external_id=identity.external_id,
                canonical_url=identity.canonical_url,
                status="pending",
                first_seen_at=now,
                last_seen_at=now,
                source_modified_at=item.source_modified_at,
            )
            session.add(page)
            session.flush()
            counters.pages_inserted += 1
            activity += 1
        else:
            page.last_seen_at = now
            previous_modified = page.source_modified_at
            page.source_modified_at = _later(previous_modified, item.source_modified_at)
            if item.source_modified_at is not None and (
                previous_modified is None or item.source_modified_at > previous_modified
            ):
                activity += 1
                if (
                    page.status == "success"
                    and page.last_fetched_at is not None
                    and item.source_modified_at > page.last_fetched_at
                ):
                    page.status = "pending"
                    page.last_error_type = None
                    page.last_error_message = None
            counters.pages_updated += 1

        source = session.scalar(
            select(PageSource).where(
                PageSource.page_id == page.id,
                PageSource.source_type == item.source_type,
                PageSource.source_key == item.source_key,
            )
        )
        if source is None:
            source = PageSource(
                page_id=page.id,
                source_type=item.source_type,
                source_key=item.source_key,
                company_ids_json=(list(item.company_ids) if item.company_ids is not None else None),
                job_id=item.job_id,
                job_level=item.job_level,
                first_seen_page=item.source_page,
                last_seen_page=item.source_page,
                source_modified_at=item.source_modified_at,
                first_seen_at=now,
                last_seen_at=now,
                first_crawl_run_id=run_id,
                last_crawl_run_id=run_id,
            )
            session.add(source)
        else:
            source.last_seen_at = now
            source.last_seen_page = item.source_page
            source.last_crawl_run_id = run_id
            source.source_modified_at = _later(source.source_modified_at, item.source_modified_at)
        if is_new:
            LOGGER.info(
                "page_discovered page_id=%s page_type=%s external_id=%s source=%s",
                page.id,
                page.page_type,
                page.external_id,
                item.source_type,
            )
    counters.urls_seen += len(discovered)
    return activity


async def _publish_candidates(
    database: Database, publisher: RabbitPublisher, counters: RunCounters
) -> None:
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
    invalid = set(sources) - {"center", "sitemap"}
    if invalid:
        raise ValueError(f"unsupported sources: {sorted(invalid)}")
    database = Database(settings.mysql_dsn)
    database.create_schema()
    counters = RunCounters()
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
            if "center" in sources:

                async def on_center_page(page_number: int, pages: list[DiscoveredPage]) -> int:
                    del page_number
                    with database.session() as session:
                        return upsert_discovered_pages(session, run_id, pages, counters)

                center_stats = await discover_center(
                    client,
                    query=CenterQuery(),
                    max_pages=max_pages,
                    stale_pages=settings.center_stale_pages,
                    on_page=on_center_page,
                )
                counters.center_pages_seen = center_stats.pages_seen

            if "sitemap" in sources:

                async def on_sitemap_pages(pages: list[DiscoveredPage]) -> int:
                    with database.session() as session:
                        return upsert_discovered_pages(session, run_id, pages, counters)

                sitemap_stats = await discover_sitemaps(
                    client,
                    roots=settings.sitemap_root_urls,
                    max_documents=settings.sitemap_max_documents,
                    max_urls=settings.sitemap_max_urls,
                    on_pages=on_sitemap_pages,
                )
                counters.sitemap_docs_seen = sitemap_stats.documents_seen

        publisher = await RabbitPublisher.connect(settings.rabbitmq_url, settings.fetch_queue)
        await _publish_candidates(database, publisher, counters)
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
