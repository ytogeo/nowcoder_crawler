from __future__ import annotations

import logging
from dataclasses import dataclass

import httpx
from sqlalchemy import or_, select

from .config import Settings
from .database import Database
from .discovery.service import DiscoveryResult, DiscoveryService, SourceResult
from .discovery.writer import CommittedBatchResult
from .models import CrawlRun, Page, utc_now
from .rabbit import RabbitPublisher

LOGGER = logging.getLogger("nowcoder_crawler.scheduler")


@dataclass
class RunCounters:
    center_pages_seen: int = 0
    sitemap_docs_seen: int = 0
    urls_seen: int = 0
    pages_inserted: int = 0
    pages_updated: int = 0


@dataclass
class DispatchState:
    enabled: bool
    publisher: RabbitPublisher | None = None
    backlog_published: int = 0
    batch_published: int = 0
    failed: bool = False
    error: str | None = None

    def mark_failed(self, exc: Exception) -> None:
        if not self.failed:
            self.failed = True
            self.error = str(exc)
            LOGGER.exception("publisher_failed error=%s", exc)

    async def publish_ids(self, page_ids: tuple[int, ...], *, backlog: bool) -> None:
        if not self.enabled or self.failed or self.publisher is None:
            return
        for page_id in page_ids:
            try:
                await self.publisher.publish_page(page_id)
            except Exception as exc:
                self.mark_failed(exc)
                return
            if backlog:
                self.backlog_published += 1
            else:
                self.batch_published += 1
            LOGGER.info("message_published page_id=%s backlog=%s", page_id, backlog)

    async def close(self) -> None:
        if self.publisher is None:
            return
        try:
            await self.publisher.close()
        except Exception as exc:
            self.mark_failed(exc)

    @property
    def messages_published(self) -> int:
        return self.backlog_published + self.batch_published


def _candidate_rows(database: Database) -> list[tuple[int, str, str | None]]:
    with database.session() as session:
        return list(
            session.execute(
                select(Page.id, Page.status, Page.last_error_type).where(
                    or_(
                        Page.status == "pending",
                        (Page.status == "failed")
                        & (Page.last_error_type == "retryable"),
                    )
                )
            ).tuples()
        )


async def _publish_backlog(database: Database, dispatch: DispatchState) -> None:
    for page_id, status, error_type in _candidate_rows(database):
        before = dispatch.backlog_published
        await dispatch.publish_ids((page_id,), backlog=True)
        if dispatch.backlog_published == before:
            return
        if status == "failed" and error_type == "retryable":
            with database.session() as session:
                page = session.get(Page, page_id)
                if (
                    page is not None
                    and page.status == "failed"
                    and page.last_error_type == "retryable"
                ):
                    page.status = "pending"


def _source_report(result: SourceResult) -> dict[str, object]:
    return {
        "complete": result.complete,
        "stop_reason": result.stop_reason,
        "upstream_exhausted": result.upstream_exhausted,
        "documents_or_pages_seen": result.units_seen,
        "urls_seen": result.urls_seen,
        "error": result.error,
    }


def _run_report(
    *,
    mode: str,
    sources: tuple[str, ...],
    settings: Settings,
    experience_api_max_pages: int,
    discovery: DiscoveryResult | None,
    dispatch: DispatchState,
) -> dict[str, object]:
    return {
        "schema_version": 2,
        "mode": mode,
        "requested": list(sources),
        "config": {
            "experience_api_max_pages": experience_api_max_pages,
            "sitemap_max_documents": settings.sitemap_max_documents,
            "sitemap_max_urls": settings.sitemap_max_urls,
        },
        "results": (
            {
                name: _source_report(result)
                for name, result in discovery.sources.items()
            }
            if discovery is not None
            else {}
        ),
        "dispatch": {
            "enabled": dispatch.enabled,
            "backlog_published": dispatch.backlog_published,
            "batch_published": dispatch.batch_published,
            "failed": dispatch.failed,
            "error": dispatch.error,
        },
    }


def _failure_message(
    fatal_error: Exception | None,
    discovery: DiscoveryResult | None,
    dispatch: DispatchState,
) -> str | None:
    if fatal_error is not None:
        return str(fatal_error)
    if discovery is not None and not discovery.complete:
        failed = [
            f"{name}: {result.error or result.stop_reason}"
            for name, result in discovery.sources.items()
            if not result.complete
        ]
        return "discovery incomplete: " + "; ".join(failed)
    if dispatch.failed:
        return f"publisher failed: {dispatch.error}"
    return None


async def _run_discovery_job(
    settings: Settings,
    *,
    mode: str,
    sources: tuple[str, ...],
    max_pages: int,
) -> int:
    invalid = set(sources) - {"experience-api", "sitemap"}
    if invalid:
        raise ValueError(f"unsupported sources: {sorted(invalid)}")
    if not sources:
        raise ValueError("at least one discovery source is required")

    database = Database(settings.mysql_dsn)
    database.create_schema()
    counters = RunCounters()
    dispatch = DispatchState(enabled=mode == "full-scan")
    discovery: DiscoveryResult | None = None
    fatal_error: Exception | None = None

    with database.session() as session:
        run = CrawlRun(
            status="running",
            sources_json={"schema_version": 2, "mode": mode, "requested": list(sources)},
            started_at=utc_now(),
        )
        session.add(run)
        session.flush()
        run_id = run.id

    try:
        if dispatch.enabled:
            try:
                dispatch.publisher = await RabbitPublisher.connect(
                    settings.rabbitmq_url, settings.fetch_queue
                )
            except Exception as exc:
                dispatch.mark_failed(exc)
            if dispatch.publisher is not None:
                await _publish_backlog(database, dispatch)

        async def on_committed_batch(result: CommittedBatchResult) -> None:
            counters.urls_seen += result.observations
            counters.pages_inserted += result.pages_inserted
            counters.pages_updated += result.pages_updated
            await dispatch.publish_ids(result.dispatchable_page_ids, backlog=False)

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
            ).run(client, on_committed_batch=on_committed_batch)
    except Exception as exc:
        fatal_error = exc
        LOGGER.exception("crawl_run_failed crawl_run_id=%s", run_id)
    finally:
        await dispatch.close()

    if discovery is not None:
        if api_result := discovery.sources.get("experience-api"):
            counters.center_pages_seen = api_result.units_seen
        if sitemap_result := discovery.sources.get("sitemap"):
            counters.sitemap_docs_seen = sitemap_result.units_seen

    error_message = _failure_message(fatal_error, discovery, dispatch)
    status = "failed" if error_message is not None else "success"
    with database.session() as session:
        run = session.get(CrawlRun, run_id)
        assert run is not None
        run.status = status
        run.finished_at = utc_now()
        run.sources_json = _run_report(
            mode=mode,
            sources=sources,
            settings=settings,
            experience_api_max_pages=max_pages,
            discovery=discovery,
            dispatch=dispatch,
        )
        run.center_pages_seen = counters.center_pages_seen
        run.sitemap_docs_seen = counters.sitemap_docs_seen
        run.urls_seen = counters.urls_seen
        run.pages_inserted = counters.pages_inserted
        run.pages_updated = counters.pages_updated
        run.messages_published = dispatch.messages_published
        run.error_message = error_message[:4000] if error_message is not None else None
    database.close()

    LOGGER.info(
        "crawl_run_finished crawl_run_id=%s status=%s urls=%s inserted=%s "
        "updated=%s published=%s",
        run_id,
        status,
        counters.urls_seen,
        counters.pages_inserted,
        counters.pages_updated,
        dispatch.messages_published,
    )
    if error_message is not None:
        raise RuntimeError(error_message) from fatal_error
    return run_id


async def run_full_scan(
    settings: Settings, *, sources: tuple[str, ...], max_pages: int
) -> int:
    return await _run_discovery_job(
        settings,
        mode="full-scan",
        sources=sources,
        max_pages=max_pages,
    )


async def run_discover_only(
    settings: Settings, *, sources: tuple[str, ...], max_pages: int
) -> int:
    return await _run_discovery_job(
        settings,
        mode="discover-only",
        sources=sources,
        max_pages=max_pages,
    )


async def run_publish_pending(settings: Settings) -> int:
    database = Database(settings.mysql_dsn)
    database.create_schema()
    dispatch = DispatchState(enabled=True)
    try:
        dispatch.publisher = await RabbitPublisher.connect(
            settings.rabbitmq_url, settings.fetch_queue
        )
        await _publish_backlog(database, dispatch)
    except Exception as exc:
        dispatch.mark_failed(exc)
    finally:
        await dispatch.close()
        database.close()

    if dispatch.failed:
        raise RuntimeError(f"publisher failed: {dispatch.error}")
    return dispatch.backlog_published
