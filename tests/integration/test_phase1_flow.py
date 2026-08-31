from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import httpx
from sqlalchemy import func, select

from nowcoder_crawler.config import Settings
from nowcoder_crawler.database import Database
from nowcoder_crawler.discovery import DiscoveredPage
from nowcoder_crawler.fetcher import RequestPacer, WorkerFetchState
from nowcoder_crawler.identity import build_identity
from nowcoder_crawler.models import CrawlRun, Page, PageSource, utc_now
from nowcoder_crawler.rabbit import encode_page_message
from nowcoder_crawler.scheduler import RunCounters, _publish_candidates, upsert_discovered_pages
from nowcoder_crawler.worker import handle_message


def _run(database: Database) -> int:
    with database.session() as session:
        run = CrawlRun(status="running", sources_json=["center"], started_at=utc_now())
        session.add(run)
        session.flush()
        return run.id


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        mysql_dsn="unused",
        rabbitmq_url="amqp://guest:guest@localhost/",
        raw_data_dir=tmp_path,
        fetch_queue="fetch.ready",
        worker_prefetch=2,
        fetch_connect_timeout_seconds=1,
        fetch_timeout_seconds=1,
        fetch_max_attempts=3,
        fetch_base_delay_seconds=0,
        fetch_jitter_seconds=0,
        retry_after_default_seconds=0,
        retry_after_max_seconds=0,
        center_max_pages=10,
        center_stale_pages=3,
        sitemap_max_documents=20,
        sitemap_max_urls=100,
        sitemap_root_urls=("https://www.nowcoder.com/sitemap.xml",),
    )


def test_duplicate_discovery_keeps_one_page_and_lineage(database: Database) -> None:
    identity = build_identity("feed", "a" * 32)
    item = DiscoveredPage(
        identity=identity,
        source_type="center",
        source_key="center:147:11200:2",
        company_ids=(147,),
        job_id=11200,
        job_level=2,
        source_page=1,
    )
    with database.session() as session:
        upsert_discovered_pages(session, _run(database), [item], RunCounters())
    with database.session() as session:
        upsert_discovered_pages(
            session,
            _run(database),
            [replace(item, source_page=2)],
            RunCounters(),
        )

    with database.session() as session:
        assert session.scalar(select(func.count()).select_from(Page)) == 1
        assert session.scalar(select(func.count()).select_from(PageSource)) == 1
        source = session.scalar(select(PageSource))
        assert source is not None
        assert source.company_ids_json == [147]
        assert source.first_seen_page == 1
        assert source.last_seen_page == 2


async def test_scheduler_only_publishes_pending_and_retryable(database: Database) -> None:
    rows = [
        ("1", "pending", None),
        ("2", "failed", "retryable"),
        ("3", "failed", "permanent"),
        ("4", "failed", "blocked"),
        ("5", "success", None),
    ]
    with database.session() as session:
        for external_id, status, error_type in rows:
            identity = build_identity("discussion", external_id)
            session.add(
                Page(
                    page_type=identity.page_type,
                    external_id=identity.external_id,
                    canonical_url=identity.canonical_url,
                    status=status,
                    last_error_type=error_type,
                )
            )

    class Publisher:
        page_ids: list[int] = []

        async def publish_page(self, page_id: int) -> None:
            self.page_ids.append(page_id)

    publisher = Publisher()
    counters = RunCounters()
    await _publish_candidates(database, publisher, counters)  # type: ignore[arg-type]

    with database.session() as session:
        published = set(
            session.scalars(select(Page.external_id).where(Page.id.in_(publisher.page_ids)))
        )
        retryable = session.scalar(select(Page).where(Page.external_id == "2"))
        assert retryable is not None
        assert retryable.status == "pending"
    assert published == {"1", "2"}
    assert counters.messages_published == 2


async def test_success_is_committed_and_gzip_exists_before_ack(
    database: Database, tmp_path: Path
) -> None:
    identity = build_identity("discussion", "123456")
    with database.session() as session:
        page = Page(
            page_type=identity.page_type,
            external_id=identity.external_id,
            canonical_url=identity.canonical_url,
            status="pending",
        )
        session.add(page)
        session.flush()
        page_id = page.id

    class Message:
        body = encode_page_message(page_id)
        acked = False

        async def ack(self) -> None:
            with database.session() as session:
                committed = session.get(Page, page_id)
                assert committed is not None
                assert committed.status == "success"
                assert committed.gzip_path is not None
                assert Path(committed.gzip_path).is_file()
            self.acked = True

    def handler(request: httpx.Request) -> httpx.Response:
        body = f"<html><main>{identity.external_id}</main></html>".encode() + b"x" * 600
        return httpx.Response(
            200,
            request=request,
            content=body,
            headers={"content-type": "text/html; charset=utf-8"},
        )

    message = Message()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        should_exit = await handle_message(
            message,  # type: ignore[arg-type]
            database=database,
            client=client,
            settings=_settings(tmp_path),
            worker_id="integration-worker",
            pacer=RequestPacer(0, 0),
            fetch_state=WorkerFetchState(),
        )

    assert message.acked is True
    assert should_exit is False
