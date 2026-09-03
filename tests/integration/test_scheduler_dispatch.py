from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import func, select

from nowcoder_crawler.config import Settings
from nowcoder_crawler.database import Database
from nowcoder_crawler.discovery import DiscoveredPage
from nowcoder_crawler.discovery.service import DiscoveryResult, SourceResult
from nowcoder_crawler.discovery.writer import DiscoveryWriteStats, write_discovered_batch
from nowcoder_crawler.identity import build_identity
from nowcoder_crawler.models import CrawlRun, Page, PageSource
from nowcoder_crawler.scheduler import (
    run_discover_only,
    run_full_scan,
    run_publish_pending,
)


def _settings(database: Database, tmp_path: Path) -> Settings:
    return Settings(
        mysql_dsn=database.engine.url.render_as_string(hide_password=False),
        rabbitmq_url="amqp://unused/",
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
        experience_api_max_pages=20,
        experience_api_interval_seconds=0,
        experience_api_jitter_seconds=0,
        discovery_queue_maxsize=10,
        discovery_db_batch_size=2,
        sitemap_max_documents=10,
        sitemap_max_urls=100,
        sitemap_root_urls=("https://www.nowcoder.com/sitemap.xml",),
    )


def _item(external_id: str, source_type: str, source_key: str) -> DiscoveredPage:
    return DiscoveredPage(
        identity=build_identity("discussion", external_id),
        source_type=source_type,
        source_key=source_key,
    )


def _retryable_page(database: Database, external_id: str = "1") -> int:
    with database.session() as session:
        identity = build_identity("discussion", external_id)
        page = Page(
            page_type=identity.page_type,
            external_id=identity.external_id,
            canonical_url=identity.canonical_url,
            status="failed",
            last_error_type="retryable",
        )
        session.add(page)
        session.flush()
        return page.id


def _result(*, complete: bool = True) -> DiscoveryResult:
    return DiscoveryResult(
        sources={
            "experience-api": SourceResult(
                "experience-api",
                complete,
                "done" if complete else "error",
                complete,
                1,
                1,
                None if complete else "source failed",
            ),
            "sitemap": SourceResult("sitemap", True, "done", True, 1, 1),
        },
        writes=DiscoveryWriteStats(),
    )


class RecordingPublisher:
    def __init__(self, database: Database, *, fail: bool = False) -> None:
        self.database = database
        self.fail = fail
        self.attempted_page_ids: list[int] = []
        self.page_ids: list[int] = []

    async def publish_page(self, page_id: int) -> None:
        self.attempted_page_ids.append(page_id)
        with self.database.session() as session:
            page = session.get(Page, page_id)
            assert page is not None
            assert page.status == "pending"
        if self.fail:
            raise RuntimeError("rabbit unavailable")
        self.page_ids.append(page_id)

    async def close(self) -> None:
        return None


def _install_publisher(monkeypatch, publisher: RecordingPublisher) -> None:
    async def connect(_url: str, _queue_name: str):
        return publisher

    monkeypatch.setattr(
        "nowcoder_crawler.scheduler.RabbitPublisher.connect", staticmethod(connect)
    )


@pytest.mark.asyncio
async def test_full_scan_publishes_existing_backlog_before_discovery(
    database: Database, monkeypatch, tmp_path: Path
) -> None:
    with database.session() as session:
        identity = build_identity("discussion", "99")
        page = Page(
            page_type=identity.page_type,
            external_id=identity.external_id,
            canonical_url=identity.canonical_url,
            status="pending",
        )
        session.add(page)
        session.flush()
        backlog_id = page.id

    publisher = RecordingPublisher(database)
    _install_publisher(monkeypatch, publisher)

    class Service:
        def __init__(self, *_args, **_kwargs):
            pass

        async def run(self, _client, *, on_committed_batch):
            del on_committed_batch
            assert publisher.page_ids == [backlog_id]
            return _result()

    monkeypatch.setattr("nowcoder_crawler.scheduler.DiscoveryService", Service)
    await run_full_scan(
        _settings(database, tmp_path),
        sources=("experience-api", "sitemap"),
        max_pages=20,
    )

    assert publisher.page_ids == [backlog_id]


@pytest.mark.asyncio
async def test_committed_duplicate_from_two_sources_is_published_once(
    database: Database, monkeypatch, tmp_path: Path
) -> None:
    publisher = RecordingPublisher(database)
    _install_publisher(monkeypatch, publisher)

    class Service:
        def __init__(self, db, run_id, *_args, **_kwargs):
            self.db = db
            self.run_id = run_id

        async def run(self, _client, *, on_committed_batch):
            first = write_discovered_batch(
                self.db,
                self.run_id,
                [_item("1", "center", "center::-1:1")],
            )
            await on_committed_batch(first)
            second = write_discovered_batch(
                self.db,
                self.run_id,
                [
                    _item(
                        "1",
                        "sitemap",
                        "sitemap:https://www.nowcoder.com/sitemap-posts.xml",
                    )
                ],
            )
            await on_committed_batch(second)
            return _result()

    monkeypatch.setattr("nowcoder_crawler.scheduler.DiscoveryService", Service)
    await run_full_scan(
        _settings(database, tmp_path),
        sources=("experience-api", "sitemap"),
        max_pages=20,
    )

    assert len(publisher.page_ids) == 1
    with database.session() as session:
        assert session.scalar(select(func.count()).select_from(Page)) == 1
        assert session.scalar(select(func.count()).select_from(PageSource)) == 2


@pytest.mark.asyncio
async def test_publisher_failure_leaves_pending_and_discovery_keeps_writing(
    database: Database, monkeypatch, tmp_path: Path
) -> None:
    publisher = RecordingPublisher(database, fail=True)
    _install_publisher(monkeypatch, publisher)

    class Service:
        def __init__(self, db, run_id, *_args, **_kwargs):
            self.db = db
            self.run_id = run_id

        async def run(self, _client, *, on_committed_batch):
            for external_id in ("1", "2"):
                result = write_discovered_batch(
                    self.db,
                    self.run_id,
                    [_item(external_id, "center", "center::-1:1")],
                )
                await on_committed_batch(result)
            return _result()

    monkeypatch.setattr("nowcoder_crawler.scheduler.DiscoveryService", Service)
    with pytest.raises(RuntimeError, match="publisher failed"):
        await run_full_scan(
            _settings(database, tmp_path),
            sources=("experience-api", "sitemap"),
            max_pages=20,
        )

    with database.session() as session:
        pages = list(session.scalars(select(Page)))
        run = session.scalar(select(CrawlRun).order_by(CrawlRun.id.desc()))
        assert [page.status for page in pages] == ["pending", "pending"]
        assert run is not None
        assert run.status == "failed"
        assert run.pages_inserted == 2
        assert run.sources_json["dispatch"]["failed"] is True
    assert len(publisher.attempted_page_ids) == 1


@pytest.mark.asyncio
async def test_late_source_failure_does_not_retract_published_batch(
    database: Database, monkeypatch, tmp_path: Path
) -> None:
    publisher = RecordingPublisher(database)
    _install_publisher(monkeypatch, publisher)

    class Service:
        def __init__(self, db, run_id, *_args, **_kwargs):
            self.db = db
            self.run_id = run_id

        async def run(self, _client, *, on_committed_batch):
            committed = write_discovered_batch(
                self.db,
                self.run_id,
                [_item("1", "center", "center::-1:1")],
            )
            await on_committed_batch(committed)
            return _result(complete=False)

    monkeypatch.setattr("nowcoder_crawler.scheduler.DiscoveryService", Service)
    with pytest.raises(RuntimeError, match="discovery incomplete"):
        await run_full_scan(
            _settings(database, tmp_path),
            sources=("experience-api", "sitemap"),
            max_pages=20,
        )

    assert len(publisher.page_ids) == 1
    with database.session() as session:
        run = session.scalar(select(CrawlRun).order_by(CrawlRun.id.desc()))
        assert run is not None
        assert run.status == "failed"
        assert run.messages_published == 1


@pytest.mark.asyncio
async def test_discover_only_commits_without_connecting_rabbitmq(
    database: Database, monkeypatch, tmp_path: Path
) -> None:
    async def unexpected_connect(_url: str, _queue_name: str):
        raise AssertionError("discover-only must not connect RabbitMQ")

    monkeypatch.setattr(
        "nowcoder_crawler.scheduler.RabbitPublisher.connect",
        staticmethod(unexpected_connect),
    )

    class Service:
        def __init__(self, db, run_id, *_args, **_kwargs):
            self.db = db
            self.run_id = run_id

        async def run(self, _client, *, on_committed_batch):
            committed = write_discovered_batch(
                self.db,
                self.run_id,
                [_item("1", "center", "center::-1:1")],
            )
            await on_committed_batch(committed)
            return _result()

    monkeypatch.setattr("nowcoder_crawler.scheduler.DiscoveryService", Service)
    await run_discover_only(
        _settings(database, tmp_path),
        sources=("experience-api", "sitemap"),
        max_pages=20,
    )

    with database.session() as session:
        page = session.scalar(select(Page))
        run = session.scalar(select(CrawlRun).order_by(CrawlRun.id.desc()))
        assert page is not None and page.status == "pending"
        assert run is not None and run.status == "success"
        assert run.sources_json["dispatch"]["enabled"] is False


@pytest.mark.asyncio
async def test_fast_worker_failure_is_not_overwritten_after_publish(
    database: Database, monkeypatch, tmp_path: Path
) -> None:
    page_id = _retryable_page(database)

    class RefailingPublisher:
        async def publish_page(self, published_page_id: int) -> None:
            assert published_page_id == page_id
            with database.session() as session:
                page = session.get(Page, page_id)
                assert page is not None and page.status == "pending"
                page.status = "failed"
                page.last_error_type = "retryable"

        async def close(self) -> None:
            return None

    async def connect(_url: str, _queue_name: str):
        return RefailingPublisher()

    monkeypatch.setattr(
        "nowcoder_crawler.scheduler.RabbitPublisher.connect", staticmethod(connect)
    )

    assert await run_publish_pending(_settings(database, tmp_path)) == 1
    with database.session() as session:
        page = session.get(Page, page_id)
        assert page is not None
        assert page.status == "failed"
        assert page.last_error_type == "retryable"


@pytest.mark.asyncio
async def test_publish_failure_leaves_retryable_backlog_pending(
    database: Database, monkeypatch, tmp_path: Path
) -> None:
    page_id = _retryable_page(database)
    publisher = RecordingPublisher(database, fail=True)
    _install_publisher(monkeypatch, publisher)

    with pytest.raises(RuntimeError, match="publisher failed"):
        await run_publish_pending(_settings(database, tmp_path))

    with database.session() as session:
        page = session.get(Page, page_id)
        assert page is not None
        assert page.status == "pending"
