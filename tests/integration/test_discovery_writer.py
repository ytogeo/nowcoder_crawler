from __future__ import annotations

from datetime import datetime

from sqlalchemy import event, func, select

from nowcoder_crawler.database import Database
from nowcoder_crawler.discovery import DiscoveredPage
from nowcoder_crawler.discovery.writer import write_discovered_batch
from nowcoder_crawler.identity import build_identity
from nowcoder_crawler.models import CrawlRun, Page, PageSource, utc_now


def _run(database: Database) -> int:
    with database.session() as session:
        run = CrawlRun(status="running", sources_json=["test"], started_at=utc_now())
        session.add(run)
        session.flush()
        return run.id


def _item(
    external_id: str,
    *,
    source_type: str,
    source_key: str,
    source_page: int | None,
    modified: datetime | None = None,
) -> DiscoveredPage:
    return DiscoveredPage(
        identity=build_identity("discussion", external_id),
        source_type=source_type,
        source_key=source_key,
        source_modified_at=modified,
        source_page=source_page,
    )


def test_batch_deduplicates_pages_and_bulk_loads_existing_rows(
    database: Database,
) -> None:
    batch = [
        _item("1", source_type="center", source_key="center::-1:1", source_page=1),
        _item("1", source_type="center", source_key="center::-1:1", source_page=2),
        _item(
            "1",
            source_type="sitemap",
            source_key="sitemap:https://www.nowcoder.com/sitemap-posts.xml",
            source_page=None,
        ),
    ]
    selects: list[str] = []

    def count_selects(_conn, _cursor, statement, _parameters, _context, _many):
        if statement.lstrip().upper().startswith("SELECT"):
            selects.append(statement)

    event.listen(database.engine, "before_cursor_execute", count_selects)
    try:
        result = write_discovered_batch(database, _run(database), batch)
    finally:
        event.remove(database.engine, "before_cursor_execute", count_selects)

    assert len(selects) == 2
    assert result.pages_inserted == 1
    assert len(result.dispatchable_page_ids) == 1
    with database.session() as session:
        assert session.scalar(select(func.count()).select_from(Page)) == 1
        assert session.scalar(select(func.count()).select_from(PageSource)) == 2
        center = session.scalar(
            select(PageSource).where(PageSource.source_type == "center")
        )
        assert center is not None
        assert center.first_seen_page == 1
        assert center.last_seen_page == 2

    repeated = write_discovered_batch(database, _run(database), batch)
    assert repeated.pages_updated == 1
    assert repeated.dispatchable_page_ids == ()


def test_only_explicitly_updated_success_page_becomes_dispatchable(
    database: Database,
) -> None:
    fetched_at = datetime(2026, 1, 2)
    with database.session() as session:
        success = Page(
            page_type="discussion",
            external_id="1",
            canonical_url=build_identity("discussion", "1").canonical_url,
            status="success",
            last_fetched_at=fetched_at,
        )
        retryable = Page(
            page_type="discussion",
            external_id="2",
            canonical_url=build_identity("discussion", "2").canonical_url,
            status="failed",
            last_error_type="retryable",
        )
        session.add_all([success, retryable])
        session.flush()
        success_id = success.id

    result = write_discovered_batch(
        database,
        _run(database),
        [
            _item(
                "1",
                source_type="center",
                source_key="center::-1:1",
                source_page=1,
                modified=datetime(2026, 1, 3),
            ),
            _item(
                "2",
                source_type="center",
                source_key="center::-1:1",
                source_page=1,
                modified=datetime(2026, 1, 3),
            ),
        ],
    )

    assert result.dispatchable_page_ids == (success_id,)
    with database.session() as session:
        pages = {
            page.external_id: page for page in session.scalars(select(Page)).all()
        }
        assert pages["1"].status == "pending"
        assert pages["2"].status == "failed"
