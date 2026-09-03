from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import select, tuple_

from nowcoder_crawler.database import Database
from nowcoder_crawler.identity import PageIdentity
from nowcoder_crawler.models import Page, PageSource, utc_now

from . import DiscoveredPage


def _later(left: datetime | None, right: datetime | None) -> datetime | None:
    if left is None:
        return right
    if right is None:
        return left
    return max(left, right)


@dataclass
class PageAggregate:
    identity: PageIdentity
    source_modified_at: datetime | None


@dataclass
class SourceAggregate:
    page_key: tuple[str, str]
    source_type: str
    source_key: str
    company_ids: tuple[int, ...] | None
    job_id: int | None
    job_level: int | None
    first_seen_page: int | None
    last_seen_page: int | None
    source_modified_at: datetime | None


@dataclass(frozen=True)
class AggregatedBatch:
    pages_by_identity: dict[tuple[str, str], PageAggregate]
    sources_by_key: dict[tuple[tuple[str, str], str, str], SourceAggregate]
    observations: int


@dataclass(frozen=True)
class CommittedBatchResult:
    observations: int
    pages_inserted: int
    pages_updated: int
    dispatchable_page_ids: tuple[int, ...]


@dataclass
class DiscoveryWriteStats:
    observations: int = 0
    pages_inserted: int = 0
    pages_updated: int = 0
    batches_written: int = 0

    def include(self, result: CommittedBatchResult) -> None:
        self.observations += result.observations
        self.pages_inserted += result.pages_inserted
        self.pages_updated += result.pages_updated
        self.batches_written += 1


def aggregate_discovered_batch(batch: list[DiscoveredPage]) -> AggregatedBatch:
    pages_by_identity: dict[tuple[str, str], PageAggregate] = {}
    sources_by_key: dict[tuple[tuple[str, str], str, str], SourceAggregate] = {}

    for item in batch:
        identity = item.identity
        page_key = (identity.page_type, identity.external_id)
        page = pages_by_identity.get(page_key)
        if page is None:
            pages_by_identity[page_key] = PageAggregate(identity, item.source_modified_at)
        else:
            page.source_modified_at = _later(page.source_modified_at, item.source_modified_at)

        lineage_key = (page_key, item.source_type, item.source_key)
        source = sources_by_key.get(lineage_key)
        if source is None:
            sources_by_key[lineage_key] = SourceAggregate(
                page_key=page_key,
                source_type=item.source_type,
                source_key=item.source_key,
                company_ids=item.company_ids,
                job_id=item.job_id,
                job_level=item.job_level,
                first_seen_page=item.source_page,
                last_seen_page=item.source_page,
                source_modified_at=item.source_modified_at,
            )
        else:
            source.last_seen_page = item.source_page
            source.source_modified_at = _later(
                source.source_modified_at, item.source_modified_at
            )

    return AggregatedBatch(pages_by_identity, sources_by_key, len(batch))


def write_discovered_batch(
    database: Database,
    run_id: int,
    batch: list[DiscoveredPage],
) -> CommittedBatchResult:
    aggregated = aggregate_discovered_batch(batch)
    if not aggregated.pages_by_identity:
        return CommittedBatchResult(0, 0, 0, ())

    now = utc_now()
    inserted = 0
    updated = 0
    dispatchable_pages: list[Page] = []
    identity_keys = list(aggregated.pages_by_identity)

    with database.session() as session:
        existing_pages = list(
            session.scalars(
                select(Page).where(
                    tuple_(Page.page_type, Page.external_id).in_(identity_keys)
                )
            )
        )
        pages_by_identity = {
            (page.page_type, page.external_id): page for page in existing_pages
        }

        for page_key, aggregate in aggregated.pages_by_identity.items():
            page = pages_by_identity.get(page_key)
            if page is None:
                page = Page(
                    page_type=aggregate.identity.page_type,
                    external_id=aggregate.identity.external_id,
                    canonical_url=aggregate.identity.canonical_url,
                    status="pending",
                    first_seen_at=now,
                    last_seen_at=now,
                    source_modified_at=aggregate.source_modified_at,
                )
                session.add(page)
                pages_by_identity[page_key] = page
                dispatchable_pages.append(page)
                inserted += 1
                continue

            page.last_seen_at = now
            page.source_modified_at = _later(
                page.source_modified_at, aggregate.source_modified_at
            )
            if (
                page.status == "success"
                and page.last_fetched_at is not None
                and aggregate.source_modified_at is not None
                and aggregate.source_modified_at > page.last_fetched_at
            ):
                page.status = "pending"
                page.last_error_type = None
                page.last_error_message = None
                dispatchable_pages.append(page)
            updated += 1

        session.flush()

        source_db_keys = [
            (
                pages_by_identity[source.page_key].id,
                source.source_type,
                source.source_key,
            )
            for source in aggregated.sources_by_key.values()
        ]
        existing_sources = list(
            session.scalars(
                select(PageSource).where(
                    tuple_(
                        PageSource.page_id,
                        PageSource.source_type,
                        PageSource.source_key,
                    ).in_(source_db_keys)
                )
            )
        )
        sources_by_key = {
            (source.page_id, source.source_type, source.source_key): source
            for source in existing_sources
        }

        for aggregate in aggregated.sources_by_key.values():
            page_id = pages_by_identity[aggregate.page_key].id
            source_key = (page_id, aggregate.source_type, aggregate.source_key)
            source = sources_by_key.get(source_key)
            if source is None:
                session.add(
                    PageSource(
                        page_id=page_id,
                        source_type=aggregate.source_type,
                        source_key=aggregate.source_key,
                        company_ids_json=(
                            list(aggregate.company_ids)
                            if aggregate.company_ids is not None
                            else None
                        ),
                        job_id=aggregate.job_id,
                        job_level=aggregate.job_level,
                        first_seen_page=aggregate.first_seen_page,
                        last_seen_page=aggregate.last_seen_page,
                        source_modified_at=aggregate.source_modified_at,
                        first_seen_at=now,
                        last_seen_at=now,
                        first_crawl_run_id=run_id,
                        last_crawl_run_id=run_id,
                    )
                )
                continue

            source.last_seen_at = now
            source.last_seen_page = aggregate.last_seen_page
            source.last_crawl_run_id = run_id
            source.source_modified_at = _later(
                source.source_modified_at, aggregate.source_modified_at
            )

        dispatchable_page_ids = tuple(page.id for page in dispatchable_pages)

    return CommittedBatchResult(
        observations=aggregated.observations,
        pages_inserted=inserted,
        pages_updated=updated,
        dispatchable_page_ids=dispatchable_page_ids,
    )


class DiscoveryWriter:
    def __init__(self, database: Database, run_id: int, batch_size: int) -> None:
        if batch_size < 1:
            raise ValueError("discovery batch_size must be positive")
        self.database = database
        self.run_id = run_id
        self.batch_size = batch_size

    async def run(
        self,
        queue: asyncio.Queue[DiscoveredPage | object],
        stop_token: object,
    ) -> AsyncIterator[CommittedBatchResult]:
        batch: list[DiscoveredPage] = []

        while True:
            item = await queue.get()
            if item is stop_token:
                if batch:
                    yield await self._write(batch)
                return

            assert isinstance(item, DiscoveredPage)
            batch.append(item)
            if len(batch) >= self.batch_size:
                yield await self._write(batch)
                batch = []

    async def _write(self, batch: list[DiscoveredPage]) -> CommittedBatchResult:
        return await asyncio.to_thread(
            write_discovered_batch,
            self.database,
            self.run_id,
            batch,
        )
