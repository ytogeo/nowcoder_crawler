from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import httpx

from nowcoder_crawler.config import Settings
from nowcoder_crawler.database import Database

from . import DiscoveredPage
from .experience_api import ExperienceApiDiscoverer
from .sitemap import discover_sitemaps
from .writer import CommittedBatchResult, DiscoveryWriter, DiscoveryWriteStats

LOGGER = logging.getLogger("nowcoder_crawler.discovery.service")

EmitPage = Callable[[DiscoveredPage], Awaitable[None]]
Producer = Callable[[EmitPage], Awaitable["SourceResult"]]
CommittedBatchHandler = Callable[[CommittedBatchResult], Awaitable[None]]


@dataclass(frozen=True)
class SourceResult:
    name: str
    complete: bool
    stop_reason: str
    upstream_exhausted: bool
    units_seen: int
    urls_seen: int
    error: str | None = None


@dataclass(frozen=True)
class DiscoveryResult:
    sources: dict[str, SourceResult]
    writes: DiscoveryWriteStats

    @property
    def complete(self) -> bool:
        return all(source.complete for source in self.sources.values())


async def _run_producer(name: str, producer: Producer, emit: EmitPage) -> SourceResult:
    try:
        return await producer(emit)
    except Exception as exc:
        LOGGER.exception("discovery_source_failed source=%s", name)
        return SourceResult(name, False, "error", False, 0, 0, str(exc))


async def run_discovery_pipeline(
    producers: dict[str, Producer],
    writer: DiscoveryWriter,
    *,
    queue_maxsize: int,
    on_committed_batch: CommittedBatchHandler | None = None,
) -> DiscoveryResult:
    if queue_maxsize < 1:
        raise ValueError("discovery queue_maxsize must be positive")
    if not producers:
        raise ValueError("at least one discovery producer is required")

    stop_token = object()
    queue: asyncio.Queue[DiscoveredPage | object] = asyncio.Queue(maxsize=queue_maxsize)

    async def emit(page: DiscoveredPage) -> None:
        await queue.put(page)

    async def run_producers() -> list[SourceResult]:
        return await asyncio.gather(
            *(
                _run_producer(name, producer, emit)
                for name, producer in producers.items()
            )
        )

    async def consume_batches() -> DiscoveryWriteStats:
        writes = DiscoveryWriteStats()
        async for result in writer.run(queue, stop_token):
            writes.include(result)
            if on_committed_batch is not None:
                await on_committed_batch(result)
        return writes

    writer_task = asyncio.create_task(consume_batches())
    producers_task = asyncio.create_task(run_producers())

    done, _ = await asyncio.wait(
        {writer_task, producers_task}, return_when=asyncio.FIRST_COMPLETED
    )
    if writer_task in done:
        error = writer_task.exception()
        if error is not None:
            producers_task.cancel()
            await asyncio.gather(producers_task, return_exceptions=True)
            raise error

    source_results = await producers_task
    stop_task = asyncio.create_task(queue.put(stop_token))
    done, _ = await asyncio.wait(
        {writer_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
    )
    if writer_task in done:
        error = writer_task.exception()
        if error is not None:
            stop_task.cancel()
            await asyncio.gather(stop_task, return_exceptions=True)
            raise error
    await stop_task
    write_stats = await writer_task
    return DiscoveryResult(
        sources={result.name: result for result in source_results},
        writes=write_stats,
    )


class DiscoveryService:
    def __init__(
        self,
        database: Database,
        run_id: int,
        settings: Settings,
        *,
        sources: tuple[str, ...],
        experience_api_max_pages: int,
    ) -> None:
        self.database = database
        self.run_id = run_id
        self.settings = settings
        self.sources = sources
        self.experience_api_max_pages = experience_api_max_pages

    async def run(
        self,
        client: httpx.AsyncClient,
        *,
        on_committed_batch: CommittedBatchHandler | None = None,
    ) -> DiscoveryResult:
        producers: dict[str, Producer] = {}

        if "experience-api" in self.sources:
            discoverer = ExperienceApiDiscoverer(
                client,
                max_pages=self.experience_api_max_pages,
                interval_seconds=self.settings.experience_api_interval_seconds,
                jitter_seconds=self.settings.experience_api_jitter_seconds,
            )

            async def experience_api(emit: EmitPage) -> SourceResult:
                async def on_page(
                    _page_number: int, pages: list[DiscoveredPage]
                ) -> None:
                    for page in pages:
                        await emit(page)

                stats = await discoverer.discover(on_page)
                return SourceResult(
                    "experience-api",
                    stats.complete,
                    stats.stop_reason,
                    stats.upstream_exhausted,
                    stats.pages_seen,
                    stats.urls_seen,
                )

            producers["experience-api"] = experience_api

        if "sitemap" in self.sources:

            async def sitemap(emit: EmitPage) -> SourceResult:
                async def on_pages(pages: list[DiscoveredPage]) -> None:
                    for page in pages:
                        await emit(page)

                stats = await discover_sitemaps(
                    client,
                    roots=self.settings.sitemap_root_urls,
                    max_documents=self.settings.sitemap_max_documents,
                    max_urls=self.settings.sitemap_max_urls,
                    on_pages=on_pages,
                )
                return SourceResult(
                    "sitemap",
                    stats.complete,
                    stats.stop_reason,
                    stats.upstream_exhausted,
                    stats.documents_seen,
                    stats.accepted_pages,
                )

            producers["sitemap"] = sitemap

        writer = DiscoveryWriter(
            self.database,
            self.run_id,
            self.settings.discovery_db_batch_size,
        )
        return await run_discovery_pipeline(
            producers,
            writer,
            queue_maxsize=self.settings.discovery_queue_maxsize,
            on_committed_batch=on_committed_batch,
        )
