"""Unit tests for discovery batch aggregation and flushing."""

from __future__ import annotations

import asyncio
from datetime import datetime

import pytest

from nowcoder_crawler.discovery import DiscoveredPage
from nowcoder_crawler.discovery.writer import (
    CommittedBatchResult,
    DiscoveryWriter,
    aggregate_discovered_batch,
)
from nowcoder_crawler.identity import build_identity


def _page(
    external_id: str,
    *,
    source_type: str = "center",
    source_key: str = "center::-1:1",
    source_page: int | None = 1,
    modified: datetime | None = None,
) -> DiscoveredPage:
    return DiscoveredPage(
        identity=build_identity("discussion", external_id),
        source_type=source_type,
        source_key=source_key,
        source_modified_at=modified,
        source_page=source_page,
    )


def test_batch_aggregation_deduplicates_pages_and_preserves_lineage() -> None:
    older = datetime(2026, 1, 1)
    newer = datetime(2026, 1, 2)
    batch = aggregate_discovered_batch(
        [
            _page("1", source_page=1, modified=older),
            _page("1", source_page=2, modified=newer),
            _page(
                "1",
                source_type="sitemap",
                source_key="sitemap:https://www.nowcoder.com/sitemap-1.xml",
                source_page=None,
                modified=older,
            ),
        ]
    )

    assert len(batch.pages_by_identity) == 1
    assert len(batch.sources_by_key) == 2
    page = batch.pages_by_identity[("discussion", "1")]
    assert page.source_modified_at == newer
    center = batch.sources_by_key[
        (("discussion", "1"), "center", "center::-1:1")
    ]
    assert center.first_seen_page == 1
    assert center.last_seen_page == 2
    assert center.source_modified_at == newer


@pytest.mark.asyncio
async def test_writer_flushes_full_and_final_partial_batches() -> None:
    class RecordingWriter(DiscoveryWriter):
        def __init__(self) -> None:
            super().__init__(None, 1, 2)  # type: ignore[arg-type]
            self.batch_sizes: list[int] = []

        async def _write(self, batch: list[DiscoveredPage]) -> CommittedBatchResult:
            self.batch_sizes.append(len(batch))
            return CommittedBatchResult(len(batch), len(batch), 0, ())

    writer = RecordingWriter()
    queue: asyncio.Queue[DiscoveredPage | object] = asyncio.Queue()
    stop_token = object()
    for external_id in range(5):
        await queue.put(_page(str(external_id)))
    await queue.put(stop_token)

    results = [result async for result in writer.run(queue, stop_token)]

    assert writer.batch_sizes == [2, 2, 1]
    assert [result.observations for result in results] == [2, 2, 1]


@pytest.mark.asyncio
async def test_writer_runs_sync_transaction_in_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[object, int, list[DiscoveredPage]]] = []
    expected = CommittedBatchResult(1, 1, 0, (7,))

    def fake_write(database: object, run_id: int, batch: list[DiscoveredPage]):
        calls.append((database, run_id, batch))
        return expected

    async def fake_to_thread(function, *args):
        return function(*args)

    monkeypatch.setattr(
        "nowcoder_crawler.discovery.writer.write_discovered_batch", fake_write
    )
    monkeypatch.setattr("nowcoder_crawler.discovery.writer.asyncio.to_thread", fake_to_thread)
    database = object()
    writer = DiscoveryWriter(database, 9, 2)  # type: ignore[arg-type]
    batch = [_page("1")]

    result = await writer._write(batch)

    assert result == expected
    assert calls == [(database, 9, batch)]
