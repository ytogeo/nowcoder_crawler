from __future__ import annotations

import asyncio

import pytest

from nowcoder_crawler.discovery import DiscoveredPage
from nowcoder_crawler.discovery.service import SourceResult, run_discovery_pipeline
from nowcoder_crawler.discovery.writer import CommittedBatchResult, DiscoveryWriter
from nowcoder_crawler.identity import build_identity


def _page(external_id: str) -> DiscoveredPage:
    return DiscoveredPage(
        identity=build_identity("discussion", external_id),
        source_type="test",
        source_key="test:source",
    )


class RecordingWriter(DiscoveryWriter):
    def __init__(self, *, fail: bool = False) -> None:
        super().__init__(None, 1, 1)  # type: ignore[arg-type]
        self.fail = fail

    async def _write(self, batch: list[DiscoveredPage]) -> CommittedBatchResult:
        if self.fail:
            raise RuntimeError("database unavailable")
        return CommittedBatchResult(len(batch), len(batch), 0, ())


@pytest.mark.asyncio
async def test_producers_run_concurrently() -> None:
    started: set[str] = set()
    both_started = asyncio.Event()

    def producer(name: str, external_id: str):
        async def run(emit):
            started.add(name)
            if len(started) == 2:
                both_started.set()
            await asyncio.wait_for(both_started.wait(), timeout=1)
            await emit(_page(external_id))
            return SourceResult(name, True, "done", True, 1, 1)

        return run

    result = await run_discovery_pipeline(
        {"api": producer("api", "1"), "sitemap": producer("sitemap", "2")},
        RecordingWriter(),
        queue_maxsize=1,
    )

    assert set(result.sources) == {"api", "sitemap"}
    assert result.complete is True
    assert result.writes.observations == 2


@pytest.mark.asyncio
async def test_one_source_failure_does_not_cancel_the_other() -> None:
    async def failing(_emit):
        raise RuntimeError("source failed")

    completed = False

    async def healthy(emit):
        nonlocal completed
        await emit(_page("1"))
        completed = True
        return SourceResult("healthy", True, "done", True, 1, 1)

    result = await run_discovery_pipeline(
        {"failing": failing, "healthy": healthy},
        RecordingWriter(),
        queue_maxsize=1,
    )

    assert completed is True
    assert result.complete is False
    assert result.sources["failing"].error == "source failed"
    assert result.writes.observations == 1


@pytest.mark.asyncio
async def test_writer_failure_cancels_producers() -> None:
    cancelled = asyncio.Event()

    async def producer(emit):
        try:
            while True:
                await emit(_page("1"))
                await asyncio.sleep(0)
        finally:
            cancelled.set()

    with pytest.raises(RuntimeError, match="database unavailable"):
        await run_discovery_pipeline(
            {"source": producer},
            RecordingWriter(fail=True),
            queue_maxsize=1,
        )

    assert cancelled.is_set()


@pytest.mark.asyncio
async def test_service_observes_each_committed_batch() -> None:
    observed: list[CommittedBatchResult] = []

    async def producer(emit):
        await emit(_page("1"))
        await emit(_page("2"))
        return SourceResult("source", True, "done", True, 1, 2)

    async def on_committed(result: CommittedBatchResult) -> None:
        observed.append(result)

    result = await run_discovery_pipeline(
        {"source": producer},
        RecordingWriter(),
        queue_maxsize=1,
        on_committed_batch=on_committed,
    )

    assert len(observed) == 2
    assert result.writes.batches_written == 2
