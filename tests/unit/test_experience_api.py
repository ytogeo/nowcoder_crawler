from __future__ import annotations

import json

import httpx
import pytest

from nowcoder_crawler.discovery.experience_api import (
    ExperienceApiDiscoverer,
)


def _response(
    request: httpx.Request,
    records: list[dict],
    *,
    size: int = 20,
    total_pages: int = 100,
) -> httpx.Response:
    return httpx.Response(
        200,
        request=request,
        json={
            "success": True,
            "code": 0,
            "msg": "OK",
            "data": {
                "records": records,
                "size": size,
                "total": 2000,
                "totalPage": total_pages,
            },
        },
    )


def _unsupported_records(page_number: int) -> list[dict]:
    return [
        {"contentType": 999, "contentId": page_number * 100 + offset}
        for offset in range(20)
    ]


@pytest.mark.asyncio
async def test_processes_configured_twenty_page_window() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return _response(request, _unsupported_records(calls))

    emitted: list = []

    async def on_page(_page_number: int, pages: list) -> None:
        emitted.extend(pages)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        stats = await ExperienceApiDiscoverer(
            client,
            max_pages=20,
            interval_seconds=0,
            jitter_seconds=0,
        ).discover(on_page)

    assert calls == 20
    assert emitted == []
    assert stats.pages_seen == 20
    assert stats.complete is True
    assert stats.stop_reason == "configured_page_limit"
    assert stats.upstream_exhausted is False


@pytest.mark.asyncio
async def test_empty_page_stops_before_emitting() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return _response(request, [])

    emitted: list = []

    async def on_page(_page_number: int, pages: list) -> None:
        emitted.extend(pages)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        stats = await ExperienceApiDiscoverer(
            client,
            max_pages=20,
            interval_seconds=0,
            jitter_seconds=0,
        ).discover(on_page)

    assert emitted == []
    assert stats.pages_seen == 1
    assert stats.stop_reason == "empty_page"
    assert stats.upstream_exhausted is True


@pytest.mark.asyncio
async def test_short_page_is_emitted_then_stops() -> None:
    record = {"contentType": 250, "contentId": 123456}

    def handler(request: httpx.Request) -> httpx.Response:
        return _response(request, [record])

    emitted: list = []

    async def on_page(_page_number: int, pages: list) -> None:
        emitted.extend(pages)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        stats = await ExperienceApiDiscoverer(
            client,
            max_pages=20,
            interval_seconds=0,
            jitter_seconds=0,
        ).discover(on_page)

    assert [item.identity.external_id for item in emitted] == ["123456"]
    assert stats.urls_seen == 1
    assert stats.stop_reason == "short_page"
    assert stats.upstream_exhausted is True


@pytest.mark.asyncio
async def test_total_page_does_not_stop_pagination() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        records = _unsupported_records(calls) if calls == 1 else []
        return _response(request, records, total_pages=1)

    async def on_page(_page_number: int, _pages: list) -> None:
        return None

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        stats = await ExperienceApiDiscoverer(
            client,
            max_pages=20,
            interval_seconds=0,
            jitter_seconds=0,
        ).discover(on_page)

    assert calls == 2
    assert stats.stop_reason == "empty_page"


@pytest.mark.asyncio
async def test_retryable_errors_are_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls < 3:
            return httpx.Response(503, request=request)
        return _response(request, [])

    async def no_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr("nowcoder_crawler.discovery.experience_api.asyncio.sleep", no_sleep)

    async def on_page(_page_number: int, _pages: list) -> None:
        return None

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        stats = await ExperienceApiDiscoverer(
            client,
            max_pages=20,
            interval_seconds=0,
            jitter_seconds=0,
        ).discover(on_page)

    assert calls == 3
    assert stats.stop_reason == "empty_page"


@pytest.mark.asyncio
async def test_permanent_http_error_is_not_retried() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(404, request=request)

    async def on_page(_page_number: int, _pages: list) -> None:
        return None

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await ExperienceApiDiscoverer(
                client,
                max_pages=20,
                interval_seconds=0,
                jitter_seconds=0,
            ).discover(on_page)

    assert calls == 1


@pytest.mark.asyncio
async def test_request_uses_fixed_broad_query() -> None:
    payloads: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        payloads.append(json.loads(request.content))
        return _response(request, [])

    async def on_page(_page_number: int, _pages: list) -> None:
        return None

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await ExperienceApiDiscoverer(
            client,
            max_pages=20,
            interval_seconds=0,
            jitter_seconds=0,
        ).discover(on_page)

    assert payloads == [{
        "companyList": [],
        "jobId": -1,
        "level": 1,
        "order": 3,
        "page": 1,
        "isNewJob": True,
    }]
