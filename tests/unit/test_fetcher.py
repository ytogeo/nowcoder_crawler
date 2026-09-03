from pathlib import Path

import httpx
import pytest

from nowcoder_crawler.config import Settings
from nowcoder_crawler.fetcher import (
    RequestPacer,
    WorkerFetchState,
    classify_status,
    fetch_with_retry,
    validate_response,
)
from nowcoder_crawler.identity import build_identity


def settings(tmp_path: Path) -> Settings:
    return Settings(
        mysql_dsn="sqlite://",
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
        experience_api_max_pages=20,
        experience_api_interval_seconds=0,
        experience_api_jitter_seconds=0,
        discovery_queue_maxsize=1000,
        discovery_db_batch_size=200,
        sitemap_max_documents=20,
        sitemap_max_urls=100,
        sitemap_root_urls=("https://www.nowcoder.com/sitemap.xml",),
    )


def test_error_classification() -> None:
    assert classify_status(200) is None
    assert classify_status(403) == "blocked"
    assert classify_status(404) == "permanent"
    assert classify_status(408) == "retryable"
    assert classify_status(429) == "retryable"
    assert classify_status(503) == "retryable"


def test_normal_page_script_mentioning_captcha_is_not_blocked() -> None:
    identity = build_identity("feed", "a" * 32)
    request = httpx.Request("GET", identity.canonical_url)
    body = (
        f"<html><title>公开面经</title><script>const captcha = true;</script>"
        f"<main>{identity.external_id}</main></html>"
    ).encode() + b"x" * 600
    response = httpx.Response(
        200,
        request=request,
        content=body,
        headers={"content-type": "text/html; charset=utf-8"},
    )

    assert validate_response(response, identity) is None


def test_explicit_risk_control_template_is_blocked() -> None:
    identity = build_identity("discussion", "123456")
    request = httpx.Request("GET", identity.canonical_url)
    body = f"<html><title>安全验证</title>{identity.external_id}</html>".encode() + b"x" * 600
    response = httpx.Response(
        200,
        request=request,
        content=body,
        headers={"content-type": "text/html"},
    )

    assert validate_response(response, identity) == (
        "blocked",
        "login/captcha/risk-control template detected",
    )


@pytest.mark.asyncio
async def test_retries_twice_then_succeeds(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    identity = build_identity("feed", "a" * 32)
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls < 3:
            return httpx.Response(503, request=request, text="temporary")
        body = f"<html>{identity.external_id}</html>".encode() + b"x" * 600
        return httpx.Response(
            200, request=request, content=body, headers={"content-type": "text/html"}
        )

    async def no_sleep(_: float) -> None:
        return None

    monkeypatch.setattr("nowcoder_crawler.fetcher.asyncio.sleep", no_sleep)
    observations = []

    async def on_attempt(value) -> None:
        observations.append(value)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await fetch_with_retry(
            client,
            identity=identity,
            settings=settings(tmp_path),
            pacer=RequestPacer(0, 0),
            state=WorkerFetchState(),
            on_attempt=on_attempt,
        )
    assert result.success is True
    assert calls == 3
    assert [item.outcome for item in observations] == ["retry", "retry", "success"]


@pytest.mark.asyncio
async def test_permanent_error_is_not_retried(tmp_path: Path) -> None:
    identity = build_identity("discussion", "123456")
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(404, request=request)

    observations = []

    async def on_attempt(value) -> None:
        observations.append(value)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await fetch_with_retry(
            client,
            identity=identity,
            settings=settings(tmp_path),
            pacer=RequestPacer(0, 0),
            state=WorkerFetchState(),
            on_attempt=on_attempt,
        )
    assert result.success is False
    assert result.error_type == "permanent"
    assert calls == 1
    assert len(observations) == 1
