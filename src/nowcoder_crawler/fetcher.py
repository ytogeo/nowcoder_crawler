from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

import httpx

from .config import Settings
from .identity import PageIdentity, identity_from_url
from .models import utc_now

BLOCKED_MARKERS = (
    "访问过于频繁",
    "请登录后继续访问",
    "verifycenter",
)


@dataclass(frozen=True)
class AttemptObservation:
    attempt_no: int
    started_at: datetime
    finished_at: datetime
    outcome: str
    http_status: int | None
    final_url: str | None
    response_bytes: int | None
    elapsed_ms: int
    retry_after: str | None
    error_type: str | None
    error_message: str | None


@dataclass(frozen=True)
class FetchOutcome:
    success: bool
    body: bytes | None
    http_status: int | None
    final_url: str | None
    content_type: str | None
    error_type: str | None
    error_message: str | None
    exit_worker: bool = False


@dataclass
class WorkerFetchState:
    consecutive_429: int = 0


class RequestPacer:
    def __init__(self, base_delay: float, jitter: float) -> None:
        self.base_delay = base_delay
        self.jitter = jitter
        self._last_started: float | None = None

    async def wait(self) -> None:
        now = time.monotonic()
        if self._last_started is not None:
            target = self.base_delay + random.uniform(0, self.jitter)
            remaining = target - (now - self._last_started)
            if remaining > 0:
                await asyncio.sleep(remaining)
        self._last_started = time.monotonic()


def classify_status(status: int) -> str | None:
    if status == 200:
        return None
    if status in {401, 403}:
        return "blocked"
    if status in {408, 429} or 500 <= status <= 599:
        return "retryable"
    if 400 <= status <= 499:
        return "permanent"
    return "retryable"


def parse_retry_after(value: str | None, settings: Settings) -> float:
    if not value:
        return min(settings.retry_after_default_seconds, settings.retry_after_max_seconds)
    try:
        seconds = max(0.0, float(value.strip()))
    except ValueError:
        try:
            when = parsedate_to_datetime(value)
            if when.tzinfo is None:
                when = when.replace(tzinfo=UTC)
            seconds = max(0.0, (when - datetime.now(UTC)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            seconds = settings.retry_after_default_seconds
    return min(seconds, settings.retry_after_max_seconds)


def validate_response(response: httpx.Response, identity: PageIdentity) -> tuple[str, str] | None:
    status_error = classify_status(response.status_code)
    if status_error is not None:
        return status_error, f"HTTP {response.status_code}"
    content_type = response.headers.get("content-type", "").lower()
    if "html" not in content_type:
        return "permanent", f"unexpected content type: {content_type!r}"
    final_identity = identity_from_url(str(response.url))
    if final_identity != identity:
        return "permanent", f"final URL does not match expected page: {response.url}"
    if len(response.content) < 512:
        return "retryable", f"HTML body is suspiciously small: {len(response.content)} bytes"
    text = response.text.lower()
    if any(marker.lower() in text for marker in BLOCKED_MARKERS):
        return "blocked", "login/captcha/risk-control template detected"
    if identity.external_id.lower() not in text:
        return "permanent", "expected page identity is absent from HTML"
    return None


async def fetch_with_retry(
    client: httpx.AsyncClient,
    *,
    identity: PageIdentity,
    settings: Settings,
    pacer: RequestPacer,
    state: WorkerFetchState,
    on_attempt: Callable[[AttemptObservation], Awaitable[None]],
) -> FetchOutcome:
    for attempt_no in range(1, settings.fetch_max_attempts + 1):
        await pacer.wait()
        started_at = utc_now()
        started_clock = time.monotonic()
        response: httpx.Response | None = None
        error_type: str | None = None
        error_message: str | None = None
        retry_after_header: str | None = None
        exit_worker = False
        try:
            response = await client.get(identity.canonical_url)
            if response.status_code == 429:
                state.consecutive_429 += 1
                retry_after_header = response.headers.get("retry-after")
                if state.consecutive_429 >= 3:
                    error_type = "blocked"
                    error_message = "worker observed three consecutive HTTP 429 responses"
                    exit_worker = True
                else:
                    error_type = "retryable"
                    error_message = "HTTP 429"
            else:
                state.consecutive_429 = 0
                validation_error = validate_response(response, identity)
                if validation_error is not None:
                    error_type, error_message = validation_error
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            error_type = "retryable"
            error_message = f"{type(exc).__name__}: {exc}"

        finished_at = utc_now()
        elapsed_ms = int((time.monotonic() - started_clock) * 1000)
        is_success = response is not None and error_type is None
        is_retrying = error_type == "retryable" and attempt_no < settings.fetch_max_attempts
        outcome_name = (
            "success"
            if is_success
            else "retry"
            if is_retrying
            else "blocked"
            if error_type == "blocked"
            else "failed"
        )
        await on_attempt(
            AttemptObservation(
                attempt_no=attempt_no,
                started_at=started_at,
                finished_at=finished_at,
                outcome=outcome_name,
                http_status=None if response is None else response.status_code,
                final_url=None if response is None else str(response.url),
                response_bytes=None if response is None else len(response.content),
                elapsed_ms=elapsed_ms,
                retry_after=retry_after_header,
                error_type=error_type,
                error_message=error_message,
            )
        )
        if is_success:
            assert response is not None
            return FetchOutcome(
                success=True,
                body=response.content,
                http_status=response.status_code,
                final_url=str(response.url),
                content_type=response.headers.get("content-type"),
                error_type=None,
                error_message=None,
            )
        if exit_worker or error_type in {"permanent", "blocked"}:
            return FetchOutcome(
                success=False,
                body=None,
                http_status=None if response is None else response.status_code,
                final_url=None if response is None else str(response.url),
                content_type=None if response is None else response.headers.get("content-type"),
                error_type=error_type,
                error_message=error_message,
                exit_worker=exit_worker,
            )
        if is_retrying:
            if response is not None and response.status_code == 429:
                await asyncio.sleep(parse_retry_after(retry_after_header, settings))
            else:
                delay = 2.0 if attempt_no == 1 else 5.0
                await asyncio.sleep(delay + random.uniform(0, settings.fetch_jitter_seconds))
            continue
        return FetchOutcome(
            success=False,
            body=None,
            http_status=None if response is None else response.status_code,
            final_url=None if response is None else str(response.url),
            content_type=None if response is None else response.headers.get("content-type"),
            error_type=error_type or "retryable",
            error_message=error_message or "request failed",
        )
    raise AssertionError("unreachable")
