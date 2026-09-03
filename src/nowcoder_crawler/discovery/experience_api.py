from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

import httpx

from nowcoder_crawler.identity import identity_from_experience_record

from . import DiscoveredPage

EXPERIENCE_LIST_URL = (
    "https://gw-c.nowcoder.com/api/sparta/job-experience/experience/job/list"
)
DEFAULT_PAGE_SIZE = 20
MAX_REQUEST_ATTEMPTS = 3
DEFAULT_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "Referer": "https://www.nowcoder.com/",
    "X-Requested-With": "XMLHttpRequest",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
}

LOGGER = logging.getLogger("nowcoder_crawler.discovery.experience_api")
PERSISTED_SOURCE_TYPE = "center"
PERSISTED_SOURCE_KEY = "center::-1:1"


@dataclass(frozen=True)
class ExperienceApiStats:
    pages_seen: int
    urls_seen: int
    complete: bool
    stop_reason: str
    upstream_exhausted: bool
    reported_total: int | None
    reported_total_pages: int | None


class ExperienceApiPacer:
    def __init__(self, interval_seconds: float, jitter_seconds: float) -> None:
        if interval_seconds < 0 or jitter_seconds < 0:
            raise ValueError("experience API pacing values must be non-negative")
        self.interval_seconds = interval_seconds
        self.jitter_seconds = jitter_seconds
        self._last_started: float | None = None

    async def wait(self) -> None:
        now = time.monotonic()
        if self._last_started is not None:
            target = self.interval_seconds + random.uniform(0, self.jitter_seconds)
            remaining = target - (now - self._last_started)
            if remaining > 0:
                await asyncio.sleep(remaining)
        self._last_started = time.monotonic()


def _from_millis(value: object) -> datetime | None:
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(int(value) / 1000, tz=UTC).replace(tzinfo=None)
    except (TypeError, ValueError, OSError):
        return None


def _optional_int(value: object) -> int | None:
    try:
        return None if value is None else int(value)
    except (TypeError, ValueError):
        return None


def _retry_after_seconds(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value.strip()))
    except ValueError:
        try:
            when = parsedate_to_datetime(value)
            if when.tzinfo is None:
                when = when.replace(tzinfo=UTC)
            return max(0.0, (when - datetime.now(UTC)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return None


def parse_record(record: dict, page_number: int) -> DiscoveredPage | None:
    identity = identity_from_experience_record(record)
    if identity is None:
        return None
    content = record.get("momentData") or record.get("contentData") or {}
    modified = _from_millis(
        content.get("editTime")
        or content.get("showTime")
        or content.get("createdAt")
        or content.get("createTime")
    )
    return DiscoveredPage(
        identity=identity,
        source_type=PERSISTED_SOURCE_TYPE,
        source_key=PERSISTED_SOURCE_KEY,
        source_modified_at=modified,
        company_ids=(),
        job_id=-1,
        job_level=1,
        source_page=page_number,
    )


async def fetch_experience_api_page(
    client: httpx.AsyncClient,
    page_number: int,
    *,
    pacer: ExperienceApiPacer,
    retry_jitter_seconds: float,
) -> dict:
    payload = {
        "companyList": [],
        "jobId": -1,
        "level": 1,
        "order": 3,
        "page": page_number,
        "isNewJob": True,
    }

    last_error: Exception | None = None
    for attempt_no in range(1, MAX_REQUEST_ATTEMPTS + 1):
        await pacer.wait()
        response: httpx.Response | None = None
        try:
            response = await client.post(
                EXPERIENCE_LIST_URL,
                params={"_": int(time.time() * 1000)},
                headers=DEFAULT_HEADERS,
                json=payload,
            )
            retryable = response.status_code in {408, 429} or response.status_code >= 500
            if not retryable:
                response.raise_for_status()
                result = response.json()
                if not isinstance(result, dict) or not result.get("success", False):
                    code = result.get("code") if isinstance(result, dict) else None
                    msg = result.get("msg") if isinstance(result, dict) else None
                    raise RuntimeError(
                        f"experience API rejected request: code={code} msg={msg!r}"
                    )
                data = result.get("data") or {}
                if not isinstance(data, dict):
                    raise RuntimeError("experience API returned non-object data")
                return data
            response.raise_for_status()
        except httpx.TransportError as exc:
            last_error = exc
        except httpx.HTTPStatusError as exc:
            last_error = exc
            if response is None or not (
                response.status_code in {408, 429} or response.status_code >= 500
            ):
                raise

        assert last_error is not None
        if attempt_no == MAX_REQUEST_ATTEMPTS:
            raise last_error

        retry_after = (
            _retry_after_seconds(response.headers.get("retry-after"))
            if response is not None and response.status_code == 429
            else None
        )
        delay = retry_after
        if delay is None:
            delay = (2.0 if attempt_no == 1 else 5.0) + random.uniform(
                0, retry_jitter_seconds
            )
        LOGGER.warning(
            "experience_api_retry page=%s attempt=%s delay_seconds=%.2f error=%s",
            page_number,
            attempt_no,
            delay,
            last_error,
        )
        await asyncio.sleep(delay)

    raise AssertionError("unreachable")


class ExperienceApiDiscoverer:
    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        max_pages: int,
        interval_seconds: float,
        jitter_seconds: float,
    ) -> None:
        if max_pages < 1:
            raise ValueError("max_pages must be positive")
        self.client = client
        self.max_pages = max_pages
        self.jitter_seconds = jitter_seconds
        self.pacer = ExperienceApiPacer(interval_seconds, jitter_seconds)

    async def discover(
        self,
        on_page: Callable[[int, list[DiscoveredPage]], Awaitable[object]],
    ) -> ExperienceApiStats:
        pages_seen = 0
        urls_seen = 0
        reported_total: int | None = None
        reported_total_pages: int | None = None

        for page_number in range(1, self.max_pages + 1):
            data = await fetch_experience_api_page(
                self.client,
                page_number,
                pacer=self.pacer,
                retry_jitter_seconds=self.jitter_seconds,
            )
            pages_seen += 1
            reported_total = _optional_int(data.get("total"))
            reported_total_pages = _optional_int(data.get("totalPage"))
            records = data.get("records") or []
            if not isinstance(records, list):
                raise RuntimeError("experience API records must be a list")
            if not records:
                return ExperienceApiStats(
                    pages_seen,
                    urls_seen,
                    True,
                    "empty_page",
                    True,
                    reported_total,
                    reported_total_pages,
                )

            discovered = [
                parsed
                for record in records
                if isinstance(record, dict)
                and (parsed := parse_record(record, page_number)) is not None
            ]
            urls_seen += len(discovered)
            if discovered:
                await on_page(page_number, discovered)

            declared_size = _optional_int(data.get("size")) or DEFAULT_PAGE_SIZE
            if len(records) < declared_size:
                return ExperienceApiStats(
                    pages_seen,
                    urls_seen,
                    True,
                    "short_page",
                    True,
                    reported_total,
                    reported_total_pages,
                )

        return ExperienceApiStats(
            pages_seen,
            urls_seen,
            True,
            "configured_page_limit",
            False,
            reported_total,
            reported_total_pages,
        )
