from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime

import httpx

from nowcoder_crawler.identity import identity_from_center_record

from . import DiscoveredPage

CENTER_LIST_URL = "https://gw-c.nowcoder.com/api/sparta/job-experience/experience/job/list"
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


@dataclass(frozen=True)
class CenterQuery:
    company_ids: tuple[int, ...] = ()
    job_id: int = -1
    level: int = 1
    order: int = 3

    @property
    def source_key(self) -> str:
        company_part = ",".join(str(value) for value in sorted(self.company_ids))
        return f"center:{company_part}:{self.job_id}:{self.level}"


@dataclass(frozen=True)
class CenterStats:
    pages_seen: int
    urls_seen: int


class CenterStopTracker:
    def __init__(self, stale_pages: int) -> None:
        if stale_pages < 1:
            raise ValueError("stale_pages must be positive")
        self.limit = stale_pages
        self.consecutive_stale = 0

    def observe(self, new_or_updated: int) -> bool:
        self.consecutive_stale = 0 if new_or_updated else self.consecutive_stale + 1
        return self.consecutive_stale >= self.limit


def _from_millis(value: object) -> datetime | None:
    if value is None:
        return None
    try:
        return datetime.utcfromtimestamp(int(value) / 1000)
    except (TypeError, ValueError, OSError):
        return None


def parse_record(record: dict, query: CenterQuery, page_number: int) -> DiscoveredPage | None:
    identity = identity_from_center_record(record)
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
        source_type="center",
        source_key=query.source_key,
        source_modified_at=modified,
        company_ids=query.company_ids,
        job_id=query.job_id,
        job_level=query.level,
        source_page=page_number,
    )


async def fetch_center_page(
    client: httpx.AsyncClient, query: CenterQuery, page_number: int
) -> dict:
    payload = {
        "companyList": list(query.company_ids),
        "jobId": query.job_id,
        "level": query.level,
        "order": query.order,
        "page": page_number,
        "isNewJob": True,
    }
    response = await client.post(
        CENTER_LIST_URL,
        params={"_": int(time.time() * 1000)},
        headers=DEFAULT_HEADERS,
        json=payload,
    )
    response.raise_for_status()
    result = response.json()
    if not result.get("success", False):
        raise RuntimeError(
            f"center API rejected request: code={result.get('code')} msg={result.get('msg')!r}"
        )
    return result.get("data") or {}


async def discover_center(
    client: httpx.AsyncClient,
    *,
    query: CenterQuery,
    max_pages: int,
    stale_pages: int,
    on_page: Callable[[int, list[DiscoveredPage]], Awaitable[int]],
) -> CenterStats:
    if max_pages < 1:
        raise ValueError("max_pages must be positive")
    tracker = CenterStopTracker(stale_pages)
    pages_seen = 0
    urls_seen = 0
    for page_number in range(1, max_pages + 1):
        data = await fetch_center_page(client, query, page_number)
        pages_seen += 1
        records = data.get("records") or []
        discovered = [
            parsed
            for record in records
            if (parsed := parse_record(record, query, page_number)) is not None
        ]
        urls_seen += len(discovered)
        activity = await on_page(page_number, discovered)
        if not records or tracker.observe(activity):
            break
        total_pages = int(data.get("totalPage") or 0)
        if total_pages and page_number >= total_pages:
            break
    return CenterStats(pages_seen=pages_seen, urls_seen=urls_seen)
