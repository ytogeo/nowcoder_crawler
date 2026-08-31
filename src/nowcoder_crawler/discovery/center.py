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


# 面经中心 API 查询参数封装
@dataclass(frozen=True)
class CenterQuery:
    company_ids: tuple[int, ...] = ()
    job_id: int = -1
    level: int = 1
    order: int = 3

    @property
    def source_key(self) -> str:
        """生成唯一标识该查询条件的 source_key，用于 page_sources 区分来源。"""
        company_part = ",".join(str(value) for value in sorted(self.company_ids))
        return f"center:{company_part}:{self.job_id}:{self.level}"


@dataclass(frozen=True)
class CenterStats:
    pages_seen: int
    urls_seen: int


class CenterStopTracker:
    """早期停止（Early Stop）跟踪器：连续 N 页无新发现或内容更新时提前终止翻页。"""

    def __init__(self, stale_pages: int) -> None:
        if stale_pages < 1:
            raise ValueError("stale_pages must be positive")
        self.limit = stale_pages
        self.consecutive_stale = 0

    def observe(self, new_or_updated: int) -> bool:
        """观察当前页是否有新增或更新页面，返回是否达到停止上限。"""
        self.consecutive_stale = 0 if new_or_updated else self.consecutive_stale + 1
        return self.consecutive_stale >= self.limit


def _from_millis(value: object) -> datetime | None:
    """将毫秒时间戳转换为 UTC datetime。"""
    if value is None:
        return None
    try:
        return datetime.utcfromtimestamp(int(value) / 1000)
    except (TypeError, ValueError, OSError):
        return None


def parse_record(record: dict, query: CenterQuery, page_number: int) -> DiscoveredPage | None:
    """解析单条 API 面经记录，提取页面身份与修改时间。"""
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
    """发送 POST 请求拉取面经中心指定页码的 JSON 数据。"""
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
    """面经中心发现器：从第 1 页开始分页请求，直到满足最大页数、尾页或触发 Early Stop。"""
    if max_pages < 1:
        raise ValueError("max_pages must be positive")
    tracker = CenterStopTracker(stale_pages)
    pages_seen = 0
    urls_seen = 0
    for page_number in range(1, max_pages + 1):
        # 1. 获取分页数据
        data = await fetch_center_page(client, query, page_number)
        pages_seen += 1
        records = data.get("records") or []
        # 2. 批量解析有效页面
        discovered = [
            parsed
            for record in records
            if (parsed := parse_record(record, query, page_number)) is not None
        ]
        urls_seen += len(discovered)
        # 3. 回调上游进行 Upsert，并获取本次活跃变更数量
        activity = await on_page(page_number, discovered)
        # 4. 判断是否提前终止（无记录或连续 N 页无新增/变更）
        if not records or tracker.observe(activity):
            break
        # 5. 到达接口声明的末页则退出
        total_pages = int(data.get("totalPage") or 0)
        if total_pages and page_number >= total_pages:
            break
    return CenterStats(pages_seen=pages_seen, urls_seen=urls_seen)
