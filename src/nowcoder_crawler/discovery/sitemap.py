from __future__ import annotations

import asyncio
import logging
import random
from collections import deque
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePosixPath
from urllib.parse import urljoin, urlsplit
from xml.etree import ElementTree

import httpx

from nowcoder_crawler.identity import ALLOWED_HOSTS, identity_from_url, strip_query_and_fragment

from . import DiscoveredPage, retry_after_seconds

LOGGER = logging.getLogger("nowcoder_crawler.discovery.sitemap")
MAX_REQUEST_ATTEMPTS = 3
EMIT_BATCH_SIZE = 200


@dataclass(frozen=True)
class SitemapStats:
    documents_seen: int
    urls_seen: int
    accepted_pages: int
    complete: bool
    stop_reason: str
    upstream_exhausted: bool


def _local_name(tag: str) -> str:
    """去除 XML 命名空间前缀，获取本地标签名。"""
    return tag.rsplit("}", 1)[-1]


def _parse_lastmod(value: str | None) -> datetime | None:
    """解析 sitemap 中的 lastmod 时间字符串为 datetime。"""
    if not value:
        return None
    normalized = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone().replace(tzinfo=None)
        return parsed
    except ValueError:
        try:
            return datetime.strptime(value.strip()[:10], "%Y-%m-%d")
        except ValueError:
            return None


def _xml_entries(content: bytes) -> list[tuple[str, datetime | None]]:
    """解析标准 XML 格式的 sitemap，提取 (url, lastmod) 列表。"""
    root = ElementTree.fromstring(content)
    entries: list[tuple[str, datetime | None]] = []
    for item in list(root):
        loc = None
        lastmod = None
        for child in list(item):
            name = _local_name(child.tag)
            if name == "loc":
                loc = (child.text or "").strip()
            elif name == "lastmod":
                lastmod = _parse_lastmod(child.text)
        if loc:
            entries.append((loc, lastmod))
    if entries:
        return entries
    for element in root.iter():
        if _local_name(element.tag) == "loc" and element.text:
            entries.append((element.text.strip(), None))
    return entries


def _text_entries(content: bytes) -> list[tuple[str, datetime | None]]:
    """解析纯文本格式的 sitemap（每行一个 URL）。"""
    text = content.decode("utf-8-sig", errors="replace")
    return [
        (line.strip(), None)
        for line in text.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]


def parse_document(
    content: bytes, content_type: str, url: str
) -> list[tuple[str, datetime | None]]:
    """根据 Content-Type 或文件后缀分发到 XML/文本解析器。"""
    if "xml" in content_type.lower() or urlsplit(url).path.lower().endswith(".xml"):
        return _xml_entries(content)
    return _text_entries(content)


def _is_allowed_sitemap(url: str) -> bool:
    """校验是否为牛客域名下的合法 sitemap URL。"""
    parsed = urlsplit(url)
    if (parsed.hostname or "").lower() not in ALLOWED_HOSTS:
        return False
    name = PurePosixPath(parsed.path).name.lower()
    return "sitemap" in parsed.path.lower() and (
        name.endswith(".xml") or name.endswith(".txt") or name.startswith("sitemap")
    )


async def _fetch_document(client: httpx.AsyncClient, url: str) -> httpx.Response:
    last_error: Exception | None = None
    for attempt_no in range(1, MAX_REQUEST_ATTEMPTS + 1):
        response: httpx.Response | None = None
        try:
            response = await client.get(url)
            retryable = response.status_code in {408, 429} or response.status_code >= 500
            if not retryable:
                response.raise_for_status()
                return response
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
            retry_after_seconds(response.headers.get("retry-after"))
            if response is not None and response.status_code == 429
            else None
        )
        delay = retry_after
        if delay is None:
            delay = (2.0 if attempt_no == 1 else 5.0) + random.uniform(0, 1)
        LOGGER.warning(
            "sitemap_retry url=%s attempt=%s delay_seconds=%.2f error=%s",
            url,
            attempt_no,
            delay,
            last_error,
        )
        await asyncio.sleep(delay)

    raise AssertionError("unreachable")


async def discover_sitemaps(
    client: httpx.AsyncClient,
    *,
    roots: Iterable[str],
    max_documents: int,
    max_urls: int,
    on_pages: Callable[[list[DiscoveredPage]], Awaitable[object]],
) -> SitemapStats:
    """Sitemap 增量发现器：使用广度优先搜索（BFS）递归下钻解析 sitemap 索引。"""
    queue = deque(strip_query_and_fragment(url) for url in roots)
    visited_documents: set[str] = set()
    seen_pages: set[tuple[str, str]] = set()
    documents_seen = 0
    urls_seen = 0
    accepted = 0
    url_limit_hit = False

    while queue and documents_seen < max_documents and urls_seen < max_urls:
        document_url = queue.popleft()
        if document_url in visited_documents or not _is_allowed_sitemap(document_url):
            continue
        visited_documents.add(document_url)
        response = await _fetch_document(client, document_url)
        documents_seen += 1
        entries = parse_document(
            response.content, response.headers.get("content-type", ""), document_url
        )
        batch: list[DiscoveredPage] = []
        for location, lastmod in entries:
            if urls_seen >= max_urls:
                url_limit_hit = True
                break
            absolute = strip_query_and_fragment(urljoin(document_url, location))
            urls_seen += 1
            # 1. 尝试匹配 feed / discussion 详情页
            identity = identity_from_url(absolute)
            if identity is not None:
                key = (identity.page_type, identity.external_id)
                if key not in seen_pages:
                    seen_pages.add(key)
                    batch.append(
                        DiscoveredPage(
                            identity=identity,
                            source_type="sitemap",
                            source_key=f"sitemap:{document_url}",
                            source_modified_at=lastmod,
                        )
                    )
                    if len(batch) >= EMIT_BATCH_SIZE:
                        accepted += len(batch)
                        await on_pages(batch)
                        batch = []
                continue
            # 2. 若是子 sitemap 则加入待遍历队列
            if _is_allowed_sitemap(absolute) and absolute not in visited_documents:
                queue.append(absolute)
        # 3. 批量回调上游进行 Upsert
        if batch:
            accepted += len(batch)
            await on_pages(batch)
    if url_limit_hit or (urls_seen >= max_urls and queue):
        return SitemapStats(
            documents_seen, urls_seen, accepted, False, "max_urls", False
        )
    if queue:
        return SitemapStats(
            documents_seen, urls_seen, accepted, False, "max_documents", False
        )
    return SitemapStats(
        documents_seen, urls_seen, accepted, True, "queue_exhausted", True
    )
