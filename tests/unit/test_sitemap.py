from __future__ import annotations

import httpx
import pytest

from nowcoder_crawler.discovery.sitemap import discover_sitemaps


@pytest.mark.asyncio
async def test_sitemap_recurses_and_emits_supported_pages() -> None:
    root = b"""<?xml version="1.0"?>
    <sitemapindex><sitemap><loc>https://www.nowcoder.com/sitemap-posts.xml</loc></sitemap></sitemapindex>
    """
    child = b"""<?xml version="1.0"?>
    <urlset>
      <url><loc>https://www.nowcoder.com/discuss/123?from=sitemap</loc></url>
      <url><loc>https://www.nowcoder.com/users/456</loc></url>
    </urlset>
    """

    def handler(request: httpx.Request) -> httpx.Response:
        content = root if request.url.path == "/sitemap.xml" else child
        return httpx.Response(
            200, request=request, content=content, headers={"content-type": "application/xml"}
        )

    emitted = []

    async def on_pages(pages):
        emitted.extend(pages)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        stats = await discover_sitemaps(
            client,
            roots=("https://www.nowcoder.com/sitemap.xml",),
            max_documents=10,
            max_urls=100,
            on_pages=on_pages,
        )

    assert stats.complete is True
    assert stats.documents_seen == 2
    assert [page.identity.external_id for page in emitted] == ["123"]


@pytest.mark.asyncio
async def test_sitemap_limit_marks_source_incomplete() -> None:
    root = b"""<?xml version="1.0"?>
    <sitemapindex>
      <sitemap><loc>https://www.nowcoder.com/sitemap-a.xml</loc></sitemap>
      <sitemap><loc>https://www.nowcoder.com/sitemap-b.xml</loc></sitemap>
    </sitemapindex>
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, request=request, content=root, headers={"content-type": "application/xml"}
        )

    async def on_pages(_pages):
        return None

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        stats = await discover_sitemaps(
            client,
            roots=("https://www.nowcoder.com/sitemap.xml",),
            max_documents=1,
            max_urls=100,
            on_pages=on_pages,
        )

    assert stats.complete is False
    assert stats.stop_reason == "max_documents"
    assert stats.upstream_exhausted is False
