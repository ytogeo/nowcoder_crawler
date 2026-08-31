"""Live URL discovery sources."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from nowcoder_crawler.identity import PageIdentity


@dataclass(frozen=True)
class DiscoveredPage:
    identity: PageIdentity
    source_type: str
    source_key: str
    source_modified_at: datetime | None = None
    company_ids: tuple[int, ...] | None = None
    job_id: int | None = None
    job_level: int | None = None
    source_page: int | None = None
