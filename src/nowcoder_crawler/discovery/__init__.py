"""Live URL discovery sources."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

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


def retry_after_seconds(value: str | None) -> float | None:
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
