from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import (
    JSON,
    BigInteger,
    DateTime,
    ForeignKey,
    Integer,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utc_now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


class Base(DeclarativeBase):
    pass


class CrawlRun(Base):
    __tablename__ = "crawl_runs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="running")
    sources_json: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=utc_now)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    center_pages_seen: Mapped[int] = mapped_column(Integer, default=0)
    sitemap_docs_seen: Mapped[int] = mapped_column(Integer, default=0)
    urls_seen: Mapped[int] = mapped_column(Integer, default=0)
    pages_inserted: Mapped[int] = mapped_column(Integer, default=0)
    pages_updated: Mapped[int] = mapped_column(Integer, default=0)
    messages_published: Mapped[int] = mapped_column(Integer, default=0)
    error_message: Mapped[str | None] = mapped_column(Text)


class Page(Base):
    __tablename__ = "pages"
    __table_args__ = (
        UniqueConstraint("page_type", "external_id", name="uq_pages_identity"),
        UniqueConstraint("canonical_url", name="uq_pages_canonical_url"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    page_type: Mapped[str] = mapped_column(String(16), nullable=False)
    external_id: Mapped[str] = mapped_column(String(64), nullable=False)
    canonical_url: Mapped[str] = mapped_column(String(512), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=utc_now)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=utc_now)
    source_modified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    last_fetched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    gzip_path: Mapped[str | None] = mapped_column(String(1024))
    body_sha256: Mapped[str | None] = mapped_column(String(64))
    http_status: Mapped[int | None] = mapped_column(SmallInteger)
    response_content_type: Mapped[str | None] = mapped_column(String(255))
    response_bytes: Mapped[int | None] = mapped_column(BigInteger)
    final_url: Mapped[str | None] = mapped_column(String(512))
    last_error_type: Mapped[str | None] = mapped_column(String(16))
    last_error_message: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False), default=utc_now, onupdate=utc_now
    )


class PageSource(Base):
    __tablename__ = "page_sources"
    __table_args__ = (
        UniqueConstraint("page_id", "source_type", "source_key", name="uq_page_source"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    page_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("pages.id", ondelete="CASCADE"), nullable=False
    )
    source_type: Mapped[str] = mapped_column(String(32), nullable=False)
    source_key: Mapped[str] = mapped_column(String(255), nullable=False)
    company_ids_json: Mapped[list[int] | None] = mapped_column(JSON)
    job_id: Mapped[int | None] = mapped_column(Integer)
    job_level: Mapped[int | None] = mapped_column(SmallInteger)
    first_seen_page: Mapped[int | None] = mapped_column(Integer)
    last_seen_page: Mapped[int | None] = mapped_column(Integer)
    source_modified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=utc_now)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=utc_now)
    first_crawl_run_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("crawl_runs.id"), nullable=False
    )
    last_crawl_run_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("crawl_runs.id"), nullable=False
    )


class FetchAttempt(Base):
    __tablename__ = "fetch_attempts"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    page_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("pages.id", ondelete="CASCADE"), nullable=False
    )
    worker_id: Mapped[str] = mapped_column(String(64), nullable=False)
    attempt_no: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)
    finished_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)
    outcome: Mapped[str] = mapped_column(String(16), nullable=False)
    http_status: Mapped[int | None] = mapped_column(SmallInteger)
    final_url: Mapped[str | None] = mapped_column(String(512))
    response_bytes: Mapped[int | None] = mapped_column(BigInteger)
    elapsed_ms: Mapped[int | None] = mapped_column(Integer)
    retry_after: Mapped[str | None] = mapped_column(String(128))
    error_type: Mapped[str | None] = mapped_column(String(16))
    error_message: Mapped[str | None] = mapped_column(Text)
