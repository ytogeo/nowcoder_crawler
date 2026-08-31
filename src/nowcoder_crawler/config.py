from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"missing required environment variable: {name}")
    return value


def _int(name: str, default: int) -> int:
    value = int(os.getenv(name, str(default)))
    if value < 0:
        raise ValueError(f"{name} must be non-negative")
    return value


def _float(name: str, default: float) -> float:
    value = float(os.getenv(name, str(default)))
    if value < 0:
        raise ValueError(f"{name} must be non-negative")
    return value


def _bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    mysql_dsn: str
    rabbitmq_url: str
    raw_data_dir: Path
    fetch_queue: str
    worker_prefetch: int
    fetch_connect_timeout_seconds: float
    fetch_timeout_seconds: float
    fetch_max_attempts: int
    fetch_base_delay_seconds: float
    fetch_jitter_seconds: float
    retry_after_default_seconds: float
    retry_after_max_seconds: float
    center_max_pages: int
    center_stale_pages: int
    sitemap_max_documents: int
    sitemap_max_urls: int
    sitemap_root_urls: tuple[str, ...]
    failpoint_after_success_commit: bool = False

    @classmethod
    def from_env(cls) -> Settings:
        roots = tuple(
            item.strip()
            for item in os.getenv(
                "SITEMAP_ROOT_URLS", "https://www.nowcoder.com/sitemap.xml"
            ).split(",")
            if item.strip()
        )
        settings = cls(
            mysql_dsn=_required("MYSQL_DSN"),
            rabbitmq_url=_required("RABBITMQ_URL"),
            raw_data_dir=Path(os.getenv("RAW_DATA_DIR", "./data/raw")).resolve(),
            fetch_queue=os.getenv("FETCH_QUEUE", "fetch.ready"),
            worker_prefetch=_int("WORKER_PREFETCH", 2),
            fetch_connect_timeout_seconds=_float("FETCH_CONNECT_TIMEOUT_SECONDS", 10),
            fetch_timeout_seconds=_float("FETCH_TIMEOUT_SECONDS", 30),
            fetch_max_attempts=_int("FETCH_MAX_ATTEMPTS", 3),
            fetch_base_delay_seconds=_float("FETCH_BASE_DELAY_SECONDS", 5),
            fetch_jitter_seconds=_float("FETCH_JITTER_SECONDS", 2),
            retry_after_default_seconds=_float("FETCH_RETRY_AFTER_DEFAULT_SECONDS", 60),
            retry_after_max_seconds=_float("FETCH_RETRY_AFTER_MAX_SECONDS", 300),
            center_max_pages=_int("CENTER_MAX_PAGES", 10),
            center_stale_pages=_int("CENTER_STALE_PAGES", 3),
            sitemap_max_documents=_int("SITEMAP_MAX_DOCUMENTS", 20),
            sitemap_max_urls=_int("SITEMAP_MAX_URLS", 50_000),
            sitemap_root_urls=roots,
            failpoint_after_success_commit=_bool("FAILPOINT_AFTER_SUCCESS_COMMIT"),
        )
        if settings.worker_prefetch < 1 or settings.fetch_max_attempts < 1:
            raise ValueError("WORKER_PREFETCH and FETCH_MAX_ATTEMPTS must be positive")
        if not roots:
            raise ValueError("SITEMAP_ROOT_URLS must contain at least one URL")
        return settings
