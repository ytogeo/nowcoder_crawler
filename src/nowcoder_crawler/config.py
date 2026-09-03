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


def _int_with_legacy(name: str, legacy_name: str, default: int) -> int:
    if os.getenv(name) is not None:
        return _int(name, default)
    return _int(legacy_name, default)


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
    experience_api_max_pages: int
    experience_api_interval_seconds: float
    experience_api_jitter_seconds: float
    discovery_queue_maxsize: int
    discovery_db_batch_size: int
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
            experience_api_max_pages=_int_with_legacy(
                "EXPERIENCE_API_MAX_PAGES", "CENTER_MAX_PAGES", 20
            ),
            experience_api_interval_seconds=_float(
                "EXPERIENCE_API_INTERVAL_SECONDS", 2
            ),
            experience_api_jitter_seconds=_float("EXPERIENCE_API_JITTER_SECONDS", 1),
            discovery_queue_maxsize=_int("DISCOVERY_QUEUE_MAXSIZE", 1_000),
            discovery_db_batch_size=_int("DISCOVERY_DB_BATCH_SIZE", 200),
            sitemap_max_documents=_int("SITEMAP_MAX_DOCUMENTS", 20),
            sitemap_max_urls=_int("SITEMAP_MAX_URLS", 50_000),
            sitemap_root_urls=roots,
            failpoint_after_success_commit=_bool("FAILPOINT_AFTER_SUCCESS_COMMIT"),
        )
        if (
            settings.worker_prefetch < 1
            or settings.fetch_max_attempts < 1
            or settings.experience_api_max_pages < 1
            or settings.discovery_queue_maxsize < 1
            or settings.discovery_db_batch_size < 1
        ):
            raise ValueError(
                "worker, experience API, and discovery capacity values must be positive"
            )
        if not roots:
            raise ValueError("SITEMAP_ROOT_URLS must contain at least one URL")
        return settings
