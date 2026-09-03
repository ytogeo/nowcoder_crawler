from pathlib import Path

import pytest

from nowcoder_crawler.config import Settings


def _required_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MYSQL_DSN", "sqlite://")
    monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")
    monkeypatch.setenv("RAW_DATA_DIR", str(tmp_path))


def test_experience_api_max_pages_prefers_new_name(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _required_env(monkeypatch, tmp_path)
    monkeypatch.setenv("EXPERIENCE_API_MAX_PAGES", "21")
    monkeypatch.setenv("CENTER_MAX_PAGES", "not-an-integer")

    assert Settings.from_env().experience_api_max_pages == 21


def test_experience_api_max_pages_supports_legacy_name(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _required_env(monkeypatch, tmp_path)
    monkeypatch.delenv("EXPERIENCE_API_MAX_PAGES", raising=False)
    monkeypatch.setenv("CENTER_MAX_PAGES", "17")

    assert Settings.from_env().experience_api_max_pages == 17


def test_discovery_capacity_defaults(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _required_env(monkeypatch, tmp_path)

    settings = Settings.from_env()

    assert settings.discovery_queue_maxsize == 1000
    assert settings.discovery_db_batch_size == 200
