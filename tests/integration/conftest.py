from __future__ import annotations

import os

import pytest

from nowcoder_crawler.database import Database
from nowcoder_crawler.models import Base


@pytest.fixture
def database() -> Database:
    dsn = os.getenv("TEST_MYSQL_DSN")
    if not dsn:
        pytest.skip("TEST_MYSQL_DSN is not configured")
    database = Database(dsn)
    Base.metadata.drop_all(database.engine)
    database.create_schema()
    try:
        yield database
    finally:
        database.close()
