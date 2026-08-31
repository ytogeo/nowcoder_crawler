from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from .models import Base


class Database:
    def __init__(self, dsn: str, *, echo: bool = False) -> None:
        self.engine: Engine = create_engine(dsn, echo=echo, pool_pre_ping=True)
        self.session_factory = sessionmaker(
            bind=self.engine, expire_on_commit=False, autoflush=False
        )

    def create_schema(self) -> None:
        Base.metadata.create_all(self.engine)

    @contextmanager
    def session(self) -> Iterator[Session]:
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def close(self) -> None:
        self.engine.dispose()
