from __future__ import annotations

from collections.abc import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backend.app.config import DATABASE_URL
from backend.app.models import Base

engine: Engine | None = None
SessionLocal: sessionmaker | None = None


def configure_engine(url: str | None = None) -> Engine:
    global engine, SessionLocal
    url = url or DATABASE_URL
    kwargs = {"future": True}
    if url.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False}
        if url in ("sqlite://", "sqlite:///:memory:"):
            kwargs["poolclass"] = StaticPool
    engine = create_engine(url, **kwargs)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    return engine


def get_engine() -> Engine:
    if engine is None:
        configure_engine()
    assert engine is not None
    return engine


def get_session_factory() -> sessionmaker:
    if SessionLocal is None:
        configure_engine()
    assert SessionLocal is not None
    return SessionLocal


def init_db() -> None:
    Base.metadata.create_all(bind=get_engine())


def get_session() -> Generator[Session, None, None]:
    factory = get_session_factory()
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def ping_db() -> bool:
    try:
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
