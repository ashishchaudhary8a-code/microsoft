from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.app.db import get_session_factory, init_db
from backend.app.ingest import attach_loop, start_ingest_thread
from backend.app.metrics import set_component_health
from backend.app.seed import seed_organizations
from backend.app.api import router

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("backend")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    if os.environ.get("SEED_ORGS", "1") == "1":
        session = get_session_factory()()
        try:
            n = seed_organizations(session)
            session.commit()
            log.info("seeded %s organizations", n)
        except Exception:
            session.rollback()
            log.exception("org seed failed")
        finally:
            session.close()
    set_component_health("backend", True)
    loop = asyncio.get_running_loop()
    attach_loop(loop)
    if os.environ.get("BACKEND_CONSUME", "1") == "1":
        start_ingest_thread()
    yield


app = FastAPI(title="Anomaly Detection API", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(router)


def run() -> None:
    import uvicorn

    uvicorn.run("backend.app.main:app", host="0.0.0.0", port=8000, reload=False)


if __name__ == "__main__":
    run()
