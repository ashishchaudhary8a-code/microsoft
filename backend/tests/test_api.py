from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from backend.app.db import configure_engine, get_session, get_session_factory, init_db
from backend.app.models import utcnow
from backend.app.persist import insert_alert, upsert_processed
from backend.app.seed import seed_organizations


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("BACKEND_CONSUME", "0")
    monkeypatch.setenv("SEED_ORGS", "0")
    monkeypatch.setenv("BACKEND_SKIP_BROKER_PROBE", "1")
    configure_engine("sqlite://")
    init_db()
    TestingSession = get_session_factory()

    def override():
        session = TestingSession()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    from backend.app import main as mainmod

    mainmod.app.dependency_overrides[get_session] = override
    # seed two orgs
    s = TestingSession()
    from pathlib import Path
    import json

    org_dir = tmp_path / "orgs"
    org_dir.mkdir()
    for org_id, name, domain in (
        ("ORG_HOSPITAL_001", "Hospital XYZ", "hospital"),
        ("ORG_HOTEL_001", "Hotel Aurora", "hotel"),
    ):
        (org_dir / f"{org_id}.json").write_text(
            json.dumps(
                {
                    "organization_id": org_id,
                    "organization_name": name,
                    "domain": domain,
                    "timezone": "UTC",
                    "currency": "INR",
                    "freshness_sla_seconds": 5,
                }
            )
        )
    seed_organizations(s, org_dir)
    s.commit()
    s.close()

    with TestClient(mainmod.app) as c:
        c.extra = {"Session": TestingSession}
        yield c
    mainmod.app.dependency_overrides.clear()


def _processed(org, eid, score=0.2, anomaly=False, user="USR_204"):
    now = datetime.now(timezone.utc)
    return {
        "event": {
            "event_id": eid,
            "organization_id": org,
            "domain": "hospital",
            "event_type": "event",
            "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "user_id": user,
            "username": "employee",
            "location": "Pune",
            "amount": 100,
            "currency": "INR",
            "payload": {},
            "metadata": {},
        },
        "features": {"amount": 100, "new_device": False},
        "processing": {
            "received_at": (now - timedelta(milliseconds=200)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "processed_at": now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "consumer_id": "streaming-0",
        },
        "anomaly": {
            "anomaly_score": score,
            "severity": "CRITICAL" if score >= 0.9 else ("HIGH" if anomaly else "NORMAL"),
            "is_anomaly": anomaly or score >= 0.75,
            "reasons": [{"code": "TEST", "feature": "amount", "detail": "test", "contribution": 0.1}],
            "model_version": "baseline-iforest-v1",
        },
    }


def test_health_and_orgs(client):
    r = client.get("/api/health")
    assert r.status_code == 200
    body = r.json()
    assert "backend" in body
    assert body["database"] == "ok"
    r = client.get("/api/organizations")
    ids = {o["organization_id"] for o in r.json()["organizations"]}
    assert "ORG_HOSPITAL_001" in ids
    assert "ORG_HOTEL_001" in ids


def test_org_required(client):
    r = client.get("/api/events")
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "MISSING_ORGANIZATION"


def test_org_isolation(client):
    Session = client.extra["Session"]
    s = Session()
    upsert_processed(s, _processed("ORG_HOSPITAL_001", "EVT_H1", score=0.96, anomaly=True))
    upsert_processed(s, _processed("ORG_HOTEL_001", "EVT_O1", score=0.2, anomaly=False, user="USR_H01"))
    insert_alert(
        s,
        {
            "alert_id": "ALT_H",
            "organization_id": "ORG_HOSPITAL_001",
            "alert_type": "SINGLE_EXTREME_EVENT",
            "severity": "CRITICAL",
            "user_id": "USR_204",
            "event_ids": ["EVT_H1"],
            "title": "Critical anomaly on event EVT_H1",
            "summary": "Single event scored 0.96 (CRITICAL)",
            "created_at": utcnow().isoformat(),
        },
    )
    s.commit()
    s.close()

    hosp = client.get("/api/events", params={"organization_id": "ORG_HOSPITAL_001"}).json()
    hotel = client.get("/api/events", params={"organization_id": "ORG_HOTEL_001"}).json()
    assert [i["event_id"] for i in hosp["items"]] == ["EVT_H1"]
    assert [i["event_id"] for i in hotel["items"]] == ["EVT_O1"]

    detail_ok = client.get("/api/anomalies/EVT_H1", params={"organization_id": "ORG_HOSPITAL_001"})
    assert detail_ok.status_code == 200
    assert detail_ok.json()["anomaly_score"] == 0.96
    assert detail_ok.json()["alerts"][0]["alert_type"] == "SINGLE_EXTREME_EVENT"

    leaked = client.get("/api/anomalies/EVT_H1", params={"organization_id": "ORG_HOTEL_001"})
    assert leaked.status_code == 404

    alerts_h = client.get("/api/alerts", params={"organization_id": "ORG_HOTEL_001"}).json()
    assert alerts_h["items"] == []


def test_dashboard_and_freshness(client):
    Session = client.extra["Session"]
    s = Session()
    upsert_processed(s, _processed("ORG_HOSPITAL_001", "EVT_H2", score=0.12, anomaly=False))
    s.commit()
    s.close()
    dash = client.get("/api/dashboard", params={"organization_id": "ORG_HOSPITAL_001"})
    assert dash.status_code == 200
    body = dash.json()
    assert body["events_total"] >= 1
    assert body["freshness_status"] in ("GREEN", "WARNING", "CRITICAL")
    fresh = client.get("/api/pipeline/freshness", params={"organization_id": "ORG_HOSPITAL_001"})
    assert fresh.status_code == 200
    assert fresh.json()["organization_id"] == "ORG_HOSPITAL_001"
