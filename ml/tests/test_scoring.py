from ml.scoring import fit_baselines, score_event, severity_from_score


def test_severity_boundaries():
    assert severity_from_score(0.39) == "NORMAL"
    assert severity_from_score(0.40) == "LOW"
    assert severity_from_score(0.90) == "CRITICAL"
    assert severity_from_score(1.00) == "CRITICAL"


def test_thin_user_fallback_and_score_bounds():
    history = []
    for i in range(40):
        history.append(
            {
                "event_id": f"H{i}",
                "organization_id": "ORG_X",
                "domain": "warehouse",
                "event_type": "event",
                "timestamp": f"2026-08-01T10:00:00Z",
                "user_id": "POWER",
                "location": "Delhi",
                "device_id": "D1",
                "ip_address": "1.1.1.1",
                "amount": 100 + i,
            }
        )
    for i in range(3):
        history.append(
            {
                "event_id": f"T{i}",
                "organization_id": "ORG_X",
                "domain": "warehouse",
                "event_type": "event",
                "timestamp": "2026-08-02T10:00:00Z",
                "user_id": "THIN",
                "location": "Delhi",
                "amount": 100,
            }
        )
    org = {"organization_id": "ORG_X", "anomaly_score_threshold": 0.75, "baseline_lookback_days": 30}
    baselines = fit_baselines(history, org)
    ub, thin = baselines.resolve_user("THIN")
    assert thin is True
    assert ub.amount_avg is not None
    event = {
        "event_id": "LIVE",
        "organization_id": "ORG_X",
        "user_id": "THIN",
        "device_id": "NEWDEV",
        "ip_address": "9.9.9.9",
        "location": "Goa",
        "amount": 5000,
    }
    features = {
        "amount": 5000,
        "user_amount_avg": ub.amount_avg,
        "user_amount_std": ub.amount_std,
        "amount_zscore": (5000 - ub.amount_avg) / (ub.amount_std or 1),
        "new_device": True,
        "new_ip": True,
        "unusual_time": True,
        "distance_from_usual": 1,
        "hour_of_day": 2,
        "events_last_5min": 1,
        "events_last_hour": 1,
        "thin_user_baseline": True,
        "user_event_frequency": ub.events_per_day,
        "device_id": "NEWDEV",
        "ip_address": "9.9.9.9",
        "usual_location": ub.usual_location,
    }
    result = score_event(event, features, org, baselines, model=None)
    assert 0 <= result.anomaly_score <= 1
    assert result.risk_score == result.anomaly_score
    assert any(r.code == "THIN_USER_BASELINE_FALLBACK" for r in result.reasons)
    assert result.model_version.startswith("baseline")
