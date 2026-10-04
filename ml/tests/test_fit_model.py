from __future__ import annotations

from ml.scoring import feature_vector, fit_baselines, fit_model, score_event


def _history(n: int = 40) -> list[dict]:
    rows = []
    for i in range(n):
        rows.append(
            {
                "event_id": f"H{i}",
                "organization_id": "ORG_X",
                "domain": "warehouse",
                "event_type": "event",
                "timestamp": "2026-08-01T10:00:00Z",
                "user_id": "POWER",
                "location": "Delhi",
                "device_id": "D1",
                "ip_address": "1.1.1.1",
                "amount": 100 + (i % 5),
            }
        )
    return rows


def test_fit_model_none_on_thin_matrix():
    assert fit_model([], {"organization_id": "ORG_X"}) is None
    assert fit_model([[0.0] * 13] * 5, {"organization_id": "ORG_X"}) is None


def test_iforest_increases_outlier_score():
    history = _history(50)
    org = {"organization_id": "ORG_X", "anomaly_score_threshold": 0.75}
    baselines = fit_baselines(history, org)
    matrix = []
    for ev in history:
        features = {
            "amount": ev["amount"],
            "user_amount_avg": 102.0,
            "user_amount_std": 2.0,
            "amount_zscore": 0.1,
            "events_last_5min": 1,
            "events_last_hour": 1,
            "distance_from_usual": 0,
            "new_device": 0,
            "new_ip": 0,
            "unusual_time": 0,
            "hour_of_day": 10,
            "time_since_previous_event_seconds": 3600,
            "user_event_frequency": 5.0,
        }
        matrix.append(feature_vector(features))
    model = fit_model(matrix, org)
    assert model is not None
    event = {
        "event_id": "LIVE",
        "organization_id": "ORG_X",
        "user_id": "POWER",
        "device_id": "NEW",
        "ip_address": "9.9.9.9",
        "amount": 50000,
    }
    features = {
        "amount": 50000,
        "user_amount_avg": 102.0,
        "user_amount_std": 2.0,
        "amount_zscore": 200.0,
        "new_device": True,
        "new_ip": True,
        "unusual_time": True,
        "distance_from_usual": 1,
        "hour_of_day": 2,
        "events_last_5min": 8,
        "events_last_hour": 22,
        "user_event_frequency": 5.0,
        "time_since_previous_event_seconds": 1,
        "thin_user_baseline": False,
    }
    with_model = score_event(event, features, org, baselines, model=model)
    without = score_event(event, features, org, baselines, model=None)
    assert 0 <= with_model.anomaly_score <= 1
    assert with_model.is_anomaly is True
    assert with_model.model_version == "baseline-iforest-v1"
    assert without.model_version == "baseline-only-v1"
    assert abs((0.40 * with_model.baseline_score + 0.30 * with_model.contextual_score + 0.30 * with_model.iforest_score) - with_model.anomaly_score) < 1e-6
