"""Tests for the monitoring endpoints: /monitor, /monitor/raw, and
/api/health."""

from __future__ import annotations


def test_health(client):
    r = client.get("/api/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert "timestamp" in body


def test_monitor_raw(client):
    r = client.get("/monitor/raw")
    assert r.status_code == 200
    body = r.json()
    assert body["success"] is True
    assert body["vehicle_count"] == 1
    vehicle = body["vehicles"][0]
    assert vehicle["vin"] == "LGX12345678901234"
    # Raw section data present, unfiltered.
    assert vehicle["realtime"]["elecPercent"] == 87
    assert vehicle["gps"]["latitude"] == 52.1


def test_monitor_raw_trailing_slash(client):
    r = client.get("/monitor/raw/")
    assert r.status_code == 200
    assert r.json()["vehicle_count"] == 1


def test_monitor_transformed(client):
    r = client.get("/monitor")
    assert r.status_code == 200
    body = r.json()
    assert body["success"] is True
    assert body["vehicle_count"] == 1
    vehicle = body["vehicles"][0]
    # Transformed schema: canonical fields, no raw pyBYD sections.
    assert "realtime" not in vehicle
    assert vehicle["vin"] == "LGX12345678901234"
    assert vehicle["vehicle"]["model"] == "SEAL U DM-i"


def test_monitor_transformed_trailing_slash(client):
    r = client.get("/monitor/")
    assert r.status_code == 200
    assert r.json()["vehicle_count"] == 1

