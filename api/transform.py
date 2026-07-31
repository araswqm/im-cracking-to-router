"""
Clean, deduplicated vehicle transform for the ``/api/v2`` endpoint.

Takes the raw per-vehicle payload produced by the v1 endpoint
(``_fetch_vehicle_data`` in ``index.py``) and returns an organized,
human-readable JSON object.  The rules applied:

* All nested ``raw`` BYD payloads are stripped.
* Values BYD repeats across sections (odometer, SoC, timezone,
  temperatures, seat states, charge state, ...) are canonicalized to a
  single source.
* Enum integers are mapped to ``{"code": ..., "label": ...}`` using the
  semantics documented in the pyBYD models.  Binary flags become real
  booleans; warning indicators become ``{"status": ok|warning|unknown}``.
* Low-value blobs are dropped entirely: ``vehicleFunLearnInfo`` (internal
  learning flags) and ``cfFixedList`` (static Chinese feature catalogue).

``transform_vehicle`` is a pure function of the dict, so it can be unit
tested against a saved v1 response without any BYD credentials.
"""

from __future__ import annotations


# ---------------------------------------------------------------------------
# Enum value → label tables (semantics from pyBYD models)
# ---------------------------------------------------------------------------

CHARGING_STATES = {
    -1: "unknown",
    0: "not_charging",
    1: "charging",
    15: "unknown",  # note: pyBYD warns 15 doesn't reliably follow the charging
    # gun — verified against a car that was NOT plugged in yet reported 15;
    # connectState → ``battery.plugged_in`` is the authoritative source.
}
CONNECT_STATES = {-1: "unknown", 0: "disconnected", 1: "connected"}
ONLINE_STATES = {-1: "unknown", 1: "online", 2: "offline"}
VEHICLE_STATES = {
    -1: "unknown",
    0: "off",
    2: "unknown",  # pyBYD flags 2="on" as NOT CONFIRMED, and a physically
    # off/parked car still reported 2 — report as unknown rather than "on".
}
GEAR_STATES = {-1: "unknown", 1: "off", 3: "on"}
WINDOW_STATES = {-1: "unknown", 1: "closed", 2: "open"}
SEAT_LEVELS = {-1: None, 0: None, 1: "off", 2: "low", 3: "high"}  # 0 = no data
STEERING_WHEEL_HEAT = {-1: "unknown", 1: "off"}  # -1 = standard unknown sentinel,
# not "on" — verified against a parked car reporting -1 while nothing was heating
TIRE_UNITS = {-1: "unknown", 1: "bar", 2: "psi", 3: "kpa"}
AIR_CIRCULATION = {-1: "unknown", 0: "unavailable", 1: "external", 2: "internal"}
HVAC_STATUS = {-1: "unknown", 1: "on", 2: "off"}
AC_MODES = {-1: "unknown", 0: "off", 1: "auto", 2: "manual"}
WIND_MODES = {
    -1: "unknown",
    0: "off",
    1: "face",
    2: "face_foot",
    3: "foot",
    4: "foot_defrost",
    5: "defrost",
}
ENERGY_TYPES = {-1: "unknown", 0: "ev", 1: "ice", 2: "hybrid"}


def _enum(value: int | None, table: dict) -> dict | None:
    """Map a raw enum int to ``{"code": n, "label": "..."}`` (or ``None``)."""
    if value is None:
        return None
    return {"code": value, "label": table.get(value, "unknown")}


def _on_off(value) -> bool | None:
    """Map a strict 0/1 flag to a boolean; anything else (incl. -1) -> ``None``."""
    try:
        value = int(value)
    except (TypeError, ValueError):
        return None
    if value not in (0, 1):
        return None
    return bool(value)


def _warn(value: int | None) -> dict | None:
    """Warning indicator: 0=ok, >0=warning, -1=unavailable."""
    if value is None:
        return None
    if value < 0:
        return {"status": "unknown", "code": value}
    return {"status": "warning" if value > 0 else "ok", "code": value}


def _pwr(value: int | None) -> dict | None:
    """Motor power indicator: 0=ok, 1=warning, anything else = unknown.

    Some BYD hybrids persistently report ``pwr=2`` with no actual motor
    fault (confirmed on a SEAL U DM-i), so only ``1`` is treated as a real
    warning and other non-zero values are reported as unknown instead of
    inventing a fault.
    """
    if value is None:
        return None
    if value < 0:
        return {"status": "unknown", "code": value}
    return {"status": "ok" if value == 0 else ("warning" if value == 1 else "unknown"), "code": value}


def _temp(value, hvac_on: bool):
    """Climate temperature value, or ``None`` for the 0.0 "not set"
    sentinel BYD reports while the HVAC is off (avoids a misleading 0°C)."""
    if not hvac_on and value in (0, 0.0):
        return None
    return value


def _seat(value: int | None) -> str | None:
    """Seat heat/ventilation level label, or ``None`` when no data."""
    if value is None:
        return None
    return SEAT_LEVELS.get(value)


def _epb(value: int | None) -> str | None:
    """Electronic parking brake: 0=released, 1=engaged, -1=unknown."""
    if value is None or value < 0:
        return None
    return "engaged" if value == 1 else "released"


def _lock(value: int | None) -> bool | None:
    """Door lock: 1=unlocked, 2=locked, 0/-1=unknown/unavailable."""
    if value is None or value < 1:
        return None
    return value == 2


def _wind_position(value: int | None) -> dict | None:
    """Fan speed: 0=off, 1-7 speed levels."""
    if value is None:
        return None
    if value < 0:
        return {"code": value, "label": "unknown"}
    return {"code": value, "label": "off" if value == 0 else f"speed_{value}"}


def _first(*values) -> int | None:
    """First value that is neither ``None`` nor the -1 unknown sentinel."""
    for value in values:
        if value not in (None, -1):
            return value
    return None


def _flatten_permissions(items) -> list[str]:
    """Flatten the recursive ``rangeDetailList`` tree into unique names."""
    names: list[str] = []

    def walk(node) -> None:
        if isinstance(node, list):
            for child in node:
                walk(child)
        elif isinstance(node, dict):
            if node.get("name"):
                names.append(node["name"])
            walk(node.get("childList") or [])

    walk(items)
    seen: set[str] = set()
    unique = [n for n in names if not (n in seen or seen.add(n))]
    return unique


# ---------------------------------------------------------------------------
# Main transform
# ---------------------------------------------------------------------------


def transform_vehicle(raw: dict) -> dict:
    """Transform one v1 vehicle dict into the clean v2 schema.

    ``raw`` is the per-vehicle payload from the v1 endpoint: keys
    ``vin``, ``info``, ``realtime``, ``gps``, ``hvac``, ``charging``,
    ``energy``, ``config``.  Any section may be ``None`` or an
    ``{"error": ...}`` dict when its upstream call failed; the transform
    degrades gracefully to ``null`` fields.
    """
    rt = raw.get("realtime") or {}
    gps = raw.get("gps") or {}
    hvac = raw.get("hvac") or {}
    charging = raw.get("charging") or {}
    charge_status = charging.get("status") or {}
    schedule = charging.get("schedule") or {}
    energy = raw.get("energy") or {}
    cfg = raw.get("config") or {}
    info = raw.get("info") or {}
    info_raw = info.get("raw") or {}

    vin = raw.get("vin") or info_raw.get("vin")

    # ── Canonical cross-section dedup ────────────────────────────────────
    odometer_km = _first(
        rt.get("totalMileageV2"),
        rt.get("totalMileage"),
        info.get("totalMileage"),
    )
    timezone = (
        info_raw.get("vehicleTimeZone")
        or rt.get("vehicleTimeZone")
        or schedule.get("timeZone")
    )
    soc_percent = _first(rt.get("elecPercent"), charge_status.get("soc"))
    charge_state = _first(rt.get("chargeState"), charge_status.get("chargingState"))
    connect_state = _first(charge_status.get("connectState"), rt.get("connectState"))

    # ── vehicle ─────────────────────────────────────────────────────────
    cf_pic = info_raw.get("cfPic") or {}
    pictures = {}
    if info.get("picMainUrl"):
        pictures["main"] = info["picMainUrl"]
    if info.get("picSetUrl"):
        pictures["set"] = info["picSetUrl"]

    vehicle = {
        "vin": vin,
        "model": info.get("modelName"),
        "brand": info.get("brandName"),
        "plate": info.get("autoPlate"),
        "energy_type": _enum(info.get("energyType"), ENERGY_TYPES),
        "timezone": timezone,
        "odometer_km": odometer_km,
        "bought_at": info.get("autoBoughtTime"),
        "activated_at": info.get("yunActiveTime"),
        "color_code": cf_pic.get("clrCode"),
        "pictures": pictures,
        "firmware_version": info.get("tboxVersion"),
        "model_id": info.get("modelId"),
        "permissions": _flatten_permissions(info_raw.get("rangeDetailList")),
    }

    # ── status ──────────────────────────────────────────────────────────
    status = {
        "power": _enum(rt.get("vehicleState"), VEHICLE_STATES),
        "gear": _enum(rt.get("powerGear"), GEAR_STATES),
        "online": _enum(rt.get("onlineState"), ONLINE_STATES),
        "speed_kmh": rt.get("speed"),
        "total_power_kw": rt.get("totalPower"),
    }

    # ── location ────────────────────────────────────────────────────────
    location = {
        "latitude": gps.get("latitude"),
        "longitude": gps.get("longitude"),
        "direction_deg": gps.get("direction"),
        "speed_kmh": gps.get("speed"),
        "updated_at": gps.get("gpsTimestamp"),
    }

    # ── battery ─────────────────────────────────────────────────────────
    full_hour = rt.get("fullHour")
    full_minute = rt.get("fullMinute")
    estimated_full_min = (
        full_hour * 60 + full_minute
        if full_hour is not None and full_minute is not None
        else None
    )
    battery = {
        "soc_percent": soc_percent,
        "is_charging": charge_state == 1,
        "charging_state": _enum(charge_state, CHARGING_STATES),
        "plugged_in": _on_off(connect_state),
        "estimated_full_minutes": estimated_full_min,
        "less_than_one_min_to_full": _on_off(rt.get("lessOneMin")),
        "waiting": _on_off(charge_status.get("waitStatus")),
        "booking_enabled": _on_off(rt.get("bookingChargeState")),
        "battery_heating": _on_off(rt.get("batteryHeatState")),
        "charge_heating": _on_off(rt.get("chargeHeatState")),
    }

    # ── fuel & range (hybrid split) ─────────────────────────────────────
    fuel = {
        "level_percent": rt.get("oilPercent"),
        "range_km": rt.get("oilEndurance"),
    }
    rng = {
        "total_km": rt.get("enduranceMileageV2"),
        "ev_km": rt.get("evEndurance"),
        "fuel_km": rt.get("oilEndurance"),
        "unit": rt.get("enduranceMileageV2Unit"),
    }

    # ── doors ───────────────────────────────────────────────────────────
    doors = {
        "front_left": {
            "open": _on_off(rt.get("leftFrontDoor")),
            "locked": _lock(rt.get("leftFrontDoorLock")),
        },
        "front_right": {
            "open": _on_off(rt.get("rightFrontDoor")),
            "locked": _lock(rt.get("rightFrontDoorLock")),
        },
        "rear_left": {
            "open": _on_off(rt.get("leftRearDoor")),
            "locked": _lock(rt.get("leftRearDoorLock")),
        },
        "rear_right": {
            "open": _on_off(rt.get("rightRearDoor")),
            "locked": _lock(rt.get("rightRearDoorLock")),
        },
        "sliding_door": {
            "open": _on_off(rt.get("slidingDoor")),
            "locked": _lock(rt.get("slidingDoorLock")),
        },
        "trunk": {"open": _on_off(rt.get("trunkLid"))},
        "hood": {"open": _on_off(rt.get("forehold"))},
    }

    # ── windows ─────────────────────────────────────────────────────────
    windows = {
        "front_left": {
            "position": _enum(rt.get("leftFrontWindow"), WINDOW_STATES),
            "open_percent": rt.get("leftFrontWindowPct"),
        },
        "front_right": {
            "position": _enum(rt.get("rightFrontWindow"), WINDOW_STATES),
            "open_percent": rt.get("rightFrontWindowPct"),
        },
        "rear_left": {
            "position": _enum(rt.get("leftRearWindow"), WINDOW_STATES),
            "open_percent": rt.get("leftRearWindowPct"),
        },
        "rear_right": {
            "position": _enum(rt.get("rightRearWindow"), WINDOW_STATES),
            "open_percent": rt.get("rightRearWindowPct"),
        },
        "skylight": {"position": _enum(rt.get("skylight"), WINDOW_STATES)},
    }

    # ── climate (hvac is canonical; realtime duplicates are dropped) ────
    hvac_on = hvac.get("status") == 1
    climate = {
        "power": _enum(hvac.get("status"), HVAC_STATUS),
        "mode": _enum(hvac.get("airConditioningMode"), AC_MODES),
        "driver_temp_c": _temp(hvac.get("mainSettingTempNew"), hvac_on),
        "passenger_temp_c": _temp(hvac.get("copilotSettingTempNew"), hvac_on),
        "inside_temp_c": _temp(hvac.get("tempInCar"), hvac_on),
        "outside_temp_c": _temp(hvac.get("tempOutCar"), hvac_on),
        "dual_zone_supported": _on_off(hvac.get("whetherSupportAdjustTemp")),
        "fan": {
            "mode": _enum(hvac.get("windMode"), WIND_MODES),
            "speed": _wind_position(hvac.get("windPosition")),
        },
        "air_circulation": _enum(hvac.get("cycleChoice"), AIR_CIRCULATION),
        "front_defrost": _on_off(hvac.get("frontDefrostStatus")),
        "rear_defrost": _on_off(hvac.get("electricDefrostStatus")),
        "wiper_heat": _on_off(hvac.get("wiperHeatStatus")),
        "steering_wheel_heat": _enum(
            hvac.get("steeringWheelHeatState"), STEERING_WHEEL_HEAT
        ),
        "rapid_heat": _on_off(hvac.get("rapidIncreaseTempState")),
        "rapid_cool": _on_off(hvac.get("rapidDecreaseTempState")),
        "seats": {
            "driver": {
                "heat": _seat(hvac.get("mainSeatHeatState")),
                "ventilation": _seat(hvac.get("mainSeatVentilationState")),
            },
            "passenger": {
                "heat": _seat(hvac.get("copilotSeatHeatState")),
                "ventilation": _seat(hvac.get("copilotSeatVentilationState")),
            },
            "rear_left": {
                "heat": _seat(hvac.get("lrSeatHeatState")),
                "ventilation": _seat(hvac.get("lrSeatVentilationState")),
            },
            "rear_right": {
                "heat": _seat(hvac.get("rrSeatHeatState")),
                "ventilation": _seat(hvac.get("rrSeatVentilationState")),
            },
        },
    }

    # ── tires ───────────────────────────────────────────────────────────
    tires = {
        "unit": _enum(rt.get("tirePressUnit"), TIRE_UNITS),
        "system": _warn(rt.get("tirepressureSystem")),
        "rapid_leak": _warn(rt.get("rapidTireLeak")),
        "pressure": {
            "front_left": rt.get("leftFrontTirePressure"),
            "front_right": rt.get("rightFrontTirePressure"),
            "rear_left": rt.get("leftRearTirePressure"),
            "rear_right": rt.get("rightRearTirePressure"),
        },
        "status": {
            "front_left": _warn(rt.get("leftFrontTireStatus")),
            "front_right": _warn(rt.get("rightFrontTireStatus")),
            "rear_left": _warn(rt.get("leftRearTireStatus")),
            "rear_right": _warn(rt.get("rightRearTireStatus")),
        },
    }

    # ── energy (from energy models + parsed realtime breakouts) ─────────
    cum = energy.get("cumulativeEnergyConsumption") or {}
    nearest = energy.get("nearestEnergyConsumption") or {}
    self_graph = energy.get("selfGraph") or {}
    fleet_graph = energy.get("autoModelGraph") or {}
    energy_out = {
        "cumulative": {
            "electricity_per_100km": cum.get("avgEvConsumption"),
            "fuel_per_100km": cum.get("avgOilConsumption"),
            "electricity_unit": cum.get("evUnit"),
            "fuel_unit": cum.get("oilUnit"),
        },
        "nearest": {
            "electricity_per_100km": nearest.get("avgEvConsumption"),
            "fuel_per_100km": nearest.get("avgOilConsumption"),
            "equivalent_fuel_per_100km": nearest.get("avgEqOilConsumption"),
            "electricity_consumed": nearest.get("evConsumption"),
            "fuel_consumed": nearest.get("oilConsumption"),
            "electricity_unit": nearest.get("evUnit"),
            "fuel_unit": nearest.get("oilUnit"),
        },
        "last_7_days": {
            "own": {
                "unit": self_graph.get("energyConsumptionUnit"),
                "per_day": self_graph.get("energyConsumption"),
            },
            "fleet_average": {
                "unit": fleet_graph.get("energyConsumptionUnit"),
                "per_day": fleet_graph.get("energyConsumption"),
            },
        },
    }

    # ── systems ─────────────────────────────────────────────────────────
    systems = {
        "warnings": {
            "power_system": _warn(rt.get("powerSystem")),
            "power_steering": _warn(rt.get("eps")),
            "stability": _warn(rt.get("esp")),
            "abs": _warn(rt.get("absWarning")),
            "service": _warn(rt.get("svs")),
            "airbag": _warn(rt.get("srs")),
            "coolant_temperature": _warn(rt.get("ect")),
            "motor_power": _pwr(rt.get("pwr")),
            "oil_pressure": _warn(rt.get("oilPressureSystem")),
            "braking": _warn(rt.get("brakingSystem")),
            "charging": _warn(rt.get("chargingSystem")),
            "steering": _warn(rt.get("steeringSystem")),
            "ok_light": _warn(rt.get("okLight")),
        },
        "parking_brake": _epb(rt.get("epb")),
        "engine_status": rt.get("engineStatus"),
        "sentry_mode": _on_off(rt.get("sentryStatus")),
        "ota_upgrade_active": _on_off(rt.get("upgradeStatus")),
        "repair_mode": _on_off(rt.get("repairModeSwitch")),
        "power_battery_connection": _on_off(rt.get("powerBatteryConnection")),
    }

    # ── charging schedule ───────────────────────────────────────────────
    charge_dto = schedule.get("charge") or {}
    journey_dto = schedule.get("journey") or {}
    journey_raw = journey_dto.get("raw") or {}
    charging_schedule = {
        "charge": {
            "enabled": _on_off(charge_dto.get("status")),
            "start_time": charge_dto.get("startTime"),
            "end_time": charge_dto.get("endTime"),
            "charge_until_full": charge_dto.get("chargeUntilFull"),
            "charge_way": charge_dto.get("chargeWay"),
            "updated_at": charge_dto.get("updateTime"),
        },
        "journey": {
            "enabled": _on_off(journey_dto.get("status")),
            "use_vehicle_time": journey_dto.get("useVehicleTime"),
            "discount_start": journey_dto.get("discountStart"),
            "discount_end": journey_dto.get("discountEnd"),
            "ac_switch": _on_off(journey_raw.get("acSwitch")),
            "battery_heat": _on_off(journey_raw.get("batHeatSwitch")),
            "updated_at": journey_dto.get("updateTime"),
        },
        "updated_at": schedule.get("updateTime"),
    }

    # ── device (config; cfFixedList static catalogue intentionally dropped)
    device = {
        "style_id": cfg.get("styleId"),
        "terminal_type": cfg.get("terminalType"),
        "app_config_version": cfg.get("appConfigVersion"),
        "config_version": cfg.get("configVersion"),
        "widget_config_id": cfg.get("widgetConfigId"),
    }

    return {
        "vin": vin,
        "vehicle": vehicle,
        "status": status,
        "location": location,
        "battery": battery,
        "fuel": fuel,
        "range": rng,
        "doors": doors,
        "windows": windows,
        "climate": climate,
        "tires": tires,
        "energy": energy_out,
        "systems": systems,
        "charging_schedule": charging_schedule,
        "device": device,
        "updated_at": rt.get("timestamp"),
    }
