"""
BYD Vehicle Data API — Vercel Serverless Function.

Provides a single GET endpoint that returns all vehicle data (realtime,
GPS, HVAC, charging, energy, config) for the BYD account configured via
environment variables.

Environment Variables:
    BYD_USERNAME (required) — BYD account email or phone
    BYD_PASSWORD (required) — BYD account password
    BYD_BASE_URL  (optional) — API base URL (default: EU endpoint)
    BYD_COUNTRY_CODE (optional) — Two-letter country code (default: NL)

Endpoints:
    GET /api              — All vehicles, all data
    GET /api?vin=XXXXX    — Single vehicle by VIN
    GET /api/health       — Health check (no BYD auth)
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from pybyd import BydClient, BydConfig
from pybyd.exceptions import (
    BydApiError,
    BydAuthenticationError,
    BydDataUnavailableError,
    BydTransportError,
)

# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------

app = FastAPI(
    title="BYD Vehicle Data API",
    description="Fetch all vehicle data from your BYD account via pyBYD.",
    version="1.0.0",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_config() -> BydConfig:
    """Read BYD credentials from environment and return a BydConfig.

    Raises HTTPException(500) if required variables are missing.
    """
    username = os.environ.get("BYD_USERNAME", "").strip()
    password = os.environ.get("BYD_PASSWORD", "").strip()

    if not username or not password:
        raise HTTPException(
            status_code=500,
            detail=(
                "Missing required environment variables. "
                "Set BYD_USERNAME and BYD_PASSWORD in your Vercel project settings."
            ),
        )

    return BydConfig(
        username=username,
        password=password,
        base_url=os.environ.get(
            "BYD_BASE_URL", "https://dilinkappoversea-eu.byd.auto"
        ),
        country_code=os.environ.get("BYD_COUNTRY_CODE", "NL"),
    )


def _serialize(value: object) -> object:
    """Convert a pyBYD model or plain value into a JSON-safe dict/value."""
    if value is None:
        return None
    if hasattr(value, "model_dump"):
        return value.model_dump(by_alias=True, mode="json")
    return value


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get("/api/health")
async def health() -> dict:
    """Lightweight health check — does NOT contact BYD servers."""
    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api")
async def get_vehicle_data(
    request: Request,
    vin: str | None = Query(
        default=None,
        description="Filter by a specific vehicle VIN. Omit to return all vehicles.",
    ),
) -> dict:
    """Fetch comprehensive vehicle data for the configured BYD account.

    Authenticates with BYD using credentials from environment variables,
    then gathers realtime, GPS, HVAC, charging, energy, and configuration
    data for every vehicle (optionally filtered by VIN).
    """
    config = _build_config()

    # ── Authenticate & discover vehicles ──────────────────────────────
    async with BydClient(config) as client:
        try:
            await client.login()
        except BydAuthenticationError as exc:
            raise HTTPException(
                status_code=401,
                detail=f"BYD authentication failed. Check your credentials. ({exc})",
            ) from exc
        except BydTransportError as exc:
            raise HTTPException(
                status_code=504,
                detail=f"Could not reach BYD servers. ({exc})",
            ) from exc
        except BydApiError as exc:
            raise HTTPException(
                status_code=502,
                detail=f"BYD API error during login. ({exc})",
            ) from exc

        # Fetch vehicle list
        try:
            vehicles = await client.get_vehicles()
        except (BydApiError, BydTransportError) as exc:
            raise HTTPException(
                status_code=502,
                detail=f"Failed to fetch vehicle list. ({exc})",
            ) from exc

        if not vehicles:
            return {
                "success": True,
                "vehicle_count": 0,
                "vehicles": [],
                "message": "No vehicles found on this BYD account.",
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }

        # Optional VIN filter
        if vin:
            vehicles = [v for v in vehicles if v.vin == vin]
            if not vehicles:
                raise HTTPException(
                    status_code=404,
                    detail=f"No vehicle found with VIN: {vin}",
                )

        # ── Gather all data for each vehicle (parallel per-vehicle) ──
        vehicle_results: list[dict] = []

        for vehicle in vehicles:
            v_data = await _fetch_vehicle_data(client, vehicle.vin)
            v_data["vin"] = vehicle.vin
            v_data["info"] = vehicle.model_dump(by_alias=True, mode="json")
            vehicle_results.append(v_data)

        return {
            "success": True,
            "vehicle_count": len(vehicle_results),
            "vehicles": vehicle_results,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }


# ---------------------------------------------------------------------------
# Per-vehicle data gathering
# ---------------------------------------------------------------------------

_FETCH_ENDPOINTS: dict[str, str] = {
    "realtime": "get_vehicle_realtime",
    "gps": "get_gps_info",
    "hvac": "get_hvac_status",
    "charging": "get_charging_homepage",
    "energy": "get_energy_consumption",
    "config": "get_latest_config",
}


async def _fetch_vehicle_data(client: BydClient, vin: str) -> dict[str, object]:
    """Fetch all data endpoints for a single vehicle in parallel.

    Returns a dict keyed by data category.  On per-endpoint failure the
    value is ``{"error": "<message>"}`` or ``null`` (unavailable data).
    """

    async def _safe_fetch(key: str) -> tuple[str, object | None, str | None]:
        """Run one endpoint and return (key, result, error_message)."""
        coro = getattr(client, _FETCH_ENDPOINTS[key])(vin)
        try:
            result = await coro
            return key, result, None
        except BydDataUnavailableError:
            return key, None, None
        except (BydApiError, BydTransportError) as exc:
            return key, None, str(exc)
        except Exception as exc:
            return key, None, f"Unexpected error: {exc}"

    # Launch all 6 endpoints concurrently
    tasks = [_safe_fetch(key) for key in _FETCH_ENDPOINTS]
    gathered = await asyncio.gather(*tasks)

    data: dict[str, object] = {}
    for key, value, error in gathered:
        if error is not None:
            data[key] = {"error": error}
        elif value is None:
            data[key] = None
        elif isinstance(value, tuple):
            # get_charging_homepage → (ChargingStatus, SmartChargingSchedule)
            data[key] = {
                "status": _serialize(value[0]),
                "schedule": _serialize(value[1]),
            }
        else:
            data[key] = _serialize(value)

    return data


# ---------------------------------------------------------------------------
# Global error handlers
# ---------------------------------------------------------------------------


@app.exception_handler(BydAuthenticationError)
async def _auth_exc_handler(_request: Request, exc: BydAuthenticationError) -> JSONResponse:
    return JSONResponse(
        status_code=401,
        content={"success": False, "error": "authentication_failed", "detail": str(exc)},
    )


@app.exception_handler(BydTransportError)
async def _transport_exc_handler(_request: Request, exc: BydTransportError) -> JSONResponse:
    return JSONResponse(
        status_code=504,
        content={"success": False, "error": "transport_error", "detail": str(exc)},
    )


@app.exception_handler(BydApiError)
async def _api_exc_handler(_request: Request, exc: BydApiError) -> JSONResponse:
    return JSONResponse(
        status_code=502,
        content={"success": False, "error": "byd_api_error", "detail": str(exc)},
    )


@app.exception_handler(Exception)
async def _generic_exc_handler(_request: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(
        status_code=500,
        content={"success": False, "error": "internal_error", "detail": str(exc)},
    )
