"""
pybyd stand-in so ``api/index.py`` can be imported and tested locally
without installing the real pyBYD library (which is a Vercel deploy-time
dependency installed from requirements.txt).

Injected as ``sys.modules["pybyd"]`` (and ``"pybyd.exceptions"``) by
``tests/conftest.py`` before the app is imported.  Only the surface the app
actually uses is implemented.
"""

from __future__ import annotations


class BydApiError(Exception):
    pass


class BydAuthenticationError(Exception):
    pass


class BydDataUnavailableError(Exception):
    pass


class BydTransportError(Exception):
    pass


class FakeVehicle:
    """Stand-in for a pyBYD vehicle list entry."""

    def __init__(self, vin: str = "LGX12345678901234", model: str = "SEAL U DM-i"):
        self.vin = vin
        self._model = model

    def model_dump(self, by_alias: bool = True, mode: str = "json") -> dict:
        return {
            "vin": self.vin,
            "modelName": self._model,
            "brandName": "BYD",
            "autoPlate": "TEST-123",
            "energyType": 2,  # hybrid
        }


class BydConfig:
    def __init__(
        self,
        username: str,
        password: str,
        base_url: str | None = None,
        country_code: str | None = None,
    ):
        self.username = username
        self.password = password
        self.base_url = base_url
        self.country_code = country_code


class BydClient:
    """Fake client returning canned, mostly-empty vehicle data."""

    def __init__(self, config: BydConfig):
        self.config = config

    async def __aenter__(self) -> "BydClient":
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False

    async def login(self) -> None:
        return None

    async def get_vehicles(self) -> list[FakeVehicle]:
        return [FakeVehicle()]

    async def get_vehicle_realtime(self, vin: str) -> dict:
        return {
            "totalMileage": 12345,
            "elecPercent": 87,
            "vehicleState": 0,
            "powerGear": 1,
            "onlineState": 1,
            "speed": 0,
        }

    async def get_gps_info(
        self,
        vin: str,
        poll_attempts: int | None = None,
        poll_interval: float | None = None,
        mqtt_timeout: float | None = None,
    ) -> dict:
        return {"latitude": 52.1, "longitude": 4.3, "direction": 90}

    async def get_hvac_status(self, vin: str) -> dict:
        return {}

    async def get_charging_homepage(self, vin: str) -> tuple[dict, dict]:
        return ({"soc": 87, "connectState": 1}, {})

    async def get_energy_consumption(self, vin: str) -> dict:
        return {}

    async def get_latest_config(self, vin: str) -> dict:
        return {}
