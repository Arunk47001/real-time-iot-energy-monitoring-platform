"""Behavioral model of a solar + battery IoT energy device.

Pure domain logic: deterministic given its RNG, no I/O. Solar output follows
a diurnal curve, household load follows a morning/evening pattern, and the
battery state of charge integrates the difference between the two.
"""

import math
import random
from datetime import datetime

from app.domain.models import TelemetryReading

_NOMINAL_VOLTAGE = 230.0
_MIN_SOC = 5.0
_MAX_SOC = 100.0


class SimulatedDevice:
    def __init__(self, device_id: str, site_id: str, rng: random.Random) -> None:
        self.device_id = device_id
        self.site_id = site_id
        self._rng = rng
        # Per-device fixed characteristics
        self._solar_capacity_kw = rng.uniform(2.0, 5.0)
        self._base_load_kw = rng.uniform(0.4, 2.5)
        self._battery_capacity_kwh = rng.uniform(5.0, 15.0)
        self._signal_base_dbm = rng.randint(-85, -55)
        # Mutable state
        self._soc = rng.uniform(30.0, 95.0)

    def take_reading(self, now: datetime, interval_seconds: float) -> TelemetryReading:
        hour = now.hour + now.minute / 60.0 + now.second / 3600.0

        solar_kw = self._solar_output_kw(hour)
        load_kw = self._load_kw(hour)
        self._update_battery(solar_kw, load_kw, interval_seconds)

        voltage = self._rng.gauss(_NOMINAL_VOLTAGE, 1.5)
        current = load_kw * 1000.0 / voltage

        return TelemetryReading(
            device_id=self.device_id,
            timestamp=now,
            site_id=self.site_id,
            battery_soc=round(self._soc, 1),
            solar_kwh=round(solar_kw, 2),
            energy_kwh=round(load_kw, 2),
            voltage=round(voltage, 1),
            current=round(current, 2),
            temperature=round(self._temperature_c(hour), 1),
            signal_strength=self._signal_base_dbm + self._rng.randint(-4, 4),
        )

    def _solar_output_kw(self, hour: float) -> float:
        """Zero at night, peaking at solar noon, with cloud-cover noise."""
        irradiance = math.sin(math.pi * (hour - 6.0) / 12.0) if 6.0 < hour < 18.0 else 0.0
        return self._solar_capacity_kw * irradiance * self._rng.uniform(0.75, 1.0)

    def _load_kw(self, hour: float) -> float:
        """Base load with morning (~8h) and evening (~20h) peaks."""
        morning = 0.5 * math.exp(-((hour - 8.0) ** 2) / 8.0)
        evening = 0.9 * math.exp(-((hour - 20.0) ** 2) / 8.0)
        factor = 0.6 + morning + evening
        return self._base_load_kw * factor * self._rng.uniform(0.85, 1.15)

    def _update_battery(self, solar_kw: float, load_kw: float, interval_seconds: float) -> None:
        net_kwh = (solar_kw - load_kw) * (interval_seconds / 3600.0)
        self._soc += net_kwh / self._battery_capacity_kwh * 100.0
        self._soc = min(_MAX_SOC, max(_MIN_SOC, self._soc))

    def _temperature_c(self, hour: float) -> float:
        """Ambient temperature: ~24C at dawn, ~36C mid-afternoon."""
        diurnal = 30.0 + 6.0 * math.sin(math.pi * (hour - 9.0) / 12.0)
        return diurnal + self._rng.gauss(0.0, 0.8)
