"""Domain entities. No framework or I/O dependencies."""

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class TelemetryReading:
    """Raw telemetry as emitted by a device."""

    device_id: str
    timestamp: datetime
    site_id: str
    battery_soc: float
    solar_kwh: float
    energy_kwh: float
    voltage: float
    current: float
    temperature: float
    signal_strength: int


@dataclass(frozen=True, slots=True)
class EnrichedTelemetry:
    """Telemetry augmented with metrics derived in stream processing."""

    reading: TelemetryReading
    power_kw: float           # instantaneous power drawn (V * I / 1000)
    net_energy_kwh: float     # solar generation minus consumption
    low_battery: bool         # SOC below alert threshold
    voltage_anomaly: bool     # outside the nominal grid band
