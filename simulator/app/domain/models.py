"""Domain entities. No framework or I/O dependencies."""

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class TelemetryReading:
    """A single telemetry sample emitted by an IoT energy device."""

    device_id: str
    timestamp: datetime
    site_id: str
    battery_soc: float        # state of charge, percent (0-100)
    solar_kwh: float          # solar generation in the reporting window
    energy_kwh: float         # energy consumption in the reporting window
    voltage: float            # volts
    current: float            # amperes
    temperature: float        # degrees Celsius
    signal_strength: int      # RSSI, dBm
