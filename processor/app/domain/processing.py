"""Business rules for validating and enriching telemetry. Pure functions."""

from app.domain.models import EnrichedTelemetry, TelemetryReading

LOW_BATTERY_THRESHOLD_PCT = 20.0
VOLTAGE_NOMINAL_MIN = 207.0   # 230V -10%
VOLTAGE_NOMINAL_MAX = 253.0   # 230V +10%


def is_plausible(reading: TelemetryReading) -> bool:
    """Reject physically impossible readings before they reach storage."""
    return (
        0.0 <= reading.battery_soc <= 100.0
        and reading.solar_kwh >= 0.0
        and reading.energy_kwh >= 0.0
        and 0.0 < reading.voltage < 400.0
        and reading.current >= 0.0
        and -60.0 <= reading.temperature <= 90.0
        and -120 <= reading.signal_strength <= 0
    )


def enrich(reading: TelemetryReading) -> EnrichedTelemetry:
    return EnrichedTelemetry(
        reading=reading,
        power_kw=round(reading.voltage * reading.current / 1000.0, 3),
        net_energy_kwh=round(reading.solar_kwh - reading.energy_kwh, 2),
        low_battery=reading.battery_soc < LOW_BATTERY_THRESHOLD_PCT,
        voltage_anomaly=not (
            VOLTAGE_NOMINAL_MIN <= reading.voltage <= VOLTAGE_NOMINAL_MAX
        ),
    )
