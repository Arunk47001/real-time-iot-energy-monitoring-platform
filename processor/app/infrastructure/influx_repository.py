"""InfluxDB adapter implementing the TelemetryRepository port."""

import logging

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from app.domain.models import EnrichedTelemetry

logger = logging.getLogger(__name__)

MEASUREMENT = "device_telemetry"


def _to_point(item: EnrichedTelemetry) -> Point:
    r = item.reading
    return (
        Point(MEASUREMENT)
        .tag("device_id", r.device_id)
        .tag("site_id", r.site_id)
        .field("battery_soc", float(r.battery_soc))
        .field("solar_kwh", float(r.solar_kwh))
        .field("energy_kwh", float(r.energy_kwh))
        .field("voltage", float(r.voltage))
        .field("current", float(r.current))
        .field("temperature", float(r.temperature))
        .field("signal_strength", int(r.signal_strength))
        .field("power_kw", float(item.power_kw))
        .field("net_energy_kwh", float(item.net_energy_kwh))
        .field("low_battery", item.low_battery)
        .field("voltage_anomaly", item.voltage_anomaly)
        .time(r.timestamp, WritePrecision.MS)
    )


class InfluxTelemetryRepository:
    def __init__(self, url: str, token: str, org: str, bucket: str) -> None:
        self._client = InfluxDBClient(url=url, token=token, org=org)
        # Synchronous writes: the caller commits Kafka offsets only after
        # this returns, which is what gives the pipeline at-least-once
        # semantics end to end.
        self._write_api = self._client.write_api(write_options=SYNCHRONOUS)
        self._bucket = bucket
        self._org = org

    def save_batch(self, batch: list[EnrichedTelemetry]) -> None:
        if not batch:
            return
        self._write_api.write(
            bucket=self._bucket,
            org=self._org,
            record=[_to_point(item) for item in batch],
        )

    def close(self) -> None:
        self._client.close()
