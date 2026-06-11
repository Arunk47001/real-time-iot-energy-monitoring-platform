"""Kafka adapter implementing the TelemetrySource port.

Parsing/deserialization failures are an infrastructure concern: malformed
messages are logged and skipped here so the domain only ever sees
well-formed TelemetryReading objects.
"""

import json
import logging
import time
from datetime import datetime

from confluent_kafka import Consumer, KafkaError

from app.domain.models import TelemetryReading

logger = logging.getLogger(__name__)


def _parse(payload: bytes) -> TelemetryReading:
    record = json.loads(payload)
    return TelemetryReading(
        device_id=str(record["device_id"]),
        timestamp=datetime.fromisoformat(record["timestamp"].replace("Z", "+00:00")),
        site_id=str(record["site_id"]),
        battery_soc=float(record["battery_soc"]),
        solar_kwh=float(record["solar_kwh"]),
        energy_kwh=float(record["energy_kwh"]),
        voltage=float(record["voltage"]),
        current=float(record["current"]),
        temperature=float(record["temperature"]),
        signal_strength=int(record["signal_strength"]),
    )


class KafkaTelemetrySource:
    def __init__(self, bootstrap_servers: str, topic: str, group_id: str) -> None:
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "client.id": "iot-stream-processor",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self._consumer.subscribe([topic])

    def poll_batch(self, max_records: int, timeout_seconds: float) -> list[TelemetryReading]:
        readings: list[TelemetryReading] = []
        deadline = time.monotonic() + timeout_seconds

        while len(readings) < max_records:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            msg = self._consumer.poll(timeout=remaining)
            if msg is None:
                break
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("Consumer error: %s", msg.error())
                continue
            try:
                readings.append(_parse(msg.value()))
            except (ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
                logger.warning("Skipping malformed message: %s", exc)

        return readings

    def commit(self) -> None:
        self._consumer.commit(asynchronous=False)

    def close(self) -> None:
        self._consumer.close()
