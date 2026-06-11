"""Kafka adapter implementing the TelemetryPublisher port."""

import json
import logging
from dataclasses import asdict
from datetime import datetime

from confluent_kafka import KafkaError, Message, Producer

from app.domain.models import TelemetryReading

logger = logging.getLogger(__name__)


def _serialize(reading: TelemetryReading) -> bytes:
    record = asdict(reading)
    record["timestamp"] = reading.timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return json.dumps(record).encode("utf-8")


class KafkaTelemetryPublisher:
    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self._topic = topic
        self._delivery_errors = 0
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "iot-device-simulator",
                "acks": "1",
                "linger.ms": 50,
                "compression.type": "lz4",
            }
        )

    def publish(self, reading: TelemetryReading) -> None:
        # Keyed by device_id so each device's readings stay ordered
        # within a single partition.
        self._producer.produce(
            topic=self._topic,
            key=reading.device_id.encode("utf-8"),
            value=_serialize(reading),
            on_delivery=self._on_delivery,
        )
        self._producer.poll(0)  # serve delivery callbacks

    def flush(self) -> None:
        self._producer.flush(timeout=30)
        if self._delivery_errors:
            logger.warning("%d deliveries failed in last tick", self._delivery_errors)
            self._delivery_errors = 0

    def close(self) -> None:
        self._producer.flush(timeout=30)

    def _on_delivery(self, err: KafkaError | None, _msg: Message) -> None:
        if err is not None:
            self._delivery_errors += 1
