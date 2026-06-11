"""Composition root: wires infrastructure adapters into the use case."""

import logging
import signal
import threading

from app.application.stream_processor import StreamProcessingService
from app.infrastructure.config import Settings
from app.infrastructure.influx_repository import InfluxTelemetryRepository
from app.infrastructure.kafka_source import KafkaTelemetrySource


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    settings = Settings.from_env()

    stop_event = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop_event.set())

    source = KafkaTelemetrySource(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=settings.kafka_topic,
        group_id=settings.kafka_consumer_group,
    )
    repository = InfluxTelemetryRepository(
        url=settings.influxdb_url,
        token=settings.influxdb_token,
        org=settings.influxdb_org,
        bucket=settings.influxdb_bucket,
    )
    StreamProcessingService(source, repository).run(stop_event)


if __name__ == "__main__":
    main()
