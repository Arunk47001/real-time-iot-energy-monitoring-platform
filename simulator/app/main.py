"""Composition root: wires infrastructure adapters into the use case."""

import logging
import signal
import threading

from app.application.simulation import FleetSimulationService, build_fleet
from app.infrastructure.config import Settings
from app.infrastructure.kafka_publisher import KafkaTelemetryPublisher


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    settings = Settings.from_env()

    stop_event = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop_event.set())

    publisher = KafkaTelemetryPublisher(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=settings.kafka_topic,
    )
    service = FleetSimulationService(
        devices=build_fleet(settings.device_count, settings.site_count),
        publisher=publisher,
        interval_seconds=settings.send_interval_seconds,
    )
    service.run(stop_event)


if __name__ == "__main__":
    main()
