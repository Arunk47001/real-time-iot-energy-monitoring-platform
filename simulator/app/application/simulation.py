"""Use case: run a fleet of simulated devices on a fixed reporting interval."""

import logging
import random
import threading
import time
from datetime import datetime, timezone

from app.domain.device import SimulatedDevice
from app.domain.ports import TelemetryPublisher

logger = logging.getLogger(__name__)


def build_fleet(device_count: int, site_count: int, seed: int = 42) -> list[SimulatedDevice]:
    """Devices SL001..SLnnn spread evenly across sites SITE01..SITEnn."""
    rng = random.Random(seed)
    return [
        SimulatedDevice(
            device_id=f"SL{i:03d}",
            site_id=f"SITE{(i - 1) % site_count + 1:02d}",
            rng=random.Random(rng.random()),
        )
        for i in range(1, device_count + 1)
    ]


class FleetSimulationService:
    def __init__(
        self,
        devices: list[SimulatedDevice],
        publisher: TelemetryPublisher,
        interval_seconds: float,
    ) -> None:
        self._devices = devices
        self._publisher = publisher
        self._interval = interval_seconds

    def run(self, stop_event: threading.Event) -> None:
        logger.info(
            "Simulating %d devices, reporting every %.1fs",
            len(self._devices), self._interval,
        )
        while not stop_event.is_set():
            tick_started = time.monotonic()
            now = datetime.now(timezone.utc)

            for device in self._devices:
                self._publisher.publish(device.take_reading(now, self._interval))
            self._publisher.flush()

            elapsed = time.monotonic() - tick_started
            logger.info("Published %d readings in %.2fs", len(self._devices), elapsed)
            stop_event.wait(max(0.0, self._interval - elapsed))

        self._publisher.close()
        logger.info("Simulation stopped")
