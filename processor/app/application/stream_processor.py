"""Use case: consume telemetry, validate, enrich, persist.

Offsets are committed only after a successful write, giving at-least-once
delivery into InfluxDB (idempotent there: same measurement/tags/timestamp
overwrites itself).
"""

import logging
import threading

from app.domain.ports import TelemetryRepository, TelemetrySource
from app.domain.processing import enrich, is_plausible

logger = logging.getLogger(__name__)


class StreamProcessingService:
    def __init__(
        self,
        source: TelemetrySource,
        repository: TelemetryRepository,
        batch_size: int = 2000,
        poll_timeout_seconds: float = 1.0,
    ) -> None:
        self._source = source
        self._repository = repository
        self._batch_size = batch_size
        self._poll_timeout = poll_timeout_seconds

    def run(self, stop_event: threading.Event) -> None:
        logger.info("Stream processor started")
        processed_total = 0
        rejected_total = 0

        while not stop_event.is_set():
            readings = self._source.poll_batch(self._batch_size, self._poll_timeout)
            if not readings:
                continue

            valid = [r for r in readings if is_plausible(r)]
            rejected = len(readings) - len(valid)
            if rejected:
                rejected_total += rejected
                logger.warning("Rejected %d implausible readings", rejected)

            self._repository.save_batch([enrich(r) for r in valid])
            self._source.commit()

            processed_total += len(valid)
            logger.info(
                "Stored %d readings (total=%d, rejected=%d)",
                len(valid), processed_total, rejected_total,
            )

        self._source.close()
        self._repository.close()
        logger.info("Stream processor stopped")
