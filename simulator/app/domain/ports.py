"""Outbound ports. The application layer depends on these abstractions;
infrastructure provides the concrete adapters (dependency inversion)."""

from typing import Protocol

from app.domain.models import TelemetryReading


class TelemetryPublisher(Protocol):
    def publish(self, reading: TelemetryReading) -> None:
        """Asynchronously enqueue a reading for delivery."""
        ...

    def flush(self) -> None:
        """Block until all enqueued readings are delivered."""
        ...

    def close(self) -> None:
        ...
