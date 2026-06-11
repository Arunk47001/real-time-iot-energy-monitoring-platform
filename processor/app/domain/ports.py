"""Inbound/outbound ports for the stream-processing use case."""

from typing import Protocol

from app.domain.models import EnrichedTelemetry, TelemetryReading


class TelemetrySource(Protocol):
    def poll_batch(self, max_records: int, timeout_seconds: float) -> list[TelemetryReading]:
        """Return up to max_records parsed readings (may be empty)."""
        ...

    def commit(self) -> None:
        """Acknowledge everything returned by poll_batch so far."""
        ...

    def close(self) -> None:
        ...


class TelemetryRepository(Protocol):
    def save_batch(self, batch: list[EnrichedTelemetry]) -> None:
        ...

    def close(self) -> None:
        ...
