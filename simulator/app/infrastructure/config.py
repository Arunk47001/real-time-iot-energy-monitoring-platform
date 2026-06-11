"""Environment-driven configuration (infrastructure concern)."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str
    kafka_topic: str
    device_count: int
    site_count: int
    send_interval_seconds: float

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            kafka_bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            kafka_topic=os.environ.get("KAFKA_TOPIC", "iot-telemetry"),
            device_count=int(os.environ.get("DEVICE_COUNT", "1000")),
            site_count=int(os.environ.get("SITE_COUNT", "20")),
            send_interval_seconds=float(os.environ.get("SEND_INTERVAL_SECONDS", "5")),
        )
