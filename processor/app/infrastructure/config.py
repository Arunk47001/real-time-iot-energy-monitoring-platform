"""Environment-driven configuration (infrastructure concern)."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_consumer_group: str
    influxdb_url: str
    influxdb_org: str
    influxdb_bucket: str
    influxdb_token: str

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            kafka_bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            kafka_topic=os.environ.get("KAFKA_TOPIC", "iot-telemetry"),
            kafka_consumer_group=os.environ.get("KAFKA_CONSUMER_GROUP", "iot-stream-processor"),
            influxdb_url=os.environ.get("INFLUXDB_URL", "http://localhost:8086"),
            influxdb_org=os.environ["INFLUXDB_ORG"],
            influxdb_bucket=os.environ["INFLUXDB_BUCKET"],
            influxdb_token=os.environ["INFLUXDB_TOKEN"],
        )
