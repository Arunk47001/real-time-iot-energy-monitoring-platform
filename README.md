# Real-Time IoT Energy Monitoring Platform

A fully local real-time streaming analytics platform. 1000 simulated solar +
battery IoT devices publish telemetry to Kafka every 5 seconds; a Python
stream processor validates and enriches the readings and persists them to
InfluxDB; Grafana visualizes everything on an auto-provisioned dashboard.

No cloud services - everything runs in Docker Compose.

> New to Kafka/InfluxDB/Grafana? Read
> [docs/HOW_IT_WORKS.md](docs/HOW_IT_WORKS.md) - a from-scratch walkthrough
> of every concept in this pipeline, following one message end to end.
>
> For day-to-day commands (start, verify, apply changes, cleanup), see
> [docs/WORKFLOW.md](docs/WORKFLOW.md).

## Architecture

```
+----------------------+
| Device Simulator     |   simulator/  (Python producer, 1000 devices)
+----------+-----------+
           |
           v
+----------------------+
| Kafka Topic          |   iot-telemetry (6 partitions, KRaft single node)
+----------+-----------+
           |
           v
+----------------------+
| Stream Processor     |   processor/  (Python consumer: validate -> enrich -> store)
+----------+-----------+
           |
           v
+----------------------+
| InfluxDB 2.x         |   bucket: iot-telemetry, measurement: device_telemetry
+----------+-----------+
           |
           v
+----------------------+
| Grafana Dashboard    |   http://localhost:3000 (auto-provisioned)
+----------------------+
```

## Quick start

```bash
docker compose up -d --build
```

Wait ~30s for Kafka and InfluxDB to become healthy, then open:

| Service  | URL                    | Credentials       |
|----------|------------------------|-------------------|
| Grafana  | http://localhost:3000  | admin / admin12345 |
| InfluxDB | http://localhost:8086  | admin / admin12345 |
| Kafka    | localhost:9092 (host)  | -                 |

The **IoT Energy Monitoring** dashboard is provisioned automatically and
refreshes every 5 seconds.

Verify the pipeline end to end:

```bash
./scripts/verify_pipeline.sh
```

Tear down (add `-v` to also wipe stored data):

```bash
docker compose down -v
```

## Telemetry record

Each device emits one record per interval:

```json
{
  "device_id": "SL001",
  "timestamp": "2026-06-11T10:00:00.000Z",
  "site_id": "SITE01",
  "battery_soc": 84.2,
  "solar_kwh": 2.8,
  "energy_kwh": 4.1,
  "voltage": 230.4,
  "current": 1.8,
  "temperature": 32.1,
  "signal_strength": -68
}
```

The simulation is stateful and physically plausible: solar output follows a
diurnal curve, household load peaks in the morning and evening, and each
device's battery SOC integrates the difference.

The stream processor rejects implausible readings and adds derived fields
before writing to InfluxDB:

| Field             | Meaning                                  |
|-------------------|------------------------------------------|
| `power_kw`        | instantaneous power (V x I / 1000)        |
| `net_energy_kwh`  | solar generation minus consumption        |
| `low_battery`     | SOC below 20%                             |
| `voltage_anomaly` | voltage outside 230V +/-10% (207-253V)    |

InfluxDB schema: measurement `device_telemetry`, tags `device_id` + `site_id`,
all metrics as fields.

## Project structure

Both Python services follow clean architecture - the domain knows nothing
about Kafka or InfluxDB; adapters implement domain-defined ports and are
wired together in `main.py`:

```
simulator/                      # Kafka producer
  app/
    domain/                     # entities + simulation physics + ports
      models.py                 #   TelemetryReading
      device.py                 #   SimulatedDevice (pure logic, no I/O)
      ports.py                  #   TelemetryPublisher protocol
    application/
      simulation.py             # FleetSimulationService use case
    infrastructure/
      kafka_publisher.py        # confluent-kafka adapter
      config.py                 # env-based settings
    main.py                     # composition root

processor/                      # Kafka consumer -> InfluxDB
  app/
    domain/
      models.py                 #   TelemetryReading, EnrichedTelemetry
      processing.py             #   validation + enrichment rules (pure)
      ports.py                  #   TelemetrySource, TelemetryRepository
    application/
      stream_processor.py       # StreamProcessingService use case
    infrastructure/
      kafka_source.py           # consumer adapter (parses, skips malformed)
      influx_repository.py      # InfluxDB write adapter
      config.py
    main.py

grafana/provisioning/           # datasource + dashboard auto-provisioning
docker-compose.yml              # kafka, kafka-init, influxdb, grafana, services
.env                            # local-dev configuration and credentials
```

## Delivery semantics

- Producer keys messages by `device_id`, so each device's readings stay
  ordered within one partition.
- The processor uses manual offset commits: offsets are committed only after
  a successful InfluxDB write, giving **at-least-once** delivery. Replays are
  harmless because InfluxDB overwrites points with identical
  measurement/tags/timestamp.

## Configuration

All knobs live in [.env](.env):

| Variable                | Default        | Description                    |
|-------------------------|----------------|--------------------------------|
| `DEVICE_COUNT`          | 1000           | simulated devices              |
| `SITE_COUNT`            | 20             | sites devices are spread over  |
| `SEND_INTERVAL_SECONDS` | 5              | reporting interval per device  |
| `KAFKA_TOPIC`           | iot-telemetry  | telemetry topic                |
| `INFLUXDB_*`            | dev defaults   | org, bucket, token, login      |
| `GRAFANA_*`             | admin/admin12345 | dashboard login              |

> The committed `.env` contains development-only credentials for local use.

## Useful commands

```bash
# Tail the pipeline
docker compose logs -f simulator processor

# Watch raw messages on the topic
docker exec -it iot-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic iot-telemetry --max-messages 5

# Consumer group lag
docker exec -it iot-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 --describe --group iot-stream-processor

# Query InfluxDB directly
docker exec -it iot-influxdb influx query \
  'from(bucket:"iot-telemetry") |> range(start:-1m) |> limit(n:5)'
```
