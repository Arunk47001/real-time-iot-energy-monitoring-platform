# Workflow — Setup, Daily Use, Cleanup

The operational runbook for this platform: every command you need, in the
order you need them. For *understanding* the system, see
[HOW_IT_WORKS.md](HOW_IT_WORKS.md).

---

## 1. Prerequisites (one time)

- **Docker Desktop** running (check: `docker info` exits without error)
- Nothing else — no local Python, Kafka or InfluxDB installs needed.
- Ports **3000** (Grafana), **8086** (InfluxDB) and **9092** (Kafka) must be
  free on your machine.

Optional: tweak [.env](../.env) before first start (device count, interval,
credentials). Defaults work fine.

---

## 2. Setup / Start

```bash
docker compose up -d --build
```

What this does, in dependency order:

| Step | Service | What happens |
|---|---|---|
| 1 | `kafka`, `influxdb` | start, then healthchecks must pass (~20-30s) |
| 2 | `kafka-init` | creates topic `iot-telemetry` (6 partitions), exits |
| 3 | `influxdb` first boot | creates org/bucket/token from `.env` |
| 4 | `grafana` | starts, auto-provisions datasource + dashboard |
| 5 | `simulator`, `processor` | built from `simulator/` and `processor/`, start streaming |

`--build` is only needed the first time or after you change Python code /
Dockerfiles. For a plain start of an already-built stack: `docker compose up -d`.

### Verify it's working

```bash
bash scripts/verify_pipeline.sh
```

Expect four `[OK]` stages and a final "Pipeline healthy." Then open:

| Service | URL | Login |
|---|---|---|
| Grafana dashboard | http://localhost:3000 → Dashboards → IoT Energy Monitoring | admin / admin12345 |
| InfluxDB UI | http://localhost:8086 | admin / admin12345 |

---

## 3. Daily use

```bash
docker compose ps                                # status of all containers
docker compose logs -f simulator processor      # tail the pipeline (Ctrl+C to stop)
docker compose logs --tail 20 processor         # recent processor activity
```

Consumer lag (is the processor keeping up? LAG should be ~0):

```bash
docker exec iot-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 --describe --group iot-stream-processor
```

> Git Bash users: run `export MSYS_NO_PATHCONV=1` first, or paths like
> `/opt/kafka/...` get rewritten to Windows paths.

### Pause / resume (keep all data)

```bash
docker compose stop      # stop everything, volumes untouched
docker compose start     # resume; processor catches up from its offset
```

Stop only the data flow but keep infrastructure up:

```bash
docker compose stop simulator processor
docker compose start simulator processor
```

### Apply a change

| You changed | Run |
|---|---|
| Python code in `simulator/` | `docker compose up -d --build simulator` |
| Python code in `processor/` | `docker compose up -d --build processor` |
| `.env` simulator knobs (DEVICE_COUNT, SEND_INTERVAL_SECONDS) | `docker compose up -d simulator` |
| Dashboard JSON in `grafana/provisioning/` | nothing — picked up within 30s |
| `docker-compose.yml` | `docker compose up -d` |
| `.env` InfluxDB credentials/org/bucket/token | full reset — see section 5 |

That last row is the one gotcha: InfluxDB reads its `DOCKER_INFLUXDB_INIT_*`
values **only on first boot with an empty volume**. Changing them later
requires wiping the volume (section 5), otherwise the processor and Grafana
will authenticate with values the database doesn't know.

---

## 4. Cleanup (keep data)

Stops and removes all containers and the network, but **keeps the volumes**
(Kafka log, InfluxDB history, Grafana state):

```bash
docker compose down
```

Next `docker compose up -d` recreates everything and your dashboard history
is still there. Use this for an ordinary end-of-day shutdown.

---

## 5. Full cleanup (wipe everything)

Removes containers, network **and all stored data** — Kafka messages,
InfluxDB history, any Grafana changes you made in the UI:

```bash
docker compose down -v
```

Use when you want a factory-fresh start, or after changing InfluxDB
credentials in `.env`. To also delete the built Python images and reclaim
all disk space:

```bash
docker compose down -v --rmi local
```

Rebuild from scratch afterwards:

```bash
docker compose up -d --build
```

### What survives what

| Action | Containers | Data (volumes) | Built images |
|---|---|---|---|
| `docker compose stop` | kept (stopped) | kept | kept |
| `docker compose down` | removed | **kept** | kept |
| `docker compose down -v` | removed | **removed** | kept |
| `docker compose down -v --rmi local` | removed | removed | removed |

---

## 6. Troubleshooting quick reference

| Symptom | Check | Likely fix |
|---|---|---|
| A container keeps restarting | `docker logs <name>` | read the error; often a dependency wasn't healthy yet — `docker compose up -d` again |
| `verify_pipeline.sh` fails at stage 2 | `docker logs iot-simulator` | simulator can't reach Kafka; restart it |
| Stage 4 fails / Grafana empty but Kafka flowing | `docker logs iot-processor` | `401 unauthorized` → token mismatch → section 5 full reset |
| Grafana "No data" but InfluxDB has points | Connections → Data sources → InfluxDB-IoT → Test | datasource token mismatch → section 5 full reset |
| Port already in use on startup | `netstat -ano \| findstr :3000` (PowerShell) | stop the conflicting app or change the host port in `docker-compose.yml` |
| Lag keeps growing | processor logs for slow/failed writes | restart processor; check InfluxDB health |

Container names: `iot-kafka`, `iot-influxdb`, `iot-grafana`,
`iot-simulator`, `iot-processor`.
