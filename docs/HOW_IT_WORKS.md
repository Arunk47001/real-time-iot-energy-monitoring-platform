# How This Platform Works — A Beginner's Guide

This guide assumes you know Python, SQL, Docker and bash — but **zero Kafka,
InfluxDB or Grafana**. It follows one telemetry message through the entire
pipeline, explaining every concept along the way using the actual code in
this repo.

```
Simulator ──> Kafka ──> Processor ──> InfluxDB ──> Grafana
(producer)   (queue)   (consumer)    (database)   (charts)
```

---

## Part 1: Why do we even need Kafka?

Imagine 1000 devices all sending data every 5 seconds. The naive approach:
each device writes directly to the database.

```
1000 devices ──────────────> database   (BAD)
```

Problems:

1. **The database becomes the bottleneck.** 200 writes/second of tiny
   individual inserts is wasteful. If the DB is slow or down, data is lost.
2. **Tight coupling.** Every device needs to know the DB address, schema,
   credentials. Change the DB → change 1000 devices.
3. **One consumer only.** Tomorrow you want the same data to also feed an
   alerting system and an ML model. Now each device sends everything 3 times?

Kafka solves this by sitting in the middle as a **durable buffer**:

```
1000 devices ──> Kafka ──> processor ──> database
                       └──> (future: alerting)
                       └──> (future: ML pipeline)
```

- Devices fire-and-forget into Kafka. Fast, even if the DB is down.
- Kafka **stores** messages on disk (24h in our setup), so a crashed
  consumer can restart and catch up — nothing is lost.
- Any number of independent consumers can read the same stream.

> **Mental model:** Kafka is not a database and not a message queue that
> deletes on read. It's an **append-only log file** that many writers append
> to and many readers read from, each keeping their own bookmark.

---

## Part 2: Kafka vocabulary (the 6 words you need)

| Term | What it is | In this project |
|---|---|---|
| **Broker** | The Kafka server process | 1 container: `iot-kafka` |
| **Topic** | A named stream, like a table name | `iot-telemetry` |
| **Partition** | A topic is split into N independent logs for parallelism | 6 partitions |
| **Offset** | Position of a message within a partition (0, 1, 2, ...) | tracked per partition |
| **Producer** | A client that appends messages | the simulator |
| **Consumer group** | A named set of consumers sharing the work, with a saved bookmark | `iot-stream-processor` |

### Topics and partitions, visually

The topic `iot-telemetry` is physically 6 separate append-only logs:

```
partition 0:  [msg][msg][msg][msg] ──> new messages appended here
partition 1:  [msg][msg][msg]
partition 2:  [msg][msg][msg][msg][msg]
partition 3:  [msg][msg]
partition 4:  [msg][msg][msg]
partition 5:  [msg][msg][msg][msg]
```

Why split? **Parallelism.** One consumer can read all 6, or you can run 6
consumers and each takes one partition. Partitions are the unit of scaling
in Kafka.

Which partition does a message go to? By the **message key**. Same key →
always the same partition. We key by `device_id`
([kafka_publisher.py](../simulator/app/infrastructure/kafka_publisher.py)):

```python
self._producer.produce(
    topic=self._topic,
    key=reading.device_id.encode("utf-8"),   # <-- "SL001"
    value=_serialize(reading),               # <-- the JSON payload
)
```

So all of SL001's readings land in one partition **in order**. Ordering is
only guaranteed *within* a partition — keying by device gives each device a
strictly ordered history.

### Offsets: the consumer's bookmark

Kafka never "delivers" messages — consumers **pull** and track how far
they've read:

```
partition 2:  [0][1][2][3][4][5][6][7][8]
                           ▲           ▲
                committed offset    newest message
                (our bookmark: 4)   (log end: 8)
                LAG = 8 - 4 = 4 messages behind
```

The consumer group `iot-stream-processor` stores its bookmark **inside
Kafka itself**. Restart the processor → it resumes from the bookmark. That's
why no data is lost when the consumer crashes. You saw this when we ran:

```bash
docker exec iot-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 --describe --group iot-stream-processor
```

`CURRENT-OFFSET` = bookmark, `LOG-END-OFFSET` = newest, `LAG` = how far
behind. LAG ≈ 0 means the processor keeps up in real time.

---

## Part 3: Producing — how data gets INTO Kafka

Every 5 seconds the simulator loops over 1000 device objects, asks each for
a reading, and publishes it
([simulation.py](../simulator/app/application/simulation.py)):

```python
for device in self._devices:
    self._publisher.publish(device.take_reading(now, self._interval))
self._publisher.flush()
```

Three things worth understanding in the publisher:

**1. The payload is just bytes.** Kafka doesn't know or care about JSON — it
stores opaque bytes. *We* choose to serialize our dataclass to JSON:

```python
json.dumps(record).encode("utf-8")     # dict -> JSON string -> bytes
```

**2. `produce()` doesn't send — it enqueues.** The Kafka client batches
messages in a local buffer and ships them in the background (that's what
`linger.ms: 50` means: wait up to 50ms to fill a bigger batch). This is why
sending 1000 messages takes only 0.04s in the logs.

**3. `flush()` waits until all are actually delivered.** We call it once per
tick so a tick is "done" only when Kafka has acknowledged everything.

```
publish() x1000 ──> [local buffer] ──batches──> broker writes to disk
                                       ▲
flush() blocks until this completes ───┘
```

---

## Part 4: Consuming — how data gets OUT of Kafka

The processor does this loop forever
([stream_processor.py](../processor/app/application/stream_processor.py)):

```python
readings = self._source.poll_batch(...)        # 1. pull a batch from Kafka
valid    = [r for r in readings if is_plausible(r)]  # 2. validate
self._repository.save_batch([enrich(r) for r in valid])  # 3. enrich + write to InfluxDB
self._source.commit()                          # 4. move the bookmark
```

**The order of steps 3 and 4 is the most important design decision in the
whole pipeline.** We write to InfluxDB *first*, and only then commit the
offset. Walk through the failure case:

```
✓ poll 1000 messages       (bookmark still at old position)
✓ write 1000 to InfluxDB
✗ CRASH before commit
─── restart ───
✓ poll the SAME 1000 messages again (bookmark never moved!)
✓ write them to InfluxDB again      (harmless — see below)
✓ commit
```

This is called **at-least-once delivery**: a message may be processed twice
but never zero times. The duplicate write is harmless because InfluxDB
*overwrites* a point that has the same measurement + tags + timestamp —
writing the same reading twice is a no-op. (The opposite order — commit
first, write second — would be "at-most-once": crash between the two and
data is silently lost.)

This is also why the consumer config has `"enable.auto.commit": False`
([kafka_source.py](../processor/app/infrastructure/kafka_source.py)) — we
take manual control of the bookmark.

**Enrichment** happens in pure Python before storage
([processing.py](../processor/app/domain/processing.py)): compute
`power_kw = V × I / 1000`, `net_energy_kwh = solar − consumption`, and flag
`low_battery` / `voltage_anomaly`. Doing this once in the stream means
Grafana never has to compute it per query.

---

## Part 5: InfluxDB — how the data is stored

InfluxDB is a **time-series database**: optimized for "lots of timestamped
measurements, queried by time range." Coming from SQL, here's the mapping:

| SQL concept | InfluxDB concept | Here |
|---|---|---|
| database | **bucket** | `iot-telemetry` |
| table | **measurement** | `device_telemetry` |
| indexed column | **tag** (always a string, indexed) | `device_id`, `site_id` |
| regular column | **field** (numbers/bools, NOT indexed) | `battery_soc`, `voltage`, ... |
| primary key | measurement + tags + **timestamp** | |

One reading becomes one **point**
([influx_repository.py](../processor/app/infrastructure/influx_repository.py)):

```python
Point("device_telemetry")
    .tag("device_id", "SL001")        # indexed -> fast "WHERE device_id = ..."
    .tag("site_id", "SITE01")
    .field("battery_soc", 84.2)       # the actual data
    .field("voltage", 230.4)
    ...
    .time(timestamp)                  # part of the primary key
```

**Why tags vs fields matters:** queries that filter/group by tags are fast
(indexed); fields are just payload. Rule of thumb: things you `GROUP BY` or
`WHERE` on → tags. Things you plot or aggregate → fields. Never make a
high-uniqueness value like a timestamp or raw reading a tag — each unique
tag combination creates a new internal series, and millions of series kill
performance. Our 1000 devices × 20 sites is tiny and safe.

### Querying: Flux instead of SQL

InfluxDB 2.x uses **Flux**, a pipeline language. Data flows top to bottom
through `|>` (pipe), like bash pipes:

```
from(bucket: "iot-telemetry")                          -- FROM
  |> range(start: -5m)                                 -- WHERE time > now()-5m
  |> filter(fn: (r) => r._field == "battery_soc")      -- WHERE / SELECT column
  |> group(columns: ["site_id"])                       -- GROUP BY site_id
  |> aggregateWindow(every: 10s, fn: mean)             -- AVG per 10s bucket
```

Rough SQL equivalent:

```sql
SELECT site_id, time_bucket('10s', time) AS t, AVG(battery_soc)
FROM device_telemetry
WHERE time > now() - interval '5 minutes'
GROUP BY site_id, t;
```

`range()` is mandatory in Flux — you can never query "all time" by accident.
Try queries yourself in the InfluxDB UI: http://localhost:8086 → Data
Explorer → switch to "Script Editor".

---

## Part 6: Grafana — how the charts work

Grafana stores **no data**. It's a query-and-render layer: every panel holds
a Flux query, and on each refresh (every 5s here) Grafana sends the query to
InfluxDB and draws the result.

```
browser ──> Grafana ──Flux query──> InfluxDB
   ▲                                    │
   └────────── chart ◀── rows ──────────┘
```

Two pieces of configuration make this work, both auto-provisioned from
files (no clicking required):

**1. The datasource** — "where is InfluxDB and how do I authenticate"
([grafana/provisioning/datasources/influxdb.yml](../grafana/provisioning/datasources/influxdb.yml)):

```yaml
url: http://influxdb:8086     # container DNS name, see Part 7
jsonData:
  version: Flux
  organization: $INFLUXDB_ORG
secureJsonData:
  token: $INFLUXDB_TOKEN      # same token the processor uses to write
```

**2. The dashboard** — a JSON file where every panel has a Flux query inside
([iot-energy-monitoring.json](../grafana/provisioning/dashboards/iot-energy-monitoring.json)).
For example, the "Battery SOC by Site" panel contains exactly the query from
Part 5, except with Grafana variables substituted at runtime:

- `v.timeRangeStart / v.timeRangeStop` → whatever time range you picked in
  the top-right corner
- `v.windowPeriod` → an aggregation interval Grafana chooses to roughly
  match your screen width (zoom out → bigger windows, automatically)

To see this live: open any panel → **Edit** → the Flux query is right there.
Change it, watch the chart change.

---

## Part 7: How Docker Compose ties it together

Compose runs each component as a container on one shared private network.
**Container names are DNS names** on that network — that's the magic that
makes `kafka:29092` and `http://influxdb:8086` work as addresses.

```
your browser ── localhost:3000 ──> [grafana] ──influxdb:8086──> [influxdb]
your browser ── localhost:8086 ──────────────────────────────────────▲
                                                                     │
                  [simulator] ──kafka:29092──> [kafka] <──kafka:29092── [processor]
                                                            (processor also writes
                                                             to influxdb:8086)
```

Startup ordering is enforced with **healthchecks** in
[docker-compose.yml](../docker-compose.yml): the simulator and processor
declare `depends_on: kafka: condition: service_healthy`, so they don't start
until Kafka actually answers requests — not just until its container exists.
A one-shot `kafka-init` container creates the topic with exactly 6
partitions, then exits.

Kafka has two listeners because of networking: containers reach it at
`kafka:29092`; if you ever run a script on your laptop directly, use
`localhost:9092`. (Inside vs outside the Docker network are different
worlds, and Kafka must advertise a reachable address for each.)

---

## Part 8: The life of one message, end to end

Tying it all together — follow one reading from SL001 at 10:00:00:

```
1. simulator/domain/device.py      SimulatedDevice computes physics:
                                   solar curve, load, battery SOC
                                        │ TelemetryReading (Python dataclass)
2. simulator/infrastructure/       serialize to JSON bytes,
   kafka_publisher.py              key = "SL001"
                                        │ produce() -> buffered -> batched
3. KAFKA broker                    hash("SL001") -> partition 3,
                                   appended at offset 10788, saved to disk
                                        │ pulled by consumer poll()
4. processor/infrastructure/       bytes -> JSON -> TelemetryReading
   kafka_source.py                 (malformed messages skipped here)
                                        │
5. processor/domain/processing.py  is_plausible()?  -> enrich():
                                   power_kw, net_energy_kwh, flags
                                        │ EnrichedTelemetry
6. processor/infrastructure/       -> InfluxDB Point, batched write
   influx_repository.py            THEN commit offset 10788 (bookmark moves)
                                        │ stored: measurement=device_telemetry
                                        │         tags={SL001, SITE01} t=10:00:00
7. GRAFANA (every 5s)              panel sends Flux query to InfluxDB,
                                   gets aggregated rows, redraws the chart
```

Total latency from "device reading taken" to "visible on dashboard" is a
few seconds, dominated by the 5s panel refresh interval.

---

## Part 9: Experiments to build intuition

The best way to learn this is to break it. All safe — `docker compose up -d`
restores everything.

**1. Watch raw messages flow through Kafka:**
```bash
docker exec iot-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic iot-telemetry --max-messages 5
```
(in Git Bash run `export MSYS_NO_PATHCONV=1` first)

**2. Kill the consumer, watch lag grow, watch it catch up:**
```bash
docker stop iot-processor
# wait a minute, then look at the LAG column:
docker exec iot-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 --describe --group iot-stream-processor
docker start iot-processor      # lag drains back to 0 - nothing was lost
```
This is the single most important Kafka behavior: **the buffer absorbs
downstream failure.**

**3. Kill InfluxDB, prove the same thing end to end:**
```bash
docker stop iot-influxdb        # processor starts failing to write
docker start iot-influxdb       # it recovers; check the Grafana chart for a gap
```

**4. Scale the consumers** — Kafka shares the 6 partitions automatically:
```bash
docker compose up -d --scale processor=3 --no-recreate
docker exec iot-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 --describe --group iot-stream-processor
# CONSUMER-ID column now shows 3 consumers, each owning 2 partitions
```

**5. Crank up the load** — edit `.env`, set `DEVICE_COUNT=5000` and
`SEND_INTERVAL_SECONDS=2`, then `docker compose up -d --build simulator`.
Watch the Ingestion Rate panel jump.

---

## Glossary

| Term | One-liner |
|---|---|
| Broker | A Kafka server; stores partitions on disk |
| Topic | Named message stream (`iot-telemetry`) |
| Partition | One append-only log within a topic; the unit of parallelism |
| Offset | A message's position in a partition; consumers commit these as bookmarks |
| Consumer group | Named team of consumers splitting partitions among themselves |
| Lag | Log-end offset minus committed offset; how far behind a consumer is |
| At-least-once | Delivery guarantee: maybe duplicates, never silent loss |
| KRaft | Modern Kafka mode with no ZooKeeper dependency (what we run) |
| Bucket | InfluxDB's "database" |
| Measurement | InfluxDB's "table" |
| Tag / Field | Indexed string metadata / unindexed data values |
| Point | One row: measurement + tags + fields + timestamp |
| Flux | InfluxDB 2.x query language (pipeline style, `|>`) |
| Datasource | Grafana's connection config for a database |
| Provisioning | Grafana auto-config from files instead of clicking in the UI |
