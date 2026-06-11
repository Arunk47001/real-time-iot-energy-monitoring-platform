#!/usr/bin/env bash
# End-to-end smoke test: checks every stage of the pipeline.
set -euo pipefail

# Stop Git Bash (MSYS) from rewriting /opt/... container paths into
# Windows paths; a no-op on Linux/macOS.
export MSYS_NO_PATHCONV=1

KAFKA_CONTAINER=iot-kafka
INFLUX_CONTAINER=iot-influxdb
TOPIC="${KAFKA_TOPIC:-iot-telemetry}"
BUCKET="${INFLUXDB_BUCKET:-iot-telemetry}"

pass() { echo "  [OK]   $1"; }
fail() { echo "  [FAIL] $1"; exit 1; }

echo "1/4 Container health"
for c in iot-kafka iot-influxdb iot-grafana iot-simulator iot-processor; do
  state=$(docker inspect -f '{{.State.Status}}' "$c" 2>/dev/null || echo "missing")
  [ "$state" = "running" ] && pass "$c running" || fail "$c is $state"
done

echo "2/4 Kafka topic receiving messages"
out=$(docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic "$TOPIC" \
  --max-messages 1 --timeout-ms 15000 2>/dev/null) || fail "no messages on $TOPIC"
echo "$out" | grep -q device_id && pass "telemetry flowing on $TOPIC"

echo "3/4 Consumer group active"
groups_out=$(docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 --describe --group iot-stream-processor 2>/dev/null) \
  || fail "could not describe consumer group"
echo "$groups_out" | grep -q "$TOPIC" && pass "iot-stream-processor consuming" \
  || fail "consumer group not active"

echo "4/4 InfluxDB receiving points (last 30s)"
count=$(docker exec "$INFLUX_CONTAINER" influx query \
  "from(bucket:\"$BUCKET\") |> range(start:-30s) \
   |> filter(fn:(r) => r._measurement == \"device_telemetry\" and r._field == \"battery_soc\") \
   |> group() |> count()" --raw 2>/dev/null | tail -2 | head -1 | awk -F',' '{print $NF}')
[ -n "${count:-}" ] && [ "${count:-0}" -gt 0 ] 2>/dev/null \
  && pass "$count points written in last 30s" \
  || fail "no recent points in bucket $BUCKET"

echo
echo "Pipeline healthy. Dashboard: http://localhost:3000 (admin/admin12345)"
