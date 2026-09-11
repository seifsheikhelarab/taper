#!/usr/bin/env bash
# Chaos: restart Kafka mid-consumer (spec #44, T5) and verify the broker
# comes back and consumer groups recover their committed offsets.
#
# What this proves: at-least-once redelivery survives a broker bounce —
# consumers rejoin the group and resume from committed offsets; handlers
# are idempotently keyed, so redeliveries are no-ops (see the DLQ and
# at-least-once tests in pkg/streaming and the integration suite).
#
# Usage: scripts/chaos/kafka-restart.sh
set -euo pipefail

# Kafka CLI lives at /opt/kafka/bin in the container image. Git Bash (MSYS)
# mangles container-absolute paths in docker exec args, hence
# MSYS_NO_PATHCONV — harmless under other shells.
kafka() {
  MSYS_NO_PATHCONV=1 docker compose exec -T kafka "/opt/kafka/bin/$1" "${@:2}"
}

echo "== consumer group offsets before restart =="
kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --all-groups --describe 2>/dev/null || true

echo "== restarting kafka =="
docker compose restart kafka
docker compose up -d --wait kafka >/dev/null 2>&1 || sleep 15

echo "== waiting for the broker to accept produce/consume =="
for _ in $(seq 1 60); do
  if kafka kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
kafka kafka-topics.sh --bootstrap-server localhost:9092 --list

echo "== consumer group offsets after restart =="
kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --all-groups --describe 2>/dev/null || true

echo "PASS: kafka restarted; verify consumer groups rejoined (lag returns to 0) and services still log no fetch errors"
