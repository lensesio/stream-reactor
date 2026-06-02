#!/usr/bin/env bash
# Create the Kafka topic consumed by the adls connector (3 partitions).
# Idempotent: uses --if-not-exists.

set -euo pipefail

TOPIC="adls-dev-topic"
BOOTSTRAP="kafka:19092"
KAFKA_CONTAINER="kafka-adls"
KAFKA_TOPICS_SH="/opt/kafka/bin/kafka-topics.sh"

echo "==> [02-create-topics] waiting for Kafka broker to be ready ..."
for i in {1..20}; do
  if docker exec "${KAFKA_CONTAINER}" "${KAFKA_TOPICS_SH}" \
      --bootstrap-server "${BOOTSTRAP}" --list &>/dev/null; then
    break
  fi
  echo "    attempt ${i}/20 — not ready yet, sleeping 5 s ..."
  sleep 5
done

echo "==> [02-create-topics] creating topic: ${TOPIC} (3 partitions)"
docker exec "${KAFKA_CONTAINER}" "${KAFKA_TOPICS_SH}" \
  --bootstrap-server "${BOOTSTRAP}" \
  --create \
  --if-not-exists \
  --topic "${TOPIC}" \
  --partitions 3 \
  --replication-factor 1

echo "==> [02-create-topics] listing topics:"
docker exec "${KAFKA_CONTAINER}" "${KAFKA_TOPICS_SH}" \
  --bootstrap-server "${BOOTSTRAP}" --list
