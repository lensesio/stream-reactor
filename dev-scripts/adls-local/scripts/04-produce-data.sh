#!/usr/bin/env bash
# Produce 6 sample JSON records to adls-dev-topic (3 partitions).
#
# Records use distinct keys so Kafka's default (murmur2) partitioner spreads
# them across all 3 partitions:
#   keys user-1..user-6 — empirically distribute 2 per partition.
#
# The KCQL uses INSERT INTO <filesystem>:adls-dev with WITH_FLUSH_COUNT = 1,
# so each record results in exactly one JSON file written to ADLS — making
# verification straightforward (expect 6 files).

set -euo pipefail

KAFKA_CONTAINER="kafka-adls"
BOOTSTRAP="kafka:19092"
TOPIC="adls-dev-topic"
PRODUCER="/opt/kafka/bin/kafka-console-producer.sh"

echo "==> [04-produce-data] producing 6 records to '${TOPIC}' ..."

# kafka-console-producer reads key<TAB>value lines when parse.key=true.
# We pass a TAB via $'\t' expanded on the host before docker exec receives it.
TAB=$'\t'

printf '%s%s%s\n' \
  "user-1${TAB}" "" '{"id":1,"name":"alice","dept":"engineering","score":95}' \
  "user-2${TAB}" "" '{"id":2,"name":"bob","dept":"marketing","score":82}' \
  "user-3${TAB}" "" '{"id":3,"name":"carol","dept":"engineering","score":77}' \
  "user-4${TAB}" "" '{"id":4,"name":"dave","dept":"sales","score":91}' \
  "user-5${TAB}" "" '{"id":5,"name":"eve","dept":"marketing","score":68}' \
  "user-6${TAB}" "" '{"id":6,"name":"frank","dept":"sales","score":55}' \
| docker exec -i "${KAFKA_CONTAINER}" "${PRODUCER}" \
    --bootstrap-server "${BOOTSTRAP}" \
    --topic "${TOPIC}" \
    --property parse.key=true \
    --property "key.separator=${TAB}"

echo "==> [04-produce-data] 6 records produced to '${TOPIC}'."
echo "    The connector will write one JSON file per record to ADLS (WITH_FLUSH_COUNT = 1)."
echo "    Expect 6 files total across the 3 task directories."
