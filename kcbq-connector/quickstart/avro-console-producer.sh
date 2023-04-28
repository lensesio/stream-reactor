#! /usr/bin/env bash
#
# Copyright 2020 Confluent, Inc.
#
# This software contains code derived from the WePay BigQuery Kafka Connector, Copyright WePay, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

BASE_DIR=`dirname "$0"`

if [ -z $CONFLUENT_DIR ]; then
  CONFLUENT_DIR="$BASE_DIR/../../confluent-3.0.0"
fi

KAFKA_TOPIC='kcbq-quickstart'
AVRO_SCHEMA='{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}'
REGISTRY_URL='http://localhost:8081'
BROKER_LIST='localhost:9092'

usage() {
  echo -e "usage:\n" \
          "    [-t|--topic <kafka_topic>]               Name of the kafka topic to write to\n" \
          "    [-s|--schema <avro_schema>]              Avro schema definition\n" \
          "    [-f|--schema-file <avro_schema_file>]    File containing Avro schema definition\n" \
          "    [-r|--registry <registry_url>]           Schema Registry URL to use\n" \
          "    [-b|--broker-list <broker_list>]         Comma-separated list of Kafka brokers\n" \
          "        <kafka_topic> defaults to '$KAFKA_TOPIC'\n" \
          "        <avro_schema> defaults to '$AVRO_SCHEMA'\n" \
          "        <registry_url> defaults to '$REGISTRY_URL'\n" \
          "        <broker_list> defaults to '$BROKER_LIST'"
  exit ${1:-0}
}

while [[ $# -gt 0 ]]; do
  case $1 in
    -h|--help)
      usage ;;
    -t|--topic)
      if [[ $# -lt 2 ]]; then
        echo "$0: must provide name of kafka topic following $1 flag"
        usage 1
      else
        shift
        KAFKA_TOPIC="$1"
      fi ;;
    -s|--schema)
      if [[ $# -lt 2 ]]; then
        echo "$0: must provide schema following $1 flag"
        usage 1
      else
        shift
        AVRO_SCHEMA="$1"
      fi ;;
    -f|--schema-file)
      if [[ $# -lt 2 ]]; then
        echo "$0: must provide schema file following $1 flag"
        usage 1
      else
        shift
        AVRO_SCHEMA=`cat "$1"`
        exit_value=$?
        if [[ $exit_value != 0 ]]; then
          exit $exit_value
        fi
      fi ;;
    -r|--registry-url)
      if [[ $# -lt 2 ]]; then
        echo "$0: must provide registry url following $1 flag"
        usage 1
      else
        shift
        REGISTRY_URL="$1"
      fi ;;
    -b|--broker-list)
      if [[ $# -lt 2 ]]; then
        echo "$0: must provide broker list following $1 flag"
        usage 1
      else
        shift
        BROKER_LIST="$1"
      fi ;;
    *)
      echo "$1: unrecognized option"
      usage 1 ;;
  esac
  shift
done

exec "$CONFLUENT_DIR/bin/kafka-avro-console-producer" \
    --broker-list "$BROKER_LIST" \
    --topic "$KAFKA_TOPIC" \
    --property value.schema="$AVRO_SCHEMA" \
    --property schema.registry.url="$REGISTRY_URL"
