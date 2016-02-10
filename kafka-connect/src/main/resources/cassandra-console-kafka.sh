#!/usr/bin/env bash
${CONFLUENT_HOME}/bin/kafka-avro-console-producer \
             --broker-list localhost:9092 --topic test_table \
             --property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"id","type":"int"}, {"name":"random_field", "type": "string"}]}'
