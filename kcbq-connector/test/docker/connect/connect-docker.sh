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

unzip -j -d /usr/local/share/kafka/plugins/kafka-connect-bigquery/ /usr/local/share/kafka/plugins/kafka-connect-bigquery/kcbq.zip 'wepay-kafka-connect-bigquery-*/lib/*.jar'

connect-standalone \
    /etc/kafka-connect-bigquery/standalone.properties \
    /etc/kafka-connect-bigquery/connector.properties &

# Time (seconds) to wait for the process for inserting rows into BigQuery to be done.
# This time can be adjusted if necessary.
sleep 180
kill $!
