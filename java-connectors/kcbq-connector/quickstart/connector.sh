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

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"

if [ -z $CONFLUENT_DIR ]; then
  CONFLUENT_DIR="$BASE_DIR/../../confluent-3.0.0"
fi

export CLASSPATH="$CLASSPATH:$BASE_DIR/../../bin/jar/*"

STANDALONE_PROPS="$BASE_DIR/properties/standalone.properties"
CONNECTOR_PROPS="$BASE_DIR/properties/connector.properties"

exec "$CONFLUENT_DIR/bin/connect-standalone" "$STANDALONE_PROPS" "$CONNECTOR_PROPS"

# RIGHT WAY
# examine dependencies, see who's pulling in what
