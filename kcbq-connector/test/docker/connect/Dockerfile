# Copyright 2016 WePay, Inc.
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
# Builds a docker image for the Kafka-BigQuery Connector.
# Expects links to "kafka" and "schema-registry" containers.
#
# Usage:
#   docker build -t kcbq/connect connect
#   docker run --name kcbq_test_connect \
#              --link kcbq_test_kafka:kafka --link kcbq_test_schema-registry:schema-registry \
#              kcbq/connect

FROM confluentinc/cp-kafka-connect-base:4.1.2

COPY connect-docker.sh /usr/local/bin/

RUN ["chmod", "+x", "/usr/local/bin/connect-docker.sh"]

RUN ["mkdir", "/usr/logs"]
RUN ["chmod", "a+rwx", "/usr/logs"]

RUN ["mkdir", "-p", "/usr/local/share/kafka/plugins/kafka-connect-bigquery"]
RUN ["chmod", "a+rwx", "/usr/local/share/kafka/plugins/kafka-connect-bigquery"]

USER root
ENTRYPOINT ["/usr/local/bin/connect-docker.sh"]
