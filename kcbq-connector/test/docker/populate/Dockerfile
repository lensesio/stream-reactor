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
# Populates Kafka and Schema Registry with test data
# Expects links to "kafka" and "schema-registry" containers.
#
# Usage:
#   docker build -t kcbq/populate populate
#   docker run --name kcbq_test_populate \
#              --link kcbq_test_kafka:kafka --link kcbq_test_schema-registry:schema-registry \
#              kcbq/populate

FROM confluentinc/cp-schema-registry:4.1.2

COPY populate-docker.sh /usr/local/bin/

RUN ["chmod", "+x", "/usr/local/bin/populate-docker.sh"]

USER root
ENTRYPOINT ["/usr/local/bin/populate-docker.sh"]
