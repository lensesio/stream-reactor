/*
 * Copyright 2020 Confluent, Inc.
 *
 * This software contains code derived from the WePay BigQuery Kafka Connector, Copyright WePay, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wepay.kafka.connect.bigquery.config;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.BIGQUERY_TIMESTAMP_PARTITION_FIELD_NAME_CONFIG;

public class PartitioningModeValidator extends MultiPropertyValidator<BigQuerySinkConfig> {
  public PartitioningModeValidator() {
    super(BIGQUERY_PARTITION_DECORATOR_CONFIG);
  }

  private static final Collection<String> DEPENDENTS = Collections.unmodifiableCollection(Arrays.asList(
      BIGQUERY_TIMESTAMP_PARTITION_FIELD_NAME_CONFIG
  ));

  @Override
  protected Collection<String> dependents() {
    return DEPENDENTS;
  }

  @Override
  protected Optional<String> doValidate(BigQuerySinkConfig config) {
    if (!config.getBoolean(BIGQUERY_PARTITION_DECORATOR_CONFIG)) {
      return Optional.empty();
    }

    if (config.getTimestampPartitionFieldName().isPresent()) {
      return Optional.of(String.format("Only one partitioning mode may be specified for the connector. "
              + "Use either %s OR %s.",
          BIGQUERY_PARTITION_DECORATOR_CONFIG,
          BIGQUERY_TIMESTAMP_PARTITION_FIELD_NAME_CONFIG
      ));
    } else {
      return Optional.empty();
    }
  }
}
