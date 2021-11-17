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

import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.DELETE_ENABLED_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.KAFKA_KEY_FIELD_NAME_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.MERGE_INTERVAL_MS_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.MERGE_RECORDS_THRESHOLD_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.UPSERT_ENABLED_CONFIG;

public abstract class UpsertDeleteValidator extends MultiPropertyValidator<BigQuerySinkConfig> {
  private UpsertDeleteValidator(String propertyName) {
    super(propertyName);
  }

  private static final Collection<String> DEPENDENTS = Collections.unmodifiableCollection(Arrays.asList(
      MERGE_INTERVAL_MS_CONFIG, MERGE_RECORDS_THRESHOLD_CONFIG, KAFKA_KEY_FIELD_NAME_CONFIG
  ));

  @Override
  protected Collection<String> dependents() {
    return DEPENDENTS;
  }

  @Override
  protected Optional<String> doValidate(BigQuerySinkConfig config) {
    if (!modeEnabled(config)) {
      return Optional.empty();
    }

    long mergeInterval = config.getLong(MERGE_INTERVAL_MS_CONFIG);
    long mergeRecordsThreshold = config.getLong(MERGE_RECORDS_THRESHOLD_CONFIG);

    if (mergeInterval == -1 && mergeRecordsThreshold == -1) {
      return Optional.of(String.format(
          "%s and %s cannot both be -1",
          MERGE_INTERVAL_MS_CONFIG,
          MERGE_RECORDS_THRESHOLD_CONFIG
      ));
    }

    if (!config.getKafkaKeyFieldName().isPresent()) {
      return Optional.of(String.format(
          "%s must be specified when %s is set to true",
          KAFKA_KEY_FIELD_NAME_CONFIG,
          propertyName()
      ));
    }

    return Optional.empty();
  }

  /**
   * @param config the user-provided configuration
   * @return whether the write mode for the validator (i.e., either upsert or delete) is enabled
   */
  protected abstract boolean modeEnabled(BigQuerySinkConfig config);

  public static class UpsertValidator extends UpsertDeleteValidator {
    public UpsertValidator() {
      super(UPSERT_ENABLED_CONFIG);
    }

    @Override
    protected boolean modeEnabled(BigQuerySinkConfig config) {
      return config.getBoolean(UPSERT_ENABLED_CONFIG);
    }
  }

  public static class DeleteValidator extends UpsertDeleteValidator {
    public DeleteValidator() {
      super(DELETE_ENABLED_CONFIG);
    }

    @Override
    protected boolean modeEnabled(BigQuerySinkConfig config) {
      return config.getBoolean(DELETE_ENABLED_CONFIG);
    }
  }
}
