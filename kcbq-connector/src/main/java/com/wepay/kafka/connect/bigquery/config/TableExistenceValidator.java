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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableId;
import com.google.common.annotations.VisibleForTesting;
import com.wepay.kafka.connect.bigquery.GcpClientBuilder;
import com.wepay.kafka.connect.bigquery.utils.TopicToTableResolver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.DATASETS_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.KEYFILE_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.KEY_SOURCE_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.PROJECT_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.TABLE_CREATE_CONFIG;

public class TableExistenceValidator extends MultiPropertyValidator<BigQuerySinkConfig> {

  public TableExistenceValidator() {
    super(TABLE_CREATE_CONFIG);
  }

  private static final Collection<String> DEPENDENTS = Collections.unmodifiableCollection(Arrays.asList(
      SANITIZE_TOPICS_CONFIG,
      KEY_SOURCE_CONFIG,
      KEYFILE_CONFIG,
      PROJECT_CONFIG,
      DATASETS_CONFIG
  ));

  @Override
  protected Collection<String> dependents() {
    return DEPENDENTS;
  }

  @Override
  protected Optional<String> doValidate(BigQuerySinkConfig config) {
    BigQuery bigQuery;
    try {
      bigQuery = new GcpClientBuilder.BigQueryBuilder()
          .withConfig(config)
          .build();
    } catch (RuntimeException e) {
      return Optional.of(String.format(
          "Failed to construct BigQuery client%s",
          e.getMessage() != null ? ": " + e.getMessage() : ""
      ));
    }

    return doValidate(bigQuery, config);
  }

  @VisibleForTesting
  Optional<String> doValidate(BigQuery bigQuery, BigQuerySinkConfig config) {
    boolean autoCreateTables = config.getBoolean(TABLE_CREATE_CONFIG);
    // No need to check if tables already exist if we're allowed to create them ourselves
    if (autoCreateTables) {
      return Optional.empty();
    }

    List<TableId> missingTables = missingTables(bigQuery, config);

    if (missingTables.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(String.format(
        "Automatic table creation is disabled and the following tables do not appear to exist: %s. "
            + "Please either manually create these tables before restarting the connector or enable automatic table "
            + "creation by the connector.",
        missingTables.stream().map(t -> t.getDataset() + ":" + t.getTable()).collect(Collectors.joining(", "))
    ));
  }

  @VisibleForTesting
  List<TableId> missingTables(BigQuery bigQuery, BigQuerySinkConfig config) {
    return TopicToTableResolver.getTopicsToTables(config).values().stream()
        .filter(t -> bigQuery.getTable(t) == null)
        .collect(Collectors.toList());
  }
}
