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

import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.SCHEMA_RETRIEVER_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.SCHEMA_UPDATE_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.TABLE_CREATE_CONFIG;

public abstract class SchemaRetrieverValidator extends MultiPropertyValidator<BigQuerySinkConfig> {

  protected SchemaRetrieverValidator(String propertyName) {
    super(propertyName);
  }

  private static final Collection<String> DEPENDENTS = Collections.unmodifiableCollection(Arrays.asList(
      SCHEMA_RETRIEVER_CONFIG
  ));

  @Override
  protected Collection<String> dependents() {
    return DEPENDENTS;
  }

  @Override
  protected Optional<String> doValidate(BigQuerySinkConfig config) {
    if (!schemaRetrieverRequired(config)) {
      return Optional.empty();
    }

    SchemaRetriever schemaRetriever = config.getSchemaRetriever();
    if (schemaRetriever != null) {
      return Optional.empty();
    } else {
      return Optional.of(missingSchemaRetrieverMessage());
    }
  }

  /**
   * @param config the user-provided configuration
   * @return whether a schema retriever class is required for the property this validator is responsible for
   */
  protected abstract boolean schemaRetrieverRequired(BigQuerySinkConfig config);

  /**
   * @return an error message explaining why a schema retriever class is required for the property this validator is
   * responsible for
   */
  protected abstract String missingSchemaRetrieverMessage();

  public static class TableCreationValidator extends SchemaRetrieverValidator {
    public TableCreationValidator() {
      super(TABLE_CREATE_CONFIG);
    }

    @Override
    protected boolean schemaRetrieverRequired(BigQuerySinkConfig config) {
      return config.getBoolean(TABLE_CREATE_CONFIG);
    }

    @Override
    protected String missingSchemaRetrieverMessage() {
      return "A schema retriever class is required when automatic table creation is enabled";
    }
  }

  public static class SchemaUpdateValidator extends SchemaRetrieverValidator {
    public SchemaUpdateValidator() {
      super(SCHEMA_UPDATE_CONFIG);
    }

    @Override
    protected boolean schemaRetrieverRequired(BigQuerySinkConfig config) {
      return config.getBoolean(SCHEMA_UPDATE_CONFIG);
    }

    @Override
    protected String missingSchemaRetrieverMessage() {
      return "A schema retriever class is required when automatic schema updates are enabled";
    }
  }
}
