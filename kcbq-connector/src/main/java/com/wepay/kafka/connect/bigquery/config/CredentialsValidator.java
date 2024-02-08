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
import com.google.cloud.storage.Storage;
import com.wepay.kafka.connect.bigquery.GcpClientBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.ENABLE_BATCH_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.GCS_BUCKET_NAME_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.KEYFILE_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.KEY_SOURCE_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.PROJECT_CONFIG;
import static com.wepay.kafka.connect.bigquery.GcpClientBuilder.KeySource;

public abstract class CredentialsValidator<ClientBuilder extends GcpClientBuilder<?>> extends MultiPropertyValidator<BigQuerySinkConfig> {

  public CredentialsValidator() {
    super(KEYFILE_CONFIG);
  }

  private static final Collection<String> DEPENDENTS = Collections.unmodifiableCollection(Arrays.asList(
      PROJECT_CONFIG, KEY_SOURCE_CONFIG
  ));

  @Override
  protected Collection<String> dependents() {
    return DEPENDENTS;
  }

  @Override
  protected Optional<String> doValidate(BigQuerySinkConfig config) {
    String keyFile = config.getKey();
    KeySource keySource = config.getKeySource();

    if (keySource == KeySource.APPLICATION_DEFAULT && keyFile != null  && !keyFile.isEmpty()) {
      String errorMessage = KEYFILE_CONFIG + " should not be provided if " + KEY_SOURCE_CONFIG
              + " is " + KeySource.APPLICATION_DEFAULT;
      return Optional.of(errorMessage);
    }

    if ((keyFile == null || keyFile.isEmpty()) && config.getKeySource() != GcpClientBuilder.KeySource.APPLICATION_DEFAULT) {
      // No credentials to validate
      return Optional.empty();
    }

    try {
      clientBuilder()
          .withConfig(config)
          .build();
      return Optional.empty();
    } catch (RuntimeException e) {
      String errorMessage = "An unexpected error occurred while validating credentials for " + gcpService();
      if (e.getMessage() != null) {
        errorMessage += ": " + e.getMessage();
      }
      return Optional.of(errorMessage);
    }
  }

  protected abstract String gcpService();
  protected abstract ClientBuilder clientBuilder();

  public static class BigQueryCredentialsValidator extends CredentialsValidator<GcpClientBuilder<BigQuery>> {
    @Override
    public String gcpService() {
      return "BigQuery";
    }

    @Override
    protected GcpClientBuilder<BigQuery> clientBuilder() {
      return new GcpClientBuilder.BigQueryBuilder();
    }
  }

  public static class GcsCredentialsValidator extends CredentialsValidator<GcpClientBuilder<Storage>> {

    private static final Collection<String> DEPENDENTS;

    static {
      List<String> dependents = new ArrayList<>(CredentialsValidator.DEPENDENTS);
      dependents.add(ENABLE_BATCH_CONFIG);
      dependents.add(GCS_BUCKET_NAME_CONFIG);
      DEPENDENTS = Collections.unmodifiableCollection(dependents);
    }

    @Override
    public Collection<String> dependents() {
      return DEPENDENTS;
    }

    @Override
    public String gcpService() {
      return "GCS";
    }

    @Override
    protected GcpClientBuilder<Storage> clientBuilder() {
      return new GcpClientBuilder.GcsBuilder();
    }
  }
}
