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

package com.wepay.kafka.connect.bigquery;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.filter.GcpCredsFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.PROJECT_CONFIG;

public abstract class GcpClientBuilder<Client> {

  public enum KeySource {
    FILE, JSON
  }

  private static final Logger logger = LoggerFactory.getLogger(GcpClientBuilder.class);

  private String project = null;
  private KeySource keySource = null;
  private String key = null;

  public GcpClientBuilder<Client> withConfig(BigQuerySinkConfig config) {
    return withProject(config.getString(PROJECT_CONFIG))
        .withKeySource(config.getKeySource())
        .withKey(config.getKey());
  }

  public GcpClientBuilder<Client> withProject(String project) {
    Objects.requireNonNull(project, "Project cannot be null");
    this.project = project;
    return this;
  }

  public GcpClientBuilder<Client> withKeySource(KeySource keySource) {
    Objects.requireNonNull(keySource, "Key cannot be null");
    this.keySource = keySource;
    return this;
  }

  public GcpClientBuilder<Client> withKey(String key) {
    this.key = key;
    return this;
  }

  public Client build() {
    return doBuild(project, credentials());
  }

  private GoogleCredentials credentials() {
    if (key == null) {
      return null;
    }

    Objects.requireNonNull(keySource, "Key source must be defined to build a GCP client");
    Objects.requireNonNull(project, "Project must be defined to build a GCP client");

    InputStream credentialsStream;
    String keyfileConfig;
    switch (keySource) {
      case JSON:
        keyfileConfig = GcpCredsFilter.filterCreds(key, false);
        break;
      case FILE:
        keyfileConfig = GcpCredsFilter.filterCreds(key, true);
        break;
      default:
        throw new IllegalArgumentException("Unexpected value for KeySource enum: " + keySource);
    }

    logger.debug("Attempting to authenticate with BigQuery using filtered json key");
    credentialsStream = new ByteArrayInputStream(keyfileConfig.getBytes(StandardCharsets.UTF_8));

    try {
      return GoogleCredentials.fromStream(credentialsStream);
    } catch (IOException e) {
      throw new BigQueryConnectException("Failed to create credentials from input stream", e);
    }
  }

  protected abstract Client doBuild(String project, GoogleCredentials credentials);

  public static class BigQueryBuilder extends GcpClientBuilder<BigQuery> {
    @Override
    protected BigQuery doBuild(String project, GoogleCredentials credentials) {
      BigQueryOptions.Builder builder = BigQueryOptions.newBuilder()
          .setProjectId(project);

      if (credentials != null) {
        builder.setCredentials(credentials);
      } else {
        logger.debug("Attempting to access BigQuery without authentication");
      }

      return builder.build().getService();
    }
  }

  public static class GcsBuilder extends GcpClientBuilder<Storage> {
    @Override
    protected Storage doBuild(String project, GoogleCredentials credentials) {
      StorageOptions.Builder builder = StorageOptions.newBuilder()
          .setProjectId(project);

      if (credentials != null) {
        builder.setCredentials(credentials);
      } else {
        logger.debug("Attempting to access GCS without authentication");
      }

      return builder.build().getService();
    }
  }
}
