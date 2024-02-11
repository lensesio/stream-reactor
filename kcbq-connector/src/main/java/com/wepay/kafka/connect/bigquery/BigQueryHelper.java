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

import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;

import com.wepay.kafka.connect.bigquery.filter.GcpCredsFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Convenience class for creating a default {@link com.google.cloud.bigquery.BigQuery} instance,
 * with or without login credentials.
 */
public class BigQueryHelper {
  private static final Logger logger = LoggerFactory.getLogger(BigQueryHelper.class);

  /**
   * Returns a default {@link BigQuery} instance for the specified project with credentials provided
   * in the specified file, which can then be used for creating, updating, and inserting into tables
   * from specific datasets.
   *
   * @param projectName The name of the BigQuery project to work with
   * @param keyFilename The name of a file containing a JSON key that can be used to provide
   *                    credentials to BigQuery, or null if no authentication should be performed.
   * @return The resulting BigQuery object.
   */
  public BigQuery connect(String projectName, String keyFilename) {
    if (keyFilename == null) {
      return connect(projectName);
    }

    String keyfileConfig = GcpCredsFilter.filterCreds(keyFilename, true);

    logger.debug("Using filtered keyfile config");
    try (InputStream credentialsStream = new ByteArrayInputStream(keyfileConfig.getBytes(StandardCharsets.UTF_8))) {
      logger.debug("Attempting to authenticate with BigQuery using provided json key");
      return new BigQueryOptions.DefaultBigqueryFactory().create(
          BigQueryOptions.newBuilder()
          .setProjectId(projectName)
          .setCredentials(GoogleCredentials.fromStream(credentialsStream))
          .build()
      );
    } catch (IOException err) {
      throw new BigQueryConnectException("Failed to access json key file", err);
    }
  }

  /**
   * Returns a default {@link BigQuery} instance for the specified project with no authentication
   * credentials, which can then be used for creating, updating, and inserting into tables from
   * specific datasets.
   *
   * @param projectName The name of the BigQuery project to work with
   * @return The resulting BigQuery object.
   */
  public BigQuery connect(String projectName) {
    logger.debug("Attempting to access BigQuery without authentication");
    return new BigQueryOptions.DefaultBigqueryFactory().create(
        BigQueryOptions.newBuilder()
        .setProjectId(projectName)
        .build()
    );
  }
}
