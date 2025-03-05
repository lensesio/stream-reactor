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

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.common.annotations.VisibleForTesting;
import com.wepay.kafka.connect.bigquery.GcpClientBuilder;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.AUTO_CREATE_BUCKET_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.ENABLE_BATCH_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.GCS_BUCKET_NAME_CONFIG;

public class GcsBucketValidator extends MultiPropertyValidator<BigQuerySinkConfig> {

  public GcsBucketValidator() {
    super(GCS_BUCKET_NAME_CONFIG);
  }

  private static final Collection<String> DEPENDENTS = Collections.unmodifiableCollection(Arrays.asList(
      ENABLE_BATCH_CONFIG, AUTO_CREATE_BUCKET_CONFIG
  ));

  @Override
  protected Collection<String> dependents() {
    return DEPENDENTS;
  }

  @Override
  protected Optional<String> doValidate(BigQuerySinkConfig config) {
    Storage gcs;
    try {
      gcs  = new GcpClientBuilder.GcsBuilder()
          .withConfig(config)
          .build();
    } catch (RuntimeException e) {
      return Optional.of(String.format(
          "Failed to construct GCS client%s",
          e.getMessage() != null ? ": " + e.getMessage() : ""
      ));
    }
    return doValidate(gcs, config);
  }

  @VisibleForTesting
  Optional<String> doValidate(Storage gcs, BigQuerySinkConfig config) {
    List<String> batchLoadedTopics = config.getList(ENABLE_BATCH_CONFIG);
    if (batchLoadedTopics ==  null || batchLoadedTopics.isEmpty()) {
      // Batch loading is disabled; no need to validate the GCS bucket
      return Optional.empty();
    }

    String bucketName = config.getString(GCS_BUCKET_NAME_CONFIG);
    if (bucketName == null || bucketName.trim().isEmpty()) {
      return Optional.of("When GCS batch loading is enabled, a bucket must be provided");
    }

    if (config.getBoolean(AUTO_CREATE_BUCKET_CONFIG)) {
      return Optional.empty();
    }

    Bucket bucket = gcs.get(bucketName);
    if (bucket == null) {
      return Optional.of(String.format(
          "Automatic bucket creation is disabled but the GCS bucket %s does not exist. "
              + "Please either manually create this table before restarting the connector or enable automatic bucket creation "
              + "by the connector",
          bucketName
      ));
    }

    return Optional.empty();
  }
}
