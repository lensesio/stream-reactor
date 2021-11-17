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
import java.util.List;
import java.util.Optional;

import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.ENABLE_BATCH_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.GCS_BUCKET_NAME_CONFIG;

public class GcsBucketValidator extends MultiPropertyValidator<BigQuerySinkConfig> {

  public GcsBucketValidator() {
    super(GCS_BUCKET_NAME_CONFIG);
  }

  private static final Collection<String> DEPENDENTS = Collections.unmodifiableCollection(Arrays.asList(
      ENABLE_BATCH_CONFIG
  ));

  @Override
  protected Collection<String> dependents() {
    return DEPENDENTS;
  }

  @Override
  protected Optional<String> doValidate(BigQuerySinkConfig config) {
    List<String> batchLoadedTopics = config.getList(ENABLE_BATCH_CONFIG);
    if (batchLoadedTopics ==  null || batchLoadedTopics.isEmpty()) {
      // Batch loading is disabled; no need to validate the GCS bucket
      return Optional.empty();
    }

    String bucket = config.getString(GCS_BUCKET_NAME_CONFIG);
    if (bucket == null || bucket.trim().isEmpty()) {
      return Optional.of("When GCS batch loading is enabled, a bucket must be provided");
    }

    // No need to validate that the bucket exists; we create it automatically if it doesn't

    return Optional.empty();
  }
}
