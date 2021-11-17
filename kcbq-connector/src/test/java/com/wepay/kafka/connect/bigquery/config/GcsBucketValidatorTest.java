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

import org.junit.Test;

import java.util.Collections;
import java.util.Optional;

import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.ENABLE_BATCH_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.GCS_BUCKET_NAME_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GcsBucketValidatorTest {

  @Test
  public void testNullBatchLoadingSkipsValidation() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);
    when(config.getList(ENABLE_BATCH_CONFIG)).thenReturn(null);

    assertEquals(
        Optional.empty(),
        new GcsBucketValidator().doValidate(config)
    );
  }

  @Test
  public void testEmptyBatchLoadingSkipsValidation() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);
    when(config.getList(ENABLE_BATCH_CONFIG)).thenReturn(Collections.emptyList());

    assertEquals(
        Optional.empty(),
        new GcsBucketValidator().doValidate(config)
    );
  }

  @Test
  public void testNullBucketWithBatchLoading() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);
    when(config.getList(ENABLE_BATCH_CONFIG)).thenReturn(Collections.singletonList("t1"));
    when(config.getString(GCS_BUCKET_NAME_CONFIG)).thenReturn(null);

    assertNotEquals(
        Optional.empty(),
        new GcsBucketValidator().doValidate(config)
    );
  }

  @Test
  public void testBlankBucketWithBatchLoading() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);
    when(config.getList(ENABLE_BATCH_CONFIG)).thenReturn(Collections.singletonList("t1"));
    when(config.getString(GCS_BUCKET_NAME_CONFIG)).thenReturn("  \t  ");

    assertNotEquals(
        Optional.empty(),
        new GcsBucketValidator().doValidate(config)
    );
  }

  @Test
  public void testValidBucketWithBatchLoading() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);
    when(config.getList(ENABLE_BATCH_CONFIG)).thenReturn(Collections.singletonList("t1"));
    when(config.getString(GCS_BUCKET_NAME_CONFIG)).thenReturn("gee_cs");

    assertEquals(
        Optional.empty(),
        new GcsBucketValidator().doValidate(config)
    );
  }
}
