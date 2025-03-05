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

import java.util.Optional;

import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PartitioningModeValidatorTest {

  @Test
  public void testDisabledDecoratorSyntaxSkipsValidation() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);
    when(config.getBoolean(BIGQUERY_PARTITION_DECORATOR_CONFIG)).thenReturn(false);

    assertEquals(
        Optional.empty(),
        new PartitioningModeValidator().doValidate(config)
    );
  }

  @Test
  public void testDecoratorSyntaxWithoutTimestampPartitionFieldName() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);
    when(config.getBoolean(BIGQUERY_PARTITION_DECORATOR_CONFIG)).thenReturn(true);
    when(config.getTimestampPartitionFieldName()).thenReturn(Optional.empty());

    assertEquals(
        Optional.empty(),
        new PartitioningModeValidator().doValidate(config)
    );
  }

  @Test
  public void testDecoratorSyntaxWithTimestampPartitionFieldName() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);
    when(config.getBoolean(BIGQUERY_PARTITION_DECORATOR_CONFIG)).thenReturn(true);
    when(config.getTimestampPartitionFieldName()).thenReturn(Optional.of("f1"));

    assertNotEquals(
        Optional.empty(),
        new PartitioningModeValidator().doValidate(config)
    );
  }

  @Test
  public void testTimestampPartitionFieldNameWithoutDecoratorSyntax() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);
    when(config.getBoolean(BIGQUERY_PARTITION_DECORATOR_CONFIG)).thenReturn(false);
    when(config.getTimestampPartitionFieldName()).thenReturn(Optional.of("f1"));

    assertEquals(
        Optional.empty(),
        new PartitioningModeValidator().doValidate(config)
    );
  }
}
