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

import com.google.cloud.bigquery.TimePartitioning;
import org.junit.Test;

import java.util.Optional;

import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.TABLE_CREATE_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PartitioningTypeValidatorTest {

  @Test
  public void testDisabledDecoratorSyntaxSkipsValidation() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);
    when(config.getBoolean(BIGQUERY_PARTITION_DECORATOR_CONFIG)).thenReturn(false);
    when(config.getBoolean(TABLE_CREATE_CONFIG)).thenReturn(true);

    assertEquals(
        Optional.empty(),
        new PartitioningTypeValidator().doValidate(config)
    );
  }

  @Test
  public void testDisabledTableCreationSkipsValidation() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);
    when(config.getBoolean(BIGQUERY_PARTITION_DECORATOR_CONFIG)).thenReturn(true);
    when(config.getBoolean(TABLE_CREATE_CONFIG)).thenReturn(false);

    assertEquals(
        Optional.empty(),
        new PartitioningTypeValidator().doValidate(config)
    );
  }

  @Test
  public void testNonDayTimePartitioningWithTableCreationAndDecoratorSyntax() {
    // TODO: This can be refactored into programmatically-generated test cases once we start using JUnit 5
    for (TimePartitioning.Type timePartitioningType : TimePartitioning.Type.values()) {
      if (TimePartitioning.Type.DAY.equals(timePartitioningType)) {
        continue;
      }
      
      BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);
      when(config.getBoolean(BIGQUERY_PARTITION_DECORATOR_CONFIG)).thenReturn(true);
      when(config.getBoolean(TABLE_CREATE_CONFIG)).thenReturn(true);
      when(config.getTimePartitioningType()).thenReturn(Optional.of(timePartitioningType));
  
      assertNotEquals(
          Optional.empty(),
          new PartitioningTypeValidator().doValidate(config)
      );
    }
  }

  @Test
  public void testDayTimePartitioningWithTableCreationAndDecoratorSyntax() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);
    when(config.getBoolean(BIGQUERY_PARTITION_DECORATOR_CONFIG)).thenReturn(true);
    when(config.getBoolean(TABLE_CREATE_CONFIG)).thenReturn(true);
    when(config.getTimePartitioningType()).thenReturn(Optional.of(TimePartitioning.Type.DAY));

    assertEquals(
        Optional.empty(),
        new PartitioningTypeValidator().doValidate(config)
    );
  }
}
