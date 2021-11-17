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
import org.junit.Test;

import java.util.Optional;

import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.SCHEMA_UPDATE_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.TABLE_CREATE_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SchemaRetrieverValidatorTest {

  @Test
  public void testDisabledTableCreationSkipsValidation() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);
    when(config.getBoolean(TABLE_CREATE_CONFIG)).thenReturn(false);

    assertEquals(
        Optional.empty(),
        new SchemaRetrieverValidator.TableCreationValidator().doValidate(config)
    );
  }

  @Test
  public void testDisabledSchemaUpdatesSkipsValidation() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);
    when(config.getBoolean(TABLE_CREATE_CONFIG)).thenReturn(true);

    assertEquals(
        Optional.empty(),
        new SchemaRetrieverValidator.SchemaUpdateValidator().doValidate(config)
    );
  }

  @Test
  public void testTableCreationEnabledWithNoRetriever() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);
    when(config.getBoolean(TABLE_CREATE_CONFIG)).thenReturn(true);
    when(config.getSchemaRetriever()).thenReturn(null);

    assertNotEquals(
        Optional.empty(),
        new SchemaRetrieverValidator.TableCreationValidator().doValidate(config)
    );
  }

  @Test
  public void testSchemaUpdatesEnabledWithNoRetriever() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);
    when(config.getBoolean(SCHEMA_UPDATE_CONFIG)).thenReturn(true);
    when(config.getSchemaRetriever()).thenReturn(null);

    assertNotEquals(
        Optional.empty(),
        new SchemaRetrieverValidator.SchemaUpdateValidator().doValidate(config)
    );
  }

  @Test
  public void testTableCreationEnabledWithValidRetriever() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);
    when(config.getBoolean(TABLE_CREATE_CONFIG)).thenReturn(true);
    SchemaRetriever mockRetriever = mock(SchemaRetriever.class);
    when(config.getSchemaRetriever()).thenReturn(mockRetriever);

    assertEquals(
        Optional.empty(),
        new SchemaRetrieverValidator.TableCreationValidator().doValidate(config)
    );
  }

  @Test
  public void testSchemaUpdatesEnabledWithValidRetriever() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);
    when(config.getBoolean(SCHEMA_UPDATE_CONFIG)).thenReturn(true);
    SchemaRetriever mockRetriever = mock(SchemaRetriever.class);
    when(config.getSchemaRetriever()).thenReturn(mockRetriever);

    assertEquals(
        Optional.empty(),
        new SchemaRetrieverValidator.SchemaUpdateValidator().doValidate(config)
    );
  }
}
