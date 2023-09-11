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

import com.wepay.kafka.connect.bigquery.GcpClientBuilder;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CredentialsValidatorTest {

  @Test
  public void testNoCredentialsSkipsValidation() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);
    when(config.getKey()).thenReturn(null);

    assertEquals(
        Optional.empty(),
        new CredentialsValidator.BigQueryCredentialsValidator().doValidate(config)
    );
    assertEquals(
        Optional.empty(),
        new CredentialsValidator.GcsCredentialsValidator().doValidate(config)
    );
  }

  @Test
  public void testFailureToConstructClient() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);
    when(config.getKey()).thenReturn("key");

    @SuppressWarnings("unchecked")
    GcpClientBuilder<Object> mockClientBuilder = mock(GcpClientBuilder.class);
    when(mockClientBuilder.withConfig(eq(config))).thenReturn(mockClientBuilder);
    when(mockClientBuilder.build()).thenThrow(new RuntimeException("Provided credentials are invalid"));

    assertNotEquals(
        Optional.empty(),
        new CredentialsValidator.BigQueryCredentialsValidator().doValidate(config)
    );
    assertNotEquals(
        Optional.empty(),
        new CredentialsValidator.GcsCredentialsValidator().doValidate(config)
    );
  }

  @Test
  public void testKeyShouldNotBeProvidedIfUsingApplicationDefaultCredentials() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);
    when(config.getKey()).thenReturn("key");
    when(config.getKeySource()).thenReturn(GcpClientBuilder.KeySource.APPLICATION_DEFAULT);

    assertTrue(
            new CredentialsValidator.BigQueryCredentialsValidator().doValidate(config)
                    .get().contains("should not be provided")
    );
  }
}
