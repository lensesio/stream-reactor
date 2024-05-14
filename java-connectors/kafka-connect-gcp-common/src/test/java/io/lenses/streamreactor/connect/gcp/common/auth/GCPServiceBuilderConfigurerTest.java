/*
 * Copyright 2017-2024 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.gcp.common.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.NoCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.http.HttpTransportOptions;
import io.lenses.streamreactor.common.config.base.RetryConfig;
import io.lenses.streamreactor.connect.gcp.common.auth.mode.NoAuthMode;
import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.threeten.bp.Duration;

class GCPServiceBuilderConfigurerTest {

  private GCPConnectionConfig.GCPConnectionConfigBuilder configBuilder;

  @BeforeEach
  public void setUp() {
    configBuilder = GCPConnectionConfig.builder().authMode(new NoAuthMode());
  }

  @Test
  void testConfigure_withHostAndProjectIdConfigured() throws IOException {
    final var config = configBuilder.host("example.com").projectId("test-project").build();

    final var builder = createMockBuilder();

    GCPServiceBuilderConfigurer.configure(config, builder);

    assertHostAndProjectIdConfigured(builder, "example.com", "test-project");
  }

  @Test
  void testConfigure_withRetrySettingsConfigured() throws IOException {
    RetryConfig retryConfig = RetryConfig.builder().retryIntervalMillis(1000).retryLimit(3).build();

    final var config = configBuilder.httpRetryConfig(retryConfig).build();

    final var builder = createMockBuilder();

    GCPServiceBuilderConfigurer.configure(config, builder);

    assertRetrySettingsConfigured(builder, 1000, 5000, 3);
  }

  @Test
  void testConfigure_withTransportOptionsConfigured() throws IOException {
    final var timeoutConfig =
        HttpTimeoutConfig.builder()
            .socketTimeoutMillis(5000L)
            .connectionTimeoutMillis(3000L)
            .build();

    final var config = configBuilder.timeouts(timeoutConfig).build();

    final var builder = createMockBuilder();

    GCPServiceBuilderConfigurer.configure(config, builder);

    assertTransportOptionsConfigured(builder, 5000, 3000);
  }

  @Test
  void testConfigure_withEmptyConfig() throws IOException {
    final var builder = createMockBuilder();

    final var config = configBuilder.build();

    GCPServiceBuilderConfigurer.configure(config, builder);

    // Ensure that no properties are set if configuration is empty
    verify(builder, never()).setHost(anyString());
    verify(builder, never()).setProjectId(anyString());
    verify(builder, times(1))
        .setRetrySettings(
            RetrySettings.newBuilder()
                .setInitialRetryDelay(Duration.ofMillis(50))
                .setMaxRetryDelay(Duration.ofMillis(250))
                .setMaxAttempts(5)
                .build());
    verify(builder, times(1)).setCredentials(NoCredentials.getInstance());
    verify(builder, never()).setTransportOptions(any());
  }

  private TestSvcServiceOptionsBuilder createMockBuilder() {
    return mock(TestSvcServiceOptionsBuilder.class);
  }

  private void assertHostAndProjectIdConfigured(
      ServiceOptions.Builder<?, ?, ?> builder, String expectedHost, String expectedProjectId) {
    verify(builder, times(1)).setHost(expectedHost);
    verify(builder, times(1)).setProjectId(expectedProjectId);
  }

  private void assertRetrySettingsConfigured(
      ServiceOptions.Builder<?, ?, ?> builder,
      long expectedInitialRetryDelay,
      long expectedMaxRetryDelay,
      int expectedMaxAttempts) {
    ArgumentCaptor<RetrySettings> retrySettingsCaptor =
        ArgumentCaptor.forClass(RetrySettings.class);
    verify(builder).setRetrySettings(retrySettingsCaptor.capture());

    RetrySettings capturedRetrySettings = retrySettingsCaptor.getValue();
    assertNotNull(capturedRetrySettings);
    assertEquals(1000, capturedRetrySettings.getInitialRetryDelay().toMillis());
    assertEquals(5000, capturedRetrySettings.getMaxRetryDelay().toMillis());
    assertEquals(3, capturedRetrySettings.getMaxAttempts());
  }

  private void assertTransportOptionsConfigured(
      ServiceOptions.Builder<?, ?, ?> builder,
      int expectedReadTimeout,
      int expectedConnectTimeout) {
    ArgumentCaptor<HttpTransportOptions> transportOptionsCaptor =
        ArgumentCaptor.forClass(HttpTransportOptions.class);
    verify(builder).setTransportOptions(transportOptionsCaptor.capture());

    HttpTransportOptions capturedTransportOptions = transportOptionsCaptor.getValue();
    assertNotNull(capturedTransportOptions);
    assertEquals(5000, capturedTransportOptions.getReadTimeout());
    assertEquals(3000, capturedTransportOptions.getConnectTimeout());
  }
}
