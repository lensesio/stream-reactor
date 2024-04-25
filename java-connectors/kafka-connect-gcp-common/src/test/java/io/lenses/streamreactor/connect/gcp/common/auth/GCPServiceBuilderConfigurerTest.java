/*
 * Copyright 2017-2024 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.gcp.common.auth;


import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.*;
import com.google.cloud.http.HttpTransportOptions;
import io.lenses.streamreactor.common.config.base.RetryConfig;
import io.lenses.streamreactor.connect.gcp.common.auth.mode.NoAuthMode;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

class GCPServiceBuilderConfigurerTest {

    private GCPConnectionConfig.GCPConnectionConfigBuilder configBuilder;

    @BeforeEach
    public void setUp() {
        configBuilder = GCPConnectionConfig.builder()
                .projectId(Optional.empty())
                .quotaProjectId(Optional.empty())
                .authMode(NoAuthMode.INSTANCE)
                .host(Optional.empty())
                .httpRetryConfig(RetryConfig.builder().build())
                .timeouts(
                        HttpTimeoutConfig.builder()
                                .connectionTimeout(Optional.empty())
                                .socketTimeout(Optional.empty())
                                .build()
                );

    }

    @Test
    void testConfigure_withHostAndProjectIdConfigured() throws IOException {
        val config = configBuilder
                .host(Optional.of("example.com"))
                .projectId(Optional.of("test-project"))
                .build();

        val builder = createMockBuilder();

        GCPServiceBuilderConfigurer.configure(config, builder);

        assertHostAndProjectIdConfigured(builder, "example.com", "test-project");
    }

    @Test
    void testConfigure_withRetrySettingsConfigured() throws IOException {
        RetryConfig retryConfig = RetryConfig.builder().errorRetryInterval(1000).numberOfRetries(3).build();

        val config = configBuilder.httpRetryConfig(retryConfig)
                .build();

        val builder = createMockBuilder();

        GCPServiceBuilderConfigurer.configure(config, builder);

        assertRetrySettingsConfigured(builder, 1000, 5000, 3);
    }

    @Test
    void testConfigure_withTransportOptionsConfigured() throws IOException {
        val timeoutConfig = HttpTimeoutConfig.builder()
                        .socketTimeout(Optional.of(5000L))
                                .connectionTimeout(Optional.of(3000L))
                                        .build();

        val config = configBuilder.timeouts(timeoutConfig).build();

        val builder = createMockBuilder();

        GCPServiceBuilderConfigurer.configure(config, builder);

        assertTransportOptionsConfigured(builder, 5000, 3000);
    }

    @Test
    void testConfigure_withEmptyConfig() throws IOException {
        val builder = createMockBuilder();

        val config = configBuilder.build();

        GCPServiceBuilderConfigurer.configure(config, builder);

        // Ensure that no properties are set if configuration is empty
        verify(builder, never()).setHost(anyString());
        verify(builder, never()).setProjectId(anyString());
        verify(builder, times(1)).setRetrySettings(RetrySettings.newBuilder().build());
        verify(builder, times(1)).setCredentials(NoCredentials.getInstance());
        verify(builder, never()).setTransportOptions(any());
    }

    private TestSvcServiceOptionsBuilder createMockBuilder() {
        return mock(TestSvcServiceOptionsBuilder.class);
    }

    private void assertHostAndProjectIdConfigured(ServiceOptions.Builder<?, ?, ?> builder, String expectedHost, String expectedProjectId) {
        verify(builder, times(1)).setHost(expectedHost);
        verify(builder, times(1)).setProjectId(expectedProjectId);
    }

    private void assertRetrySettingsConfigured(ServiceOptions.Builder<?, ?, ?> builder, long expectedInitialRetryDelay, long expectedMaxRetryDelay, int expectedMaxAttempts) {
        ArgumentCaptor<RetrySettings> retrySettingsCaptor = ArgumentCaptor.forClass(RetrySettings.class);
        verify(builder).setRetrySettings(retrySettingsCaptor.capture());

        RetrySettings capturedRetrySettings = retrySettingsCaptor.getValue();
        assertNotNull(capturedRetrySettings);
        assertEquals(1000, capturedRetrySettings.getInitialRetryDelay().toMillis());
        assertEquals(5000, capturedRetrySettings.getMaxRetryDelay().toMillis());
        assertEquals(3, capturedRetrySettings.getMaxAttempts());
    }

    private void assertTransportOptionsConfigured(ServiceOptions.Builder<?, ?, ?> builder, int expectedReadTimeout, int expectedConnectTimeout) {
        ArgumentCaptor<HttpTransportOptions> transportOptionsCaptor = ArgumentCaptor.forClass(HttpTransportOptions.class);
        verify(builder).setTransportOptions(transportOptionsCaptor.capture());

        HttpTransportOptions capturedTransportOptions = transportOptionsCaptor.getValue();
        assertNotNull(capturedTransportOptions);
        assertEquals(5000, capturedTransportOptions.getReadTimeout());
        assertEquals(3000, capturedTransportOptions.getConnectTimeout());
    }

}
