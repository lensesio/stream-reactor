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
package io.lenses.streamreactor.connect.gcp.common.config;

import io.lenses.streamreactor.common.config.base.ConfigMap;
import io.lenses.streamreactor.common.config.base.RetryConfig;
import io.lenses.streamreactor.common.config.base.model.ConnectorPrefix;
import lombok.val;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GCPSettingsTest {

    private static final ConnectorPrefix connectorPrefix = new ConnectorPrefix("connect.gcpstorage");
    private static final GCPSettings gcpSettings = new GCPSettings(connectorPrefix);

    private static Stream<Arguments> provideRetryConfigData() {
        return Stream.of(
                Arguments.of("no values", 0, 0L, new RetryConfig(0, 0L)),
                Arguments.of("retry limit only", 10, 0L, new RetryConfig(10, 0L)),
                Arguments.of("interval only", 0, 20L, new RetryConfig(0, 20L)),
                Arguments.of("retry limit and interval", 30, 40L, new RetryConfig(30, 40L))
                );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("provideRetryConfigData")
    void testHttpRetryConfig(String testName, Object retries, Object interval, RetryConfig expected) {
        val configMap = new ConfigMap(Map.of(
                "connect.gcpstorage.http.max.retries", retries,
                "connect.gcpstorage.http.retry.interval", interval
        ));

        RetryConfig actual = gcpSettings.parseFromConfig(configMap).getHttpRetryConfig();
        assertEquals(expected, actual);
    }
}
