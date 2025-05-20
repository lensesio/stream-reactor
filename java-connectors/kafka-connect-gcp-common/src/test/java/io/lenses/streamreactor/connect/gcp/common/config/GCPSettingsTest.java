/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.gcp.common.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.lenses.streamreactor.common.config.base.RetryConfig;
import io.lenses.streamreactor.common.config.base.model.ConnectorPrefix;
import io.lenses.streamreactor.common.config.source.MapConfigSource;
import io.lenses.streamreactor.test.utils.EitherValues;
import lombok.val;

class GCPSettingsTest {

  private static final ConnectorPrefix connectorPrefix = new ConnectorPrefix("connect.gcpstorage");
  private static final GCPSettings gcpSettings = new GCPSettings(connectorPrefix);

  private static Stream<Arguments> provideRetryConfigData() {
    double defaultMultiplier = 1.0;
    return Stream.of(
        Arguments.of("no values", 0, 0L, defaultMultiplier, new RetryConfig(0, 0L, defaultMultiplier)),
        Arguments.of("retry limit only", 10, 0L, defaultMultiplier, new RetryConfig(10, 0L, defaultMultiplier)),
        Arguments.of("interval only", 0, 20L, defaultMultiplier, new RetryConfig(0, 20L, defaultMultiplier)),
        Arguments.of("retry limit and interval", 30, 40L, defaultMultiplier, new RetryConfig(30, 40L,
            defaultMultiplier)),
        Arguments.of("limit, interval and multiplier", 30, 40L, 10.0, new RetryConfig(30, 40L, 10.0)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("provideRetryConfigData")
  void testHttpRetryConfig(String testName, Object retries, Object interval, Double multiplier, RetryConfig expected) {
    val configMap =
        new MapConfigSource(
            Map.of(
                "connect.gcpstorage.http.max.retries", retries,
                "connect.gcpstorage.http.retry.timeout.multiplier", multiplier,
                "connect.gcpstorage.http.retry.interval", interval));

    val optionalRetryConfig = EitherValues.getRight(gcpSettings.parseFromConfig(configMap)).getHttpRetryConfig();
    assertThat(optionalRetryConfig)
        .isPresent()
        .contains(expected);
  }
}
