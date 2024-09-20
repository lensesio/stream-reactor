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
package io.lenses.streamreactor.connect.reporting.config;

import static io.lenses.streamreactor.connect.reporting.config.ReportProducerConfigConst.BOOTSTRAP_SERVERS_CONFIG;
import static io.lenses.streamreactor.connect.reporting.config.ReportProducerConfigConst.REPORTING_ENABLED_CONFIG;
import static io.lenses.streamreactor.connect.reporting.config.ReportProducerConfigConst.SASL_JAAS_CONFIG;
import static io.lenses.streamreactor.connect.reporting.config.ReportProducerConfigConst.SASL_MECHANISM_CONFIG;
import static io.lenses.streamreactor.connect.reporting.config.ReportProducerConfigConst.SECURITY_PROTOCOL_CONFIG;
import static io.lenses.streamreactor.connect.reporting.config.ReportProducerConfigConst.TOPIC;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.junit.jupiter.api.Test;

class ReporterConfigTest {

  private static final String SUCCESS_REPORTING_CONFIG_PREFIX = "connect.reporting.success.config.";
  private static final UnaryOperator<String> SUCCESS_CONFIG_NAME_PREFIX_APPENDER =
      name -> SUCCESS_REPORTING_CONFIG_PREFIX + name;
  private static final String ERROR_REPORTING_CONFIG_PREFIX = "connect.reporting.error.config.";
  private static final UnaryOperator<String> ERROR_CONFIG_NAME_PREFIX_APPENDER =
      name -> ERROR_REPORTING_CONFIG_PREFIX + name;

  private static final String NONPRODUCER_KEY_NAME = "np.name";
  private static final String NONPRODUCER_KEY_NAME_DOCS = "some additional name property";
  private static final String NONPRODUCER_KEY_NAME_DEFAULT = "default";
  private static final String NONPRODUCER_KEY_GROUP = "nonproducer";
  private static final int NONPRODUCER_KEY_ORDER = 1;
  private static final Importance NONPRODUCER_KEY_IMPORTANCE = Importance.HIGH;

  private static final Set<String> PRODUCER_PROPERTIES =
      Set.of(REPORTING_ENABLED_CONFIG, SASL_MECHANISM_CONFIG,
          SASL_JAAS_CONFIG, SECURITY_PROTOCOL_CONFIG,
          BOOTSTRAP_SERVERS_CONFIG, TOPIC);
  private static final Integer PRODUCER_PROPERTIES_SIZE = PRODUCER_PROPERTIES.size();

  @Test
  void withErrorRecordReportingSupportShouldAppendErrorReportingConfigs() {
    //given
    ConfigKey nonProducerConfigKey = getNonProducerConfigKey();
    ConfigDef configDef =
        new ConfigDef()
            .define(nonProducerConfigKey);

    //when
    ConfigDef configDefWithReporting = ReporterConfig.withErrorRecordReportingSupport(configDef);

    //then
    assertThat(configDefWithReporting).isNotNull();
    assertThat(configDefWithReporting.configKeys().size())
        .isEqualTo(PRODUCER_PROPERTIES_SIZE + 1);

    Set<String> errorReportProducerProperties = getErrorReportProducerProperties();
    errorReportProducerProperties.add(nonProducerConfigKey.name);
    assertProperties(errorReportProducerProperties, configDefWithReporting);

  }

  @Test
  void withSuccessRecordReportingSupportShouldAppendSuccessReportingConfigs() {
    //given
    ConfigKey nonProducerConfigKey = getNonProducerConfigKey();
    ConfigDef configDef =
        new ConfigDef()
            .define(nonProducerConfigKey);

    //when
    ConfigDef configDefWithReporting = ReporterConfig.withSuccessRecordReportingSupport(configDef);

    //then
    assertThat(configDefWithReporting).isNotNull();
    assertThat(configDefWithReporting.configKeys().size())
        .isEqualTo(PRODUCER_PROPERTIES_SIZE + 1);

    Set<String> successReportProducerProperties = getSuccessReportProducerProperties();
    successReportProducerProperties.add(nonProducerConfigKey.name);
    assertProperties(successReportProducerProperties, configDefWithReporting);
  }

  @Test
  void getErrorReportingProducerConfigShouldCallOriginalsWithPrefix() {
    //given
    Map<String, Object> expectedResult = new HashMap<>();
    AbstractConfig config = mockAbstractConfigWithPrefix(expectedResult, ERROR_REPORTING_CONFIG_PREFIX);

    //when
    Map<String, Object> producerMap = ReporterConfig.getErrorReportingProducerConfig(config);

    //then
    verify(config).originalsWithPrefix(ERROR_REPORTING_CONFIG_PREFIX, true);
    assertEquals(expectedResult, producerMap);
  }

  @Test
  void getSuccessReportingProducerConfigShouldCallOriginalsWithPrefix() {
    //given
    Map<String, Object> expectedResult = new HashMap<>();
    AbstractConfig config = mockAbstractConfigWithPrefix(expectedResult, SUCCESS_REPORTING_CONFIG_PREFIX);

    //when
    Map<String, Object> producerMap = ReporterConfig.getSuccessReportingProducerConfig(config);

    //then
    verify(config).originalsWithPrefix(SUCCESS_REPORTING_CONFIG_PREFIX, true);
    assertEquals(expectedResult, producerMap);
  }

  private static AbstractConfig mockAbstractConfigWithPrefix(
      Map<String, Object> expectedResult,
      String configPropNamePrefix) {
    AbstractConfig config = mock(AbstractConfig.class);
    PRODUCER_PROPERTIES.forEach(prop -> when(config.originalsWithPrefix(configPropNamePrefix, true)).thenReturn(
        expectedResult));

    return config;
  }

  private static void assertProperties(Set<String> errorReportProducerProperties, ConfigDef configDefWithReporting) {
    errorReportProducerProperties.forEach(prop -> assertTrue(configDefWithReporting.configKeys().containsKey(prop))
    );
  }

  private static ConfigKey getNonProducerConfigKey() {
    return new ConfigKey(
        NONPRODUCER_KEY_NAME,
        Type.STRING,
        NONPRODUCER_KEY_NAME_DEFAULT,
        null,
        NONPRODUCER_KEY_IMPORTANCE,
        NONPRODUCER_KEY_NAME_DOCS,
        NONPRODUCER_KEY_GROUP,
        NONPRODUCER_KEY_ORDER,
        Width.SHORT,
        NONPRODUCER_KEY_NAME,
        null,
        null,
        false
    );
  }

  private static Set<String> getErrorReportProducerProperties() {
    return PRODUCER_PROPERTIES.stream()
        .map(ERROR_CONFIG_NAME_PREFIX_APPENDER).collect(Collectors.toSet());

  }

  private static Set<String> getSuccessReportProducerProperties() {
    return PRODUCER_PROPERTIES.stream()
        .map(SUCCESS_CONFIG_NAME_PREFIX_APPENDER).collect(Collectors.toSet());
  }
}
