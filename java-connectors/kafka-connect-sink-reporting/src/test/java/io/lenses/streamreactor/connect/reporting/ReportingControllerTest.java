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
package io.lenses.streamreactor.connect.reporting;

import io.lenses.streamreactor.connect.reporting.ReportingController.ErrorReportingController;
import io.lenses.streamreactor.connect.reporting.config.ReportProducerConfigConst;
import io.lenses.streamreactor.connect.reporting.config.ReporterConfig;
import io.lenses.streamreactor.connect.reporting.model.generic.ReportingRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Properties;
import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class ReportingControllerTest {

  private static final String KAFKA_BOOTSTRAP_PROPERTY = "bootstrap.local";
  private static final int DEFAULT_CLOSE_DURATION_IN_MILLIS = 500;

  private static final String TOPIC_PROPERTY = "reporting";

  @Test
  void enqueueShouldCallEnqueueOnHolderIfEnabled() {
    //given
    final AbstractConfig disabledReportingConfig = createEnabledReportingConfig();
    ReportingRecord report = mock(ReportingRecord.class);

    ErrorReportingController reportingController;
    ReportHolder reportHolder;
    try (MockedConstruction<KafkaProducer> ignored =
        Mockito.mockConstruction(KafkaProducer.class)) {
      try (MockedConstruction<ReportHolder> mockedHolderConstr =
          Mockito.mockConstruction(ReportHolder.class)) {
        reportingController = new ErrorReportingController(disabledReportingConfig);
        reportHolder = mockedHolderConstr.constructed().get(0);
      }
    }

    //when
    reportingController.enqueue(report);

    //then
    reportHolder.enqueueReport(report);
  }

  @Test
  void enqueueShouldCallEnqueueOnHolderIfDisabled() {
    //given
    final AbstractConfig disabledReportingConfig = createDisabledReportingConfig();
    ReportingRecord report = mock(ReportingRecord.class);

    ErrorReportingController reportingController;
    try (MockedConstruction<KafkaProducer> ignored =
        Mockito.mockConstruction(KafkaProducer.class); MockedConstruction<ReportHolder> mockedHolderConstr =
            Mockito.mockConstruction(ReportHolder.class)) {
      reportingController = new ErrorReportingController(disabledReportingConfig);
      assertTrue(mockedHolderConstr.constructed().isEmpty());
    }

    //when
    reportingController.enqueue(report);

    //then
  }

  @Test
  void closeShouldCloseKafkaProducer() {
    //given
    final AbstractConfig enabledReportingConfig = createEnabledReportingConfig();
    KafkaProducer kafkaProducer;

    //when
    ErrorReportingController reportingController;
    try (MockedConstruction<KafkaProducer> mocked = Mockito.mockConstruction(KafkaProducer.class)) {
      reportingController = new ErrorReportingController(enabledReportingConfig);
      kafkaProducer = mocked.constructed().get(0);
    }
    reportingController.close();

    //then
    verify(kafkaProducer).close(eq(Duration.of(DEFAULT_CLOSE_DURATION_IN_MILLIS, ChronoUnit.MILLIS)));
  }

  @Test
  void isSenderEnabledReturnsFalseForDisabledReporterConfig() {
    //given
    AbstractConfig disabledReportingConfig = createDisabledReportingConfig();

    //when
    ErrorReportingController reportingController = new ErrorReportingController(disabledReportingConfig);

    //then
    assertFalse(reportingController.isSenderEnabled());
  }

  @Test
  void isSenderEnabledReturnsTrueForEnabledReporterConfig() {
    //given
    final AbstractConfig enabledReportingConfig = createEnabledReportingConfig();

    //when
    ErrorReportingController reportingController;
    try (MockedConstruction<KafkaProducer> ignored = Mockito.mockConstruction(KafkaProducer.class)) {
      reportingController = new ErrorReportingController(enabledReportingConfig);
    }

    //then
    assertTrue(reportingController.isSenderEnabled());
  }

  private AbstractConfig createDisabledReportingConfig() {

    final String ERROR_REPORTING_CONFIG_PREFIX = "connect.reporting.error.config.";
    final UnaryOperator<String> ERROR_CONFIG_NAME_PREFIX_APPENDER =
        name -> ERROR_REPORTING_CONFIG_PREFIX + name;

    Properties properties = new Properties();
    properties.put(ERROR_CONFIG_NAME_PREFIX_APPENDER
        .apply(ReportProducerConfigConst.REPORTING_ENABLED_CONFIG), "false");
    return new TestingErrorReportingConfig(properties);
  }

  private AbstractConfig createEnabledReportingConfig() {

    final String ERROR_REPORTING_CONFIG_PREFIX = "connect.reporting.error.config.";
    final UnaryOperator<String> ERROR_CONFIG_NAME_PREFIX_APPENDER =
        name -> ERROR_REPORTING_CONFIG_PREFIX + name;

    Properties properties = new Properties();
    properties.put(ERROR_CONFIG_NAME_PREFIX_APPENDER
        .apply(ReportProducerConfigConst.REPORTING_ENABLED_CONFIG), "true");
    properties.put(ERROR_CONFIG_NAME_PREFIX_APPENDER
        .apply(ReportProducerConfigConst.BOOTSTRAP_SERVERS_CONFIG), KAFKA_BOOTSTRAP_PROPERTY);
    properties.put(ERROR_CONFIG_NAME_PREFIX_APPENDER
        .apply(ReportProducerConfigConst.TOPIC), TOPIC_PROPERTY);
    return new TestingErrorReportingConfig(properties);
  }

  static class TestingErrorReportingConfig extends AbstractConfig {

    private static final ConfigDef configDef =
        ReporterConfig.withErrorRecordReportingSupport(new ConfigDef());

    public TestingErrorReportingConfig(Map<?, ?> properties) {
      super(configDef, properties);
    }
  }
}
