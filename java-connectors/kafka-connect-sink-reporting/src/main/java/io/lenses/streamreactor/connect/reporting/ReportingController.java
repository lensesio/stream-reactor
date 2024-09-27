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

import cyclops.control.Option;
import io.lenses.streamreactor.connect.reporting.config.ReportProducerConfigConst;
import io.lenses.streamreactor.connect.reporting.config.ReporterConfig;
import io.lenses.streamreactor.connect.reporting.model.ReportingRecord;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.common.config.AbstractConfig;

import java.util.Map;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class ReportingController {

  protected Option<ReportSender> reportSender;

  public static ReportingController fromConfig(Map<String, Object> senderConfig) {

    val senderEnabled =
        getEnabledBoolean(senderConfig
            .getOrDefault(ReportProducerConfigConst.REPORTING_ENABLED_CONFIG, "false"));

    val reportSenderOption =
        (senderEnabled) ? Option.of(ReportSender.fromConfigMap(senderConfig)) : Option.<ReportSender>none();
    return new ReportingController(reportSenderOption);

  }

  /**
   * Enqueues report for Kafka Producer to send.
   * 
   * @param report a {@link ReportingRecord} instance
   */
  public void enqueue(ReportingRecord report) {
    reportSender.forEach(sender -> sender.enqueue(report));
  }

  /**
   * Allows Kafka Producer to start reading for enqueued Reports then sending them periodically
   * to Kafka topic (specified in config).
   */
  public void start() {
    reportSender.forEach(ReportSender::start);
  }

  /**
   * This method should be called before Connector closes in order to gracefully close KafkaProducer
   */
  public void close() {
    reportSender.forEach(ReportSender::close);
  }

  private static boolean getEnabledBoolean(Object o) {
    //TODO: check if we have something like
    // io.lenses.streamreactor.connect.cloud.common.config.ConfigParse#getBoolean
    if (Boolean.class.isAssignableFrom(o.getClass())) {
      return (Boolean) o;
    }
    if (String.class.isAssignableFrom(o.getClass())) {
      return Boolean.parseBoolean((String) o);
    }
    return false;
  }

  @NoArgsConstructor(access = AccessLevel.PRIVATE)
  public static class ErrorReportingController {

    public static ReportingController fromAbstractConfig(AbstractConfig connectorConfig) {
      return ReportingController.fromConfig(ReporterConfig.getErrorReportingProducerConfig(connectorConfig));
    }
  }

  @NoArgsConstructor(access = AccessLevel.PRIVATE)
  public static class SuccessReportingController {

    public static ReportingController fromAbstractConfig(AbstractConfig connectorConfig) {
      return ReportingController.fromConfig(ReporterConfig.getSuccessReportingProducerConfig(connectorConfig));
    }
  }

}
