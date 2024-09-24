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

import static io.lenses.streamreactor.common.util.StringUtils.isBlank;

import cyclops.control.Try;
import io.lenses.streamreactor.common.exception.StreamReactorException;
import io.lenses.streamreactor.connect.reporting.config.ReportProducerConfigConst;
import io.lenses.streamreactor.connect.reporting.config.ReporterConfig;
import io.lenses.streamreactor.connect.reporting.model.RecordReport;
import io.lenses.streamreactor.connect.reporting.model.SinkRecordRecordReport;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
public abstract class ReportingController {

  private static final String EXCEPTION_WHILE_PRODUCING_MESSAGE =
      "Exception was thrown when sending report, will try again for next reports:";
  private static final String CLIENT_ID_DEFAULT = "ConnectorReportsProducer";
  private static final int DEFAULT_CLOSE_DURATION_IN_MILLIS = 500;
  private static final String TOPIC_ERROR = "If reporting is enabled then reporting kafka topic must be specified";

  @Getter
  private boolean senderEnabled;
  private final ReportHolder reportHolder;
  private final Producer<byte[], String> producer;
  private final ExecutorService executorService;
  private final String reportTopic;

  protected ReportingController(Map<String, Object> senderConfig) {

    this.senderEnabled =
        getEnabledBoolean(senderConfig
            .getOrDefault(ReportProducerConfigConst.REPORTING_ENABLED_CONFIG, "false"));

    this.reportTopic = senderEnabled ? getReportTopic(senderConfig.get(ReportProducerConfigConst.TOPIC)) : null;

    this.producer = senderEnabled ? createKafkaProducer(senderConfig) : null;
    this.reportHolder = senderEnabled ? new ReportHolder(null) : null;
    this.executorService = senderEnabled ? Executors.newFixedThreadPool(1) : null;
  }

  /**
   * Enqueues report for Kafka Producer to send.
   * 
   * @param report a {@link SinkRecordRecordReport} instance
   */
  public void enqueue(RecordReport report) {
    if (isSenderEnabled()) {
      reportHolder.enqueueReport(report);
    }
  }

  /**
   * Allows Kafka Producer to start reading for enqueued Reports then sending them periodically
   * to Kafka topic (specified in config).
   */
  public void start() {
    if (isSenderEnabled()) {
      executorService.submit(() -> {

        while (isSenderEnabled()) {
          RecordReport report = reportHolder.pollReport();
          if (report != null) {
            Optional<ProducerRecord<byte[], String>> optionalReport =
                report.produceReportRecord(reportTopic);
            Try.runWithCatch(() -> optionalReport.ifPresent(producer::send))
                .toFailedOption()
                .stream().forEach(ex -> log.warn(EXCEPTION_WHILE_PRODUCING_MESSAGE, ex));
          }
        }

      });
    }
  }

  /**
   * This method should be called before Connector closes in order to gracefully close KafkaProducer
   */
  public void close() {
    if (isSenderEnabled()) {
      Try.withCatch(() -> executorService.awaitTermination(DEFAULT_CLOSE_DURATION_IN_MILLIS, TimeUnit.MILLISECONDS));
      senderEnabled = false;
      producer.close(Duration.ofMillis(DEFAULT_CLOSE_DURATION_IN_MILLIS));
    }
  }

  private static String getReportTopic(Object senderConfig) {
    if (isBlank((String) senderConfig)) {
      throw new StreamReactorException(TOPIC_ERROR);
    }
    return (String) senderConfig;
  }

  private boolean getEnabledBoolean(Object o) {
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

  private Producer<byte[], String> createKafkaProducer(Map<String, Object> senderConfig) {
    senderConfig.put(ProducerConfig.CLIENT_ID_CONFIG, createProducerId());
    senderConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    senderConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return new KafkaProducer<>(senderConfig);
  }

  private String createProducerId() {
    return CLIENT_ID_DEFAULT + UUID.randomUUID();
  }

  public static class ErrorReportingController extends ReportingController {

    public ErrorReportingController(AbstractConfig connectorConfig) {
      super(ReporterConfig.getErrorReportingProducerConfig(connectorConfig));
    }
  }

  public static class SuccessReportingController extends ReportingController {

    public SuccessReportingController(AbstractConfig connectorConfig) {
      super(ReporterConfig.getSuccessReportingProducerConfig(connectorConfig));
    }
  }

}
