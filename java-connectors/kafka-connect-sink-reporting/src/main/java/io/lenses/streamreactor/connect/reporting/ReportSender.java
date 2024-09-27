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
import cyclops.control.Try;
import io.lenses.streamreactor.common.exception.StreamReactorException;
import io.lenses.streamreactor.connect.reporting.config.ReportProducerConfigConst;
import io.lenses.streamreactor.connect.reporting.model.ProducerRecordConverter;
import io.lenses.streamreactor.connect.reporting.model.ReportingRecord;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.lenses.streamreactor.common.util.StringUtils.isBlank;

@Slf4j
@AllArgsConstructor
@Getter
public class ReportSender {

  private static final String CLIENT_ID_PREFIX = "http-sink-reporter-";
  private static final String TOPIC_ERROR = "If reporting is enabled then reporting kafka topic must be specified";
  private static final String EXCEPTION_WHILE_PRODUCING_MESSAGE =
      "Exception was thrown when sending report, will try again for next reports:";
  private static final int DEFAULT_CLOSE_DURATION_IN_MILLIS = 500;

  private final ProducerRecordConverter producerRecordConverter;
  private final String reportingClientId;
  private final ReportHolder reportHolder;
  private final Producer<byte[], String> producer;
  private final ScheduledExecutorService executorService;
  private final String reportTopic;

  public void enqueue(ReportingRecord report) {
    reportHolder.enqueueReport(report);
  }

  public void start() {
    log.info("Starting reporting Kafka Producer with clientId:" + reportingClientId);
    executorService.scheduleWithFixedDelay(
        () -> reportHolder.pollReport().forEach(
            report -> {
              Option<ProducerRecord<byte[], String>> optionalReport =
                  producerRecordConverter.convert(report, reportTopic);
              Try.runWithCatch(() -> optionalReport.map(producer::send))
                  .toFailedOption()
                  .stream()
                  .forEach(ex -> log.warn(EXCEPTION_WHILE_PRODUCING_MESSAGE, ex));
            }
        ), 0, 1, TimeUnit.SECONDS);
  }

  public void close() {
    log.info("Stopping reporting Kafka Producer with clientId:" + reportingClientId);
    Try.withCatch(() -> executorService.awaitTermination(DEFAULT_CLOSE_DURATION_IN_MILLIS, TimeUnit.MILLISECONDS));
    producer.close(Duration.ofMillis(DEFAULT_CLOSE_DURATION_IN_MILLIS));
  }

  protected static ReportSender fromConfigMap(Map<String, Object> senderConfig) {
    val reportTopic = getReportTopic(senderConfig.get(ReportProducerConfigConst.TOPIC));

    final String reportingClientId = CLIENT_ID_PREFIX + UUID.randomUUID();

    val producer = createKafkaProducer(senderConfig, reportingClientId);
    val reportHolder = new ReportHolder(null);
    val executorService = Executors.newScheduledThreadPool(1);

    val producerRecordConverter = new ProducerRecordConverter();
    return new ReportSender(producerRecordConverter, reportingClientId, reportHolder, producer, executorService,
        reportTopic);
  }

  protected static String getReportTopic(Object senderConfig) {
    if (isBlank((String) senderConfig)) {
      throw new StreamReactorException(TOPIC_ERROR);
    }
    return (String) senderConfig;
  }

  private static Producer<byte[], String> createKafkaProducer(Map<String, Object> senderConfig,
      String reportingClientId) {
    return new KafkaProducer<>(addExtraConfig(senderConfig, reportingClientId));
  }

  protected static Map<String, Object> addExtraConfig(Map<String, Object> senderConfig, String reportingClientId) {
    return Stream.concat(
        senderConfig.entrySet().stream(),
        Stream.of(
            Map.entry(ProducerConfig.CLIENT_ID_CONFIG, reportingClientId),
            Map.entry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName()),
            Map.entry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
        )
    )
        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
