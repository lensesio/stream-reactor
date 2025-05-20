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
package io.lenses.streamreactor.connect.reporting;

import cyclops.control.Either;
import cyclops.control.Option;
import cyclops.control.Try;
import cyclops.instances.control.OptionInstances;
import cyclops.typeclasses.Do;
import io.lenses.streamreactor.common.config.source.ConfigSource;
import io.lenses.streamreactor.common.config.source.MapConfigSource;
import io.lenses.streamreactor.common.exception.StreamReactorException;
import io.lenses.streamreactor.common.util.StringUtils;
import io.lenses.streamreactor.connect.reporting.config.ReportProducerConfigConst;
import io.lenses.streamreactor.connect.reporting.model.ConnectorSpecificRecordData;
import io.lenses.streamreactor.connect.reporting.model.RecordConverter;
import io.lenses.streamreactor.connect.reporting.model.ReportingRecord;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.lenses.streamreactor.common.util.EitherUtils.unpackOrThrow;
import static io.lenses.streamreactor.common.util.TryHandler.logAndDiscardTry;
import static java.util.function.Function.identity;

/**
 * @param <C> the type of connector-specific record data
 */
@Slf4j
@AllArgsConstructor
@Getter
public class ReportSender<C extends ConnectorSpecificRecordData> {

  private static final String CLIENT_ID_PREFIX = "http-sink-reporter-";
  private static final String TOPIC_ERROR = "If reporting is enabled then reporting kafka topic must be specified";
  private static final String EXCEPTION_WHILE_PRODUCING_MESSAGE =
      "Exception was thrown when sending report, will try again for next reports:";
  private static final int DEFAULT_CLOSE_DURATION_IN_MILLIS = 500;
  private static final int DEFAULT_QUEUES_SIZE = 1000;
  private static final Integer PARTITION_NOT_DEFINED = -1;

  private final String reportingClientId;
  private final RecordConverter<C> recordConverter;
  private final ReportHolder<C> reportHolder;
  private final Producer<byte[], String> producer;
  private final ScheduledExecutorService executorService;

  public void enqueue(ReportingRecord<C> report) {
    reportHolder.enqueueReport(report);
  }

  public void start() {
    log.info("Starting reporting Kafka Producer with clientId:" + reportingClientId);
    executorService.scheduleWithFixedDelay(
        () -> reportHolder.pollReport().forEach(this::sendReport), 0, 50, TimeUnit.MILLISECONDS);
  }

  private void sendReport(ReportingRecord<C> report) {
    log.debug("Sending Report");

    logAndDiscardTry(
        Try.withCatch(() -> recordConverter.convert(report), Exception.class), EXCEPTION_WHILE_PRODUCING_MESSAGE
    ).flatMap(identity())
        .forEach3(
            producerRecord -> logAndDiscardTry(
                Try.withCatch(() -> producer.send(producerRecord), Exception.class),
                EXCEPTION_WHILE_PRODUCING_MESSAGE
            ),
            (producerRecord, future) -> logAndDiscardTry(
                resolveFuture(future),
                EXCEPTION_WHILE_PRODUCING_MESSAGE
            ),
            (producerRecord, future, md) -> {
              log.debug("Report send complete");
              return md;
            }
        );

  }

  private static Try<RecordMetadata, Exception> resolveFuture(Future<RecordMetadata> fut) {
    return Try.withCatch(
        () -> fut.get(5L, TimeUnit.SECONDS), InterruptedException.class, ExecutionException.class,
        TimeoutException.class
    );
  }

  public void close() {
    log.info("Stopping reporting Kafka Producer with clientId:" + reportingClientId);
    Try.withCatch(() -> executorService.awaitTermination(DEFAULT_CLOSE_DURATION_IN_MILLIS, TimeUnit.MILLISECONDS));
    producer.close(Duration.ofMillis(DEFAULT_CLOSE_DURATION_IN_MILLIS));
  }

  protected static <C extends ConnectorSpecificRecordData> ReportSender<C> fromConfigMap(
      Function<ReportingMessagesConfig, RecordConverter<C>> recordConverter,
      Map<String, Object> senderConfig) {

    val configSource = new MapConfigSource(senderConfig);

    // TODO: Return Either<StreamReactorException, ReportSender> instead of throwing exception here
    val reportTopic = unpackOrThrow(getReportTopic(configSource));
    val reportTopicPartition = getReportTopicPartition(configSource);
    val reportingMessagesConfig = new ReportingMessagesConfig(reportTopic, reportTopicPartition);

    final String reportingClientId = CLIENT_ID_PREFIX + UUID.randomUUID();

    val producer = createKafkaProducer(senderConfig, reportingClientId);
    val queue = new ArrayBlockingQueue<ReportingRecord<C>>(DEFAULT_QUEUES_SIZE);
    val reportHolder = new ReportHolder<C>(queue);
    val executorService = Executors.newScheduledThreadPool(1);

    return new ReportSender<>(reportingClientId, recordConverter.apply(reportingMessagesConfig), reportHolder, producer,
        executorService);
  }

  private static Either<StreamReactorException, String> getReportTopic(ConfigSource mapConfigSource) {
    return Option
        .fromOptional(mapConfigSource.getString(ReportProducerConfigConst.TOPIC))
        .filterNot(StringUtils::isBlank)
        .toEither(new StreamReactorException(TOPIC_ERROR));
  }

  private static Option<Integer> getReportTopicPartition(ConfigSource mapConfigSource) {
    return Option
        .fromOptional(mapConfigSource.getInt(ReportProducerConfigConst.PARTITION))
        .filterNot(partition -> partition.compareTo(PARTITION_NOT_DEFINED) == 0);
  }

  private static Producer<byte[], String> createKafkaProducer(
      Map<String, Object> senderConfig,
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
