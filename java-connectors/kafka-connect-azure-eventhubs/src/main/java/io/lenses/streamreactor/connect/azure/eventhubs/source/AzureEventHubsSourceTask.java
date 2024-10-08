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
package io.lenses.streamreactor.connect.azure.eventhubs.source;

import static io.lenses.streamreactor.common.util.EitherUtils.unpackOrThrow;
import static java.util.Optional.ofNullable;

import cyclops.control.Either;
import io.lenses.kcql.Kcql;
import io.lenses.streamreactor.common.exception.StreamReactorException;
import io.lenses.streamreactor.common.util.EitherUtils;
import io.lenses.streamreactor.common.util.JarManifest;
import io.lenses.streamreactor.connect.azure.eventhubs.config.AzureEventHubsConfigConstants;
import io.lenses.streamreactor.connect.azure.eventhubs.config.AzureEventHubsSourceConfig;
import io.lenses.streamreactor.connect.azure.eventhubs.util.EventHubsKcqlMappingsValidator;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.OffsetStorageReader;

/**
 * Implementation of {@link SourceTask} for Microsoft Azure EventHubs.
 */
@Slf4j
public class AzureEventHubsSourceTask extends SourceTask {

  private static final Duration ONE_SECOND_DURATION = Duration.of(1, ChronoUnit.SECONDS);
  private Duration closeTimeout;
  private static final int RECORDS_QUEUE_DEFAULT_SIZE = 10;
  private final JarManifest jarManifest;
  private EventHubsKafkaConsumerController eventHubsKafkaConsumerController;
  private BlockingQueueProducerProvider blockingQueueProducerProvider;

  public AzureEventHubsSourceTask() {
    jarManifest =
        unpackOrThrow(JarManifest
            .produceFromClass(getClass())
        );
  }

  public AzureEventHubsSourceTask(JarManifest jarManifest) {
    this.jarManifest = jarManifest;
  }

  @Override
  public String version() {
    return jarManifest.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    OffsetStorageReader offsetStorageReader =
        ofNullable(this.context).flatMap(
            context -> ofNullable(context.offsetStorageReader())).orElseThrow();
    AzureEventHubsSourceConfig azureEventHubsSourceConfig = new AzureEventHubsSourceConfig(props);
    TopicPartitionOffsetProvider topicPartitionOffsetProvider = new TopicPartitionOffsetProvider(offsetStorageReader);

    ArrayBlockingQueue<ConsumerRecords<byte[], byte[]>> recordsQueue =
        new ArrayBlockingQueue<>(RECORDS_QUEUE_DEFAULT_SIZE);
    Either<StreamReactorException, List<Kcql>> mappedInputsOutputsEither =
        EventHubsKcqlMappingsValidator.mapInputToOutputsFromConfig(
            azureEventHubsSourceConfig.getString(AzureEventHubsConfigConstants.KCQL_CONFIG));
    Map<String, String> inputToOutputTopics =
        EitherUtils.unpackOrThrow(mappedInputsOutputsEither)
            .stream().collect(Collectors.toUnmodifiableMap(Kcql::getSource, Kcql::getTarget));

    blockingQueueProducerProvider = new BlockingQueueProducerProvider(topicPartitionOffsetProvider);
    KafkaByteBlockingQueuedProducer producer =
        blockingQueueProducerProvider.createProducer(azureEventHubsSourceConfig, recordsQueue, inputToOutputTopics);
    EventHubsKafkaConsumerController kafkaConsumerController =
        new EventHubsKafkaConsumerController(producer, recordsQueue, inputToOutputTopics);
    initialize(kafkaConsumerController, azureEventHubsSourceConfig);
  }

  /**
   * Initializes the Task. This method shouldn't be called if start() was already called with
   * {@link EventHubsKafkaConsumerController} instance.
   *
   * @param eventHubsKafkaConsumerController {@link EventHubsKafkaConsumerController} for this task
   * @param azureEventHubsSourceConfig       config for task
   */
  public void initialize(EventHubsKafkaConsumerController eventHubsKafkaConsumerController,
      AzureEventHubsSourceConfig azureEventHubsSourceConfig) {
    this.eventHubsKafkaConsumerController = eventHubsKafkaConsumerController;
    closeTimeout =
        Duration.of(azureEventHubsSourceConfig.getInt(AzureEventHubsConfigConstants.CONSUMER_CLOSE_TIMEOUT),
            ChronoUnit.SECONDS);
    log.info("{} initialised.", getClass().getSimpleName());
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    List<SourceRecord> poll =
        eventHubsKafkaConsumerController.poll(ONE_SECOND_DURATION);
    return poll.isEmpty() ? null : poll;
  }

  @Override
  public void stop() {
    ofNullable(eventHubsKafkaConsumerController)
        .ifPresent(consumerController -> consumerController.close(closeTimeout));
  }
}
