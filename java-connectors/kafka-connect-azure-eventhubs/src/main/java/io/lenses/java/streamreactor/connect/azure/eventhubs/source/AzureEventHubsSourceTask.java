package io.lenses.java.streamreactor.connect.azure.eventhubs.source;

import static java.util.Optional.ofNullable;

import io.lenses.java.streamreactor.common.util.JarManifest;
import io.lenses.java.streamreactor.connect.azure.eventhubs.config.AzureEventHubsConfigConstants;
import io.lenses.java.streamreactor.connect.azure.eventhubs.config.AzureEventHubsSourceConfig;
import io.lenses.java.streamreactor.connect.azure.eventhubs.util.ExceptionProviders;
import io.lenses.java.streamreactor.connect.azure.eventhubs.util.KcqlConfigPort;
import io.lenses.kcql.Kcql;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.OffsetStorageReader;

/**
 * Implementation of {@link SourceTask} for Microsoft Azure EventHubs.
 */
@Slf4j
public class AzureEventHubsSourceTask extends SourceTask {

  private Duration closeTimeout;
  private static final int RECORDS_QUEUE_DEFAULT_SIZE = 10;
  private final JarManifest jarManifest;
  private EventHubsKafkaConsumerController eventHubsKafkaConsumerController;
  private BlockingQueueProducerProvider blockingQueueProducerProvider;

  public AzureEventHubsSourceTask() {
    jarManifest = new JarManifest(getClass().getProtectionDomain().getCodeSource().getLocation());
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
    OffsetStorageReader offsetStorageReader = ofNullable(this.context).flatMap(
        context -> ofNullable(context.offsetStorageReader())).orElseThrow();
    AzureEventHubsSourceConfig azureEventHubsSourceConfig = new AzureEventHubsSourceConfig(props);
    TopicPartitionOffsetProvider topicPartitionOffsetProvider = new TopicPartitionOffsetProvider(offsetStorageReader);

    ArrayBlockingQueue<ConsumerRecords<byte[], byte[]>> recordsQueue = new ArrayBlockingQueue<>(
        RECORDS_QUEUE_DEFAULT_SIZE);
    blockingQueueProducerProvider = new BlockingQueueProducerProvider(topicPartitionOffsetProvider);
    KafkaByteBlockingQueuedProducer producer = blockingQueueProducerProvider.createProducer(
        azureEventHubsSourceConfig, recordsQueue);
    List<String> outputTopics = getOutputTopicsFromConfig(azureEventHubsSourceConfig);
    EventHubsKafkaConsumerController kafkaConsumerController = new EventHubsKafkaConsumerController(
        producer, recordsQueue, outputTopics);
    initialize(kafkaConsumerController, azureEventHubsSourceConfig);
  }

  /**
   * Initializes the Task. This method shouldn't be called if start() was already called with
   * {@link EventHubsKafkaConsumerController} instance.
   *
   * @param eventHubsKafkaConsumerController {@link EventHubsKafkaConsumerController} for this task
   * @param azureEventHubsSourceConfig config for task
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
    List<SourceRecord> poll = eventHubsKafkaConsumerController.poll(
        Duration.of(1, ChronoUnit.SECONDS));
    return poll.isEmpty() ? null : poll;
  }

  @Override
  public void stop() {
    ofNullable(eventHubsKafkaConsumerController)
        .ifPresent(consumerController -> consumerController.close(closeTimeout));
  }

  /**
   * Returns output topics (if specified in config) or throws {@link ConfigException}.
   *
   * @param azureEventHubsSourceConfig task configuration
   * @return output topics list
   */
  private List<String> getOutputTopicsFromConfig(
      AzureEventHubsSourceConfig azureEventHubsSourceConfig) {
    Kcql kcql = KcqlConfigPort.parseMultipleKcqlStatementsPickingOnlyFirst(
        azureEventHubsSourceConfig.getString(AzureEventHubsConfigConstants.KCQL_CONFIG));
    List<String> outputTopics = Arrays.stream(kcql.getTarget().split(",")).collect(Collectors.toList());
    if (outputTopics.isEmpty()) {
      throw ExceptionProviders.OUTPUT_TOPIC_CONFIG_EXCEPTION_SUPPLIER.get();
    }
    return outputTopics;
  }
}
