package io.lenses.java.streamreactor.connect.azure.eventhubs.source;

import static java.util.Optional.ofNullable;

import io.lenses.java.streamreactor.common.util.JarManifest;
import io.lenses.java.streamreactor.connect.azure.eventhubs.config.AzureEventHubsConfig;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
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

  private static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.of(30, ChronoUnit.SECONDS);
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
    TopicPartitionOffsetProvider topicPartitionOffsetProvider = new TopicPartitionOffsetProvider(offsetStorageReader);

    ArrayBlockingQueue<ConsumerRecords<String, String>> recordsQueue = new ArrayBlockingQueue<>(
        RECORDS_QUEUE_DEFAULT_SIZE);
    blockingQueueProducerProvider = new BlockingQueueProducerProvider(topicPartitionOffsetProvider);
    BlockingQueuedKafkaProducer producer = blockingQueueProducerProvider.createProducer(
        new AzureEventHubsConfig(props), recordsQueue);
    EventHubsKafkaConsumerController kafkaConsumerController = new EventHubsKafkaConsumerController(
        producer, recordsQueue);
    initialize(kafkaConsumerController);
  }

  /**
   * Initializes the Task. This method shouldn't be called if start() was already called with
   * {@link EventHubsKafkaConsumerController} instance.
   *
   * @param eventHubsKafkaConsumerController {@link EventHubsKafkaConsumerController} for this task
   */
  public void initialize(EventHubsKafkaConsumerController eventHubsKafkaConsumerController) {
    this.eventHubsKafkaConsumerController = eventHubsKafkaConsumerController;
    log.info("{} initialising.", getClass().getSimpleName());
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
        .ifPresent(consumerController -> consumerController.close(DEFAULT_CLOSE_TIMEOUT));
  }
}
