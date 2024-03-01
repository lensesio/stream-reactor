package io.lenses.java.streamreactor.connect.azure.eventhubs.source;

import static java.util.Optional.ofNullable;

import io.lenses.java.streamreactor.common.util.JarManifest;
import io.lenses.java.streamreactor.connect.azure.eventhubs.config.AzureEventHubsConfig;
import io.lenses.java.streamreactor.connect.azure.eventhubs.config.AzureEventHubsConfigConstants;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

/**
 * Implementation of {@link SourceTask} for Microsoft Azure EventHubs.
 */
@Slf4j
public class AzureEventHubsSourceTask extends SourceTask {

  private static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.of(30, ChronoUnit.SECONDS);
  private final JarManifest jarManifest;
  private EventHubsKafkaConsumerController eventHubsKafkaConsumerController;

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
    int pollQueueSize = Integer.parseInt(props.get(AzureEventHubsConfigConstants.POLL_QUEUE_SIZE));
    eventHubsKafkaConsumerController = new EventHubsKafkaConsumerController(
        new AzureEventHubsConfig(props), new KafkaConsumerProvider(),
        new ArrayBlockingQueue<>(pollQueueSize));
    String topic = props.get(AzureEventHubsConfigConstants.EVENTHUB_NAME);
    ofNullable(this.context).flatMap(context -> ofNullable(context.offsetStorageReader()))
        .ifPresent(TopicPartitionOffsetProvider::initialize);
    initialize(eventHubsKafkaConsumerController);
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
