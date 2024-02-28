package io.lenses.java.streamreactor.connect.azure.eventhubs.source;

import static io.lenses.java.streamreactor.common.util.AsciiArtPrinter.printAsciiHeader;
import static java.util.Optional.ofNullable;

import io.lenses.java.streamreactor.common.util.JarManifest;
import io.lenses.java.streamreactor.connect.azure.eventhubs.config.AzureEventHubsConfig;
import io.lenses.java.streamreactor.connect.azure.eventhubs.config.AzureEventHubsConfigConstants;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;

/**
 * Implementation of {@link SourceTask} for Microsoft Azure EventHubs.
 */
@Slf4j
public class AzureEventHubsSourceTask extends SourceTask {

  private static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.of(30, ChronoUnit.SECONDS);
  private static final int BLOCKING_QUEUE_DEFAULT_CAPACITY = 20;
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
    Optional<SourceTaskContext> contextOptional = ofNullable(context);
    Map<String, String> configProps = null;
    if (contextOptional.isPresent()) {
      configProps = context.configs();
      ofNullable(context.offsetStorageReader())
          .ifPresent(TopicPartitionOffsetProvider::initialize);
    }

    if (configProps == null || configProps.isEmpty()) {
      configProps = props;
    }

    eventHubsKafkaConsumerController = new EventHubsKafkaConsumerController(
        new AzureEventHubsConfig(configProps), new KafkaConsumerProvider(),
        new ArrayBlockingQueue<>(BLOCKING_QUEUE_DEFAULT_CAPACITY));
    String topic = configProps.get(AzureEventHubsConfigConstants.EVENTHUB_NAME);
    initialize(eventHubsKafkaConsumerController, topic);
  }

  /**
   * Initializes the Task. This method shouldn't be called if start() was already called with
   * {@link EventHubsKafkaConsumerController} instance.
   *
   * @param eventHubsKafkaConsumerController {@link EventHubsKafkaConsumerController} for this task
   * @param topic topic to subscribe to
   */
  public void initialize(EventHubsKafkaConsumerController eventHubsKafkaConsumerController,
      String topic) {
    this.eventHubsKafkaConsumerController = eventHubsKafkaConsumerController;
    log.info("{} initialising.", getClass().getSimpleName());
    printAsciiHeader(jarManifest, "/azure-eventhubs-ascii.txt");
    eventHubsKafkaConsumerController.subscribeToTopic(topic);
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
