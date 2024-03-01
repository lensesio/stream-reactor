package io.lenses.java.streamreactor.connect.azure.eventhubs.source;

import static io.lenses.java.streamreactor.common.util.AsciiArtPrinter.printAsciiHeader;
import static java.util.Optional.ofNullable;

import io.lenses.java.streamreactor.common.util.JarManifest;
import io.lenses.java.streamreactor.connect.azure.eventhubs.config.AzureEventHubsConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.apache.kafka.connect.source.SourceConnector;

/**
 * Implementation of {@link SourceConnector} for Microsoft Azure EventHubs.
 */
@Slf4j
public class AzureEventHubsSourceConnector extends SourceConnector {

  private final JarManifest jarManifest =
      new JarManifest(getClass().getProtectionDomain().getCodeSource().getLocation());
  private Map<String, String> configProperties;

  @Override
  public void start(Map<String, String> props) {
    configProperties = props;
    new AzureEventHubsConfig(props);
    printAsciiHeader(jarManifest, "/azure-eventhubs-ascii.txt");
    ofNullable(context()).flatMap(context -> ofNullable(context.offsetStorageReader()))
        .ifPresent(TopicPartitionOffsetProvider::initialize);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return AzureEventHubsSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    log.info("Setting task configurations for {} workers.", maxTasks);
    List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
    IntStream.range(0, maxTasks).forEach(task -> taskConfigs.add(configProperties));
    return taskConfigs;
  }

  @Override
  public ExactlyOnceSupport exactlyOnceSupport(Map<String, String> connectorConfig) {
    return ExactlyOnceSupport.SUPPORTED;
  }

  @Override
  public void stop() {
  }

  @Override
  public ConfigDef config() {
    return AzureEventHubsConfig.getConfigDefinition();
  }

  @Override
  public String version() {
    return jarManifest.getVersion();
  }
}
