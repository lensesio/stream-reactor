package io.lenses.streamreactor.connect.azure.eventhubs.source;

import static io.lenses.streamreactor.common.util.AsciiArtPrinter.printAsciiHeader;

import io.lenses.streamreactor.common.util.JarManifest;
import io.lenses.streamreactor.connect.azure.eventhubs.config.AzureEventHubsConfigConstants;
import io.lenses.streamreactor.connect.azure.eventhubs.config.AzureEventHubsSourceConfig;
import io.lenses.streamreactor.connect.azure.eventhubs.util.KcqlConfigPort;
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
    parseAndValidateConfigs(props);
    printAsciiHeader(jarManifest, "/azure-eventhubs-ascii.txt");
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
    // connector-specific implementation not needed
  }

  @Override
  public ConfigDef config() {
    return AzureEventHubsSourceConfig.getConfigDefinition();
  }

  @Override
  public String version() {
    return jarManifest.getVersion();
  }

  private static void parseAndValidateConfigs(Map<String, String> props) {
    AzureEventHubsSourceConfig azureEventHubsSourceConfig = new AzureEventHubsSourceConfig(props);
    String kcqlMappings = azureEventHubsSourceConfig.getString(AzureEventHubsConfigConstants.KCQL_CONFIG);
    KcqlConfigPort.mapInputToOutputsFromConfig(kcqlMappings);
  }
}