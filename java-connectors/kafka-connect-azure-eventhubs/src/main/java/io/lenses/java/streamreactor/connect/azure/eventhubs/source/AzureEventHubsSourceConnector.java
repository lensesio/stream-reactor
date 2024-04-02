package io.lenses.java.streamreactor.connect.azure.eventhubs.source;

import static io.lenses.java.streamreactor.common.util.AsciiArtPrinter.printAsciiHeader;

import io.lenses.java.streamreactor.common.util.JarManifest;
import io.lenses.java.streamreactor.connect.azure.eventhubs.config.AzureEventHubsConfigConstants;
import io.lenses.java.streamreactor.connect.azure.eventhubs.config.AzureEventHubsSourceConfig;
import io.lenses.kcql.Kcql;
import java.util.ArrayList;
import java.util.HashMap;
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
    new AzureEventHubsSourceConfig(props);
    printAsciiHeader(jarManifest, "/azure-eventhubs-ascii.txt");
  }

  @Override
  public Class<? extends Task> taskClass() {
    return AzureEventHubsSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Kcql> kcqls = Kcql.parseMultiple(
        configProperties.get(AzureEventHubsConfigConstants.KCQL_CONFIG));
    int numberOfMappings = kcqls.size();
    List<Map<String, String>> taskConfigs = new ArrayList<>(numberOfMappings);

    IntStream.range(0, maxTasks).forEach(taskNumber -> {
      HashMap<String, String> taskProperties = new HashMap<>(configProperties);
      taskProperties.put(AzureEventHubsConfigConstants.KCQL_CONFIG, kcqls.get(taskNumber).getQuery());
      taskConfigs.add(taskProperties);
    });

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
    return AzureEventHubsSourceConfig.getConfigDefinition();
  }

  @Override
  public String version() {
    return jarManifest.getVersion();
  }
}
