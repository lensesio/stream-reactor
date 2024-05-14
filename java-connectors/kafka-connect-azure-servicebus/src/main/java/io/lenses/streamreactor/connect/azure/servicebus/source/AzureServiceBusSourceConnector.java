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
package io.lenses.streamreactor.connect.azure.servicebus.source;

import static io.lenses.streamreactor.common.util.AsciiArtPrinter.printAsciiHeader;

import io.lenses.streamreactor.common.util.JarManifest;
import io.lenses.streamreactor.connect.azure.servicebus.config.AzureServiceBusSourceConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

/**
 * Implementation of {@link SourceConnector} for Microsoft Azure EventHubs.
 */
@Slf4j
public class AzureServiceBusSourceConnector extends SourceConnector {

  private final JarManifest jarManifest =
      new JarManifest(getClass().getProtectionDomain().getCodeSource().getLocation());
  private Map<String, String> configProperties;

  @Override
  public void start(Map<String, String> props) {
    parseAndValidateConfigs(props);
    configProperties = props;
    printAsciiHeader(jarManifest, "/azure-servicebus-ascii.txt");
  }

  private void parseAndValidateConfigs(Map<String, String> props) {
    new AzureServiceBusSourceConfig(props);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return AzureServiceBusSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    log.info("Setting task configurations for {} workers.", maxTasks);
    List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);

    IntStream.range(0, maxTasks).forEach(task -> taskConfigs.add(configProperties));
    return taskConfigs;
  }

  @Override
  public void stop() {
    // connector-specific implementation not needed
  }

  @Override
  public ConfigDef config() {
    return AzureServiceBusSourceConfig.getConfigDefinition();
  }

  @Override
  public String version() {
    return jarManifest.getVersion();
  }
}
