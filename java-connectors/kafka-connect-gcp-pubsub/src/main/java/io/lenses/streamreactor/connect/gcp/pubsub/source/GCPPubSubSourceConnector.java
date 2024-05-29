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
package io.lenses.streamreactor.connect.gcp.pubsub.source;

import static io.lenses.streamreactor.common.util.AsciiArtPrinter.printAsciiHeader;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

import io.lenses.streamreactor.common.util.JarManifest;
import io.lenses.streamreactor.common.util.TasksSplitter;
import io.lenses.streamreactor.connect.gcp.pubsub.source.configdef.PubSubConfigSettings;
import lombok.val;

/**
 * GCPPubSubSourceConnector is a source connector for Google Cloud Pub/Sub.
 * It is responsible for starting the connector, creating tasks, and stopping the connector.
 */
public class GCPPubSubSourceConnector extends SourceConnector {

  private Map<String, String> props;

  private final PubSubConfigSettings pubSubConfigSettings = new PubSubConfigSettings();

  private final JarManifest jarManifest =
      new JarManifest(getClass().getProtectionDomain().getCodeSource().getLocation());

  @Override
  public void start(Map<String, String> props) {
    printAsciiHeader(jarManifest, "/gcp-pubsub-source-ascii.txt");
    this.props = validateProps(props);
  }

  private Map<String, String> validateProps(Map<String, String> props) {
    try {
      val pubSubConfigDef = new PubSubConfigSettings();
      val pubSubSourceConfig = pubSubConfigDef.parse(props);
      pubSubSourceConfig.validateKcql();
      return props;
    } catch (Exception e) {
      throw new ConnectException("Invalid connector properties configuration: " + e.getMessage(), e);
    }
  }

  @Override
  public Class<? extends Task> taskClass() {
    return GCPPubSubSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    return TasksSplitter.splitByKcqlStatements(
        maxTasks,
        props,
        PubSubConfigSettings.getKcqlSettings()
    );
  }

  @Override
  public void stop() {
    // No implementation required!
  }

  @Override
  public ConfigDef config() {
    return pubSubConfigSettings.getConfigDef();
  }

  @Override
  public String version() {
    return jarManifest.getVersion();
  }
}
