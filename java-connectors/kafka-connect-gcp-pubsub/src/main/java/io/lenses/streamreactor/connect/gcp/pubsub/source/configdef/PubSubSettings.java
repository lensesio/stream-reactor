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
package io.lenses.streamreactor.connect.gcp.pubsub.source.configdef;

import org.apache.kafka.common.config.ConfigDef;

import io.lenses.streamreactor.common.config.base.ConfigSettings;
import io.lenses.streamreactor.common.config.base.model.ConnectorPrefix;
import io.lenses.streamreactor.common.config.source.ConfigSource;
import io.lenses.streamreactor.connect.gcp.common.config.AuthModeSettings;
import io.lenses.streamreactor.connect.gcp.pubsub.source.config.PubSubConfig;
import io.lenses.streamreactor.connect.gcp.pubsub.source.mapping.MappingConfig;
import lombok.Getter;
import lombok.val;

import static io.lenses.streamreactor.connect.gcp.pubsub.source.mapping.MappingConfig.OUTPUT_MODE_DEFAULT;

/**
 * PubSubSettings is responsible for configuration settings for connecting to Google Cloud Platform (GCP) services.
 * This class provides methods for defining and parsing GCP-specific configuration properties.
 */
@Getter
public class PubSubSettings implements ConfigSettings<PubSubConfig> {

  private static final String EMPTY_STRING = "";

  private final String gcpProjectIdKey;
  private final String outputModeKey;

  private final AuthModeSettings authModeSettings;

  /**
   * Constructs a new instance of {@code GCPSettings} with the specified connector prefix.
   *
   * @param connectorPrefix the prefix used for configuration keys
   */
  public PubSubSettings(ConnectorPrefix connectorPrefix) {
    gcpProjectIdKey = connectorPrefix.prefixKey("gcp.project.id");
    outputModeKey = connectorPrefix.prefixKey("output.mode");
    authModeSettings = new AuthModeSettings(connectorPrefix);
  }

  /**
   * Configures the provided {@link ConfigDef} with GCP-specific settings.
   *
   * @param configDef the base configuration definition to extend
   * @return the updated {@link ConfigDef} with GCP-specific settings
   */
  @Override
  public ConfigDef withSettings(ConfigDef configDef) {
    val conf =
        configDef
            .define(
                gcpProjectIdKey,
                ConfigDef.Type.STRING,
                EMPTY_STRING,
                ConfigDef.Importance.HIGH,
                "GCP Project ID")
            .define(
                outputModeKey,
                ConfigDef.Type.STRING,
                EMPTY_STRING,
                ConfigDef.Importance.HIGH,
                "Output Mode (options are DEFAULT or COMPATIBILITY)");

    return authModeSettings.withSettings(conf);
  }

  public PubSubConfig parseFromConfig(ConfigSource configSource) {
    return new PubSubConfig(
        configSource.getString(gcpProjectIdKey).orElse(null),
        authModeSettings.parseFromConfig(configSource),
        MappingConfig.fromOutputMode(configSource.getString(outputModeKey).orElse(OUTPUT_MODE_DEFAULT)));
  }
}
