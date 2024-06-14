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

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import cyclops.control.Either;
import io.lenses.streamreactor.common.config.base.ConfigSettings;
import io.lenses.streamreactor.common.config.base.KcqlSettings;
import io.lenses.streamreactor.common.config.base.model.ConnectorPrefix;
import io.lenses.streamreactor.common.config.source.ConfigSource;
import io.lenses.streamreactor.common.config.source.ConfigWrapperSource;
import io.lenses.streamreactor.connect.gcp.pubsub.source.config.PubSubSourceConfig;
import lombok.Getter;

/**
 * PubSubConfigDef is responsible for holding the configuration definition for the PubSub connector.
 * It contains the gcpSettings and kcqlSettings.
 */
public class PubSubConfigSettings implements ConfigSettings<PubSubSourceConfig> {

  @Getter
  private final ConfigDef configDef;

  private static final ConnectorPrefix connectorPrefix = new ConnectorPrefix("connect.pubsub");

  @Getter
  private static final PubSubSettings gcpSettings = new PubSubSettings(connectorPrefix);

  @Getter
  private static final KcqlSettings kcqlSettings = new KcqlSettings(connectorPrefix);

  private static final List<ConfigSettings<?>> settings = List.of(kcqlSettings, gcpSettings);

  public PubSubConfigSettings() {
    configDef = new ConfigDef();
    withSettings(configDef);
  }

  public Either<ConfigException, PubSubSourceConfig> parse(Map<String, String> props) {
    return ConfigWrapperSource.fromConfigDef(getConfigDef(), props).flatMap(this::parseFromConfig);
  }

  @Override
  public Either<ConfigException, PubSubSourceConfig> parseFromConfig(ConfigSource configSource) {
    return gcpSettings.parseFromConfig(configSource)
        .flatMap(gcp -> kcqlSettings.parseFromConfig(configSource)
            .map(kcql -> new PubSubSourceConfig(gcp, kcql)));
  }

  @Override
  public ConfigDef withSettings(ConfigDef configDef) {

    // side-effects
    settings.forEach(s -> s.withSettings(configDef));

    return configDef;
  }

}
