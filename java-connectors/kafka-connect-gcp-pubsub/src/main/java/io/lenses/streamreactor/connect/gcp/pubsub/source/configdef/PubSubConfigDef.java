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

import org.apache.kafka.common.config.ConfigDef;

import io.lenses.streamreactor.common.config.base.ConfigSettings;
import io.lenses.streamreactor.common.config.base.KcqlSettings;
import io.lenses.streamreactor.common.config.base.model.ConnectorPrefix;
import lombok.Getter;
import lombok.experimental.UtilityClass;

/**
 * PubSubConfigDef is responsible for holding the configuration definition for the PubSub connector.
 * It contains the gcpSettings and kcqlSettings.
 */
@UtilityClass
public class PubSubConfigDef {

  @Getter
  private static final ConfigDef configDef = new ConfigDef();

  private static final ConnectorPrefix connectorPrefix = new ConnectorPrefix("connect.pubsub");

  @Getter
  private static final PubSubSettings gcpSettings = new PubSubSettings(connectorPrefix);

  @Getter
  private static final KcqlSettings kcqlSettings = new KcqlSettings(connectorPrefix);

  private static final List<ConfigSettings<?>> settings = List.of(kcqlSettings, gcpSettings);

  static {
    // side-effects
    settings.forEach(s -> s.withSettings(configDef));
  }
}
