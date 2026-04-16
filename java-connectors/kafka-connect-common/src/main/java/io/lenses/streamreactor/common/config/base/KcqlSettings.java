/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.common.config.base;

import java.util.List;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import cyclops.control.Either;
import cyclops.control.Try;
import io.lenses.kcql.Kcql;
import io.lenses.streamreactor.common.config.base.model.ConnectorPrefix;
import io.lenses.streamreactor.common.config.source.ConfigSource;
import lombok.Getter;
import lombok.val;

@Getter
public class KcqlSettings implements ConfigSettings<List<Kcql>> {

  private static final String KCQL_DOC =
      "Contains the Kafka Connect Query Language describing data mappings from the source to the target system.";

  private final String kcqlSettingsKey;

  public KcqlSettings(
      ConnectorPrefix connectorPrefix) {
    kcqlSettingsKey = connectorPrefix.prefixKey("kcql");
  }

  @Override
  public ConfigDef withSettings(ConfigDef configDef) {
    return configDef.define(
        kcqlSettingsKey,
        ConfigDef.Type.STRING,
        ConfigDef.Importance.HIGH,
        KCQL_DOC
    );
  }

  @Override
  public Either<ConfigException, List<Kcql>> parseFromConfig(ConfigSource configSource) {
    return Try.withCatch(() -> Kcql.parseMultiple(getKCQLString(configSource))).toEither()
        .mapLeft(ex -> new ConfigException(ex.getMessage()));
  }

  private String getKCQLString(ConfigSource configSource) {
    val raw = configSource.getString(kcqlSettingsKey);
    return raw.orElseThrow(() -> new ConfigException(String.format("Missing [%s]", kcqlSettingsKey)));
  }
}
