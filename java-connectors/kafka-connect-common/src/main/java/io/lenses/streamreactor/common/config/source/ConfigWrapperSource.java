/*
 * Copyright 2017-2024 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.common.config.source;

import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import cyclops.control.Either;
import cyclops.control.Try;
import lombok.AllArgsConstructor;

/**
 * A wrapper for Kafka Connect properties stored in the `AbstractConfig` that provides methods to retrieve property
 * values.
 */
@AllArgsConstructor
public class ConfigWrapperSource implements ConfigSource {

  public static Either<ConfigException, ConfigWrapperSource> fromConfigDef(ConfigDef configDef,
      Map<String, String> props) {
    return Try.withCatch(() -> new AbstractConfig(configDef, props), ConfigException.class).toEither().map(
        ConfigWrapperSource::new);
  }

  private final AbstractConfig abstractConfig;

  @Override
  public Optional<String> getString(String key) {
    return Optional.ofNullable(abstractConfig.getString(key)).filter(s -> !s.isEmpty());
  }

  @Override
  public Optional<Integer> getInt(String key) {
    return Optional.ofNullable(abstractConfig.getInt(key));
  }

  @Override
  public Optional<Long> getLong(String key) {
    return Optional.ofNullable(abstractConfig.getLong(key));
  }

  @Override
  public Optional<Double> getDouble(String key) {
    return Optional.ofNullable(abstractConfig.getDouble(key));
  }

  @Override
  public Optional<Password> getPassword(String key) {
    return Optional.ofNullable(abstractConfig.getPassword(key));
  }
}
