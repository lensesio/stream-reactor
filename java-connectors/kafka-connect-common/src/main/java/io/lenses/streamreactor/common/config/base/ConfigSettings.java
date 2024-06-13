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
package io.lenses.streamreactor.common.config.base;

import org.apache.kafka.common.config.ConfigDef;

import io.lenses.streamreactor.common.config.source.ConfigSource;

/**
 * Defines operations to manage settings and parse configurations into objects.
 *
 * @param <M> the type of object to materialize from settings
 */
public interface ConfigSettings<M> {

  /**
   * Adds the settings defined by this interface to the provided {@code ConfigDef}.
   *
   * @param configDef the {@code ConfigDef} to which settings should be added
   * @return the updated {@code ConfigDef} with added settings
   */
  ConfigDef withSettings(ConfigDef configDef);

  /**
   * Parses settings from the specified {@code ConfigSource} and materializes an object of type {@code M}.
   *
   * @param configSource the {@code ConfigSource} containing configuration settings
   * @return an object of type {@code M} materialized from the given {@code ConfigSource}
   */
  M parseFromConfig(ConfigSource configSource);
}
