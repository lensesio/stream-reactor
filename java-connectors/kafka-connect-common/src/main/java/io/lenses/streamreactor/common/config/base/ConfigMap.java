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
package io.lenses.streamreactor.common.config.base;

import lombok.AllArgsConstructor;
import org.apache.kafka.common.config.types.Password;

import java.util.Map;
import java.util.Optional;

/**
 * A wrapper for Kafka Connect properties that provides methods to retrieve property values.
 */
@AllArgsConstructor
public class ConfigMap {

  private Map<String, Object> wrapped;

  /**
   * Retrieves a String property value associated with the given key.
   *
   * @param key the property key
   * @return an {@link Optional} containing the property value if present, otherwise empty
   */
  public Optional<String> getString(String key) {
    return Optional.ofNullable((String) wrapped.get(key));
  }

  /**
   * Retrieves a Password property value associated with the given key.
   *
   * @param key the property key
   * @return an {@link Optional} containing the {@link Password} value if present, otherwise empty
   */
  public Optional<Password> getPassword(String key) {
    return Optional.ofNullable((Password) wrapped.get(key));
  }

}
