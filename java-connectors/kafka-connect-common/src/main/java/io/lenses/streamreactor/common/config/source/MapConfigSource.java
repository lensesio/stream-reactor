/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.common.config.source;

import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.config.types.Password;

import lombok.AllArgsConstructor;

/**
 * A wrapper for Kafka Connect properties that provides methods to retrieve property values from a Map.
 */
@AllArgsConstructor
public class MapConfigSource implements ConfigSource {

  private final Map<String, Object> wrapped;

  @Override
  public Optional<String> getString(String key) {
    return Optional.ofNullable((String) wrapped.get(key)).filter(s -> !s.isEmpty());
  }

  @Override
  public Optional<Integer> getInt(String key) {
    return Optional.ofNullable((Integer) wrapped.get(key));
  }

  @Override
  public Optional<Long> getLong(String key) {
    return Optional.ofNullable((Long) wrapped.get(key));
  }

  @Override
  public Optional<Double> getDouble(String key) {
    return Optional.ofNullable((Double) wrapped.get(key));
  }

  @Override
  public Optional<Boolean> getBoolean(String key) {
    Object value = wrapped.get(key);
    if (value != null) {
      if (Boolean.class.isAssignableFrom(value.getClass())) {
        return Optional.of((Boolean) value);
      } else if (String.class.isAssignableFrom(value.getClass())) {
        return Optional.of(Boolean.parseBoolean((String) value));
      }
    }
    return Optional.empty();
  }

  @Override
  public Optional<Password> getPassword(String key) {
    return Optional.ofNullable((Password) wrapped.get(key));
  }
}
