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
package io.lenses.streamreactor.common.util;

import java.util.Map;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Utility class for map operations.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MapUtils {

  /**
   * Casts a map to a specified key and value type.
   *
   * @param map             the map to cast
   * @param targetKeyType   the class of the key type
   * @param targetValueType the class of the value type
   * @param <K>             the target key type
   * @param <V>             the target value type
   * @return the casted map
   * @throws IllegalArgumentException if the map contains keys or values of incorrect types
   */
  @SuppressWarnings("unchecked")
  public static <K, V> Map<K, V> castMap(Map<?, ?> map, Class<K> targetKeyType, Class<V> targetValueType) {
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      if (!isAssignable(entry.getKey(), targetKeyType) || !isAssignable(entry.getValue(), targetValueType)) {
        throw new IllegalArgumentException("Map contains invalid key or value type");
      }
    }
    return (Map<K, V>) map;
  }

  /**
   * Checks if an object is assignable to a specified type, allowing for null values.
   *
   * @param obj  the object to check
   * @param type the target type
   * @param <T>  the target type
   * @return true if the object is null or assignable to the type, false otherwise
   */
  private static <T> boolean isAssignable(Object obj, Class<T> type) {
    return obj == null || type.isAssignableFrom(obj.getClass());
  }
}
