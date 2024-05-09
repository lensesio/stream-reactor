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

import lombok.experimental.UtilityClass;

import java.util.Map;

@UtilityClass
public class MapUtils {

  @SuppressWarnings("unchecked")
  public static <K, V> Map<K, V> castMap(Map<?, ?> map, Class<K> targetKeyType, Class<V> targetValueType) {
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      if (!(isInstance(entry.getKey(), targetKeyType)) || !(isInstance(entry.getValue(), targetValueType))) {
        throw new IllegalArgumentException("Map contains invalid key or value type");
      }
    }
    return (Map<K, V>) map;
  }

  private static <T> boolean isInstance(Object obj, Class<T> type) {
    return obj == null || type.isInstance(obj);
  }
}
