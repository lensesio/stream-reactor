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

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

class MapUtilsTest {

  @Test
  void testCastMap_ValidStringMap_ReturnsTypedMap() {
    Map<Object, Object> rawMap = new HashMap<>();
    rawMap.put("key1", "value1");
    rawMap.put("key2", "value2");

    Map<String, String> typedMap = MapUtils.castMap(rawMap, String.class, String.class);

    assertEquals("value1", typedMap.get("key1"));
    assertEquals("value2", typedMap.get("key2"));
  }

  @Test
  void testCastMap_NonStringKeyOrValue_ThrowsException() {
    Map<Object, Object> rawMap = new HashMap<>();
    rawMap.put("key1", "value1");
    rawMap.put(123, "value2"); // Non-String key

    assertThrows(IllegalArgumentException.class, () -> {
      MapUtils.castMap(rawMap, String.class, String.class);
    });
  }

  @Test
  void testCastMap_ValidIntegerMap_ReturnsTypedMap() {
    Map<Object, Object> rawMap = new HashMap<>();
    rawMap.put(10, 99);
    rawMap.put(20, 145);

    Map<Integer, Integer> typedMap = MapUtils.castMap(rawMap, Integer.class, Integer.class);

    assertEquals(99, typedMap.get(10));
    assertEquals(145, typedMap.get(20));
  }

  @Test
  void testCastMap_NonIntegerKeyOrValue_ThrowsException() {
    Map<Object, Object> rawMap = new HashMap<>();
    rawMap.put(10, 99);
    rawMap.put(20, "pigeons"); // Non-Integer value

    assertThrows(IllegalArgumentException.class, () -> {
      MapUtils.castMap(rawMap, Integer.class, Integer.class);
    });
  }

}
