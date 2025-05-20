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
package io.lenses.streamreactor.common.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

class MapUtilsTest {

  @Test
  void testCastMap_ValidStringMap_ReturnsTypedMap() {
    Map<Object, Object> rawMap =
        Map.of(
            "key1", "value1",
            "key2", "value2"
        );

    Map<String, String> typedMap = MapUtils.castMap(rawMap, String.class, String.class);

    assertEquals("value1", typedMap.get("key1"));
    assertEquals("value2", typedMap.get("key2"));
  }

  @Test
  void testCastMap_NonStringKeyOrValue_ThrowsException() {
    Map<Object, Object> rawMap =
        Map.of(
            "key1", "value1",
            123, "value2" // Non-String key
        );

    assertThrows(IllegalArgumentException.class, () -> MapUtils.castMap(rawMap, String.class, String.class));
  }

  @Test
  void testCastMap_ValidIntegerMap_ReturnsTypedMap() {
    Map<Object, Object> rawMap =
        Map.of(
            10, 99,
            20, 145
        );

    Map<Integer, Integer> typedMap = MapUtils.castMap(rawMap, Integer.class, Integer.class);

    assertEquals(99, typedMap.get(10));
    assertEquals(145, typedMap.get(20));
  }

  @Test
  void testCastMap_NonIntegerKeyOrValue_ThrowsException() {
    Map<Object, Object> rawMap =
        Map.of(
            10, 99,
            20, "pigeons" // Non-Integer value
        );

    assertThrows(IllegalArgumentException.class, () -> MapUtils.castMap(rawMap, Integer.class, Integer.class));
  }

  @Test
  void testCastMap_WithNullValues_ShouldHandleGracefully() {
    Map<Object, Object> rawMap = new HashMap<>();
    rawMap.put("key1", "value1");
    rawMap.put("key2", null); // Null value

    Map<String, String> typedMap = MapUtils.castMap(rawMap, String.class, String.class);

    assertEquals("value1", typedMap.get("key1"));
    assertNull(typedMap.get("key2"));
  }

  @Test
  void testCastMap_SuperclassTypeCompatibility_ShouldPass() {
    Map<Object, Object> rawMap =
        Map.of(
            "key1", "value1",
            "key2", "value2"
        );

    // Cast to Map<CharSequence, CharSequence> since String implements CharSequence
    Map<CharSequence, CharSequence> typedMap = MapUtils.castMap(rawMap, CharSequence.class, CharSequence.class);

    assertEquals("value1", typedMap.get("key1"));
    assertEquals("value2", typedMap.get("key2"));
  }

  @Test
  void testCastMap_InterfaceTypeCompatibility_ShouldPass() {
    Map<Object, Object> rawMap =
        Map.of(
            "key1", "value1",
            "key2", "value2"
        );

    // Cast to Map<Object, Object> since String is an Object
    Map<Object, Object> typedMap = MapUtils.castMap(rawMap, Object.class, Object.class);

    assertEquals("value1", typedMap.get("key1"));
    assertEquals("value2", typedMap.get("key2"));
  }

  @Test
  void testCastMap_StringToObject_ShouldPass() {
    Map<Object, Object> rawMap =
        Map.of(
            "key1", "value1",
            "key2", "value2"
        );

    // Cast to Map<Object, Object> since String is an Object
    Map<Object, Object> typedMap = MapUtils.castMap(rawMap, Object.class, Object.class);

    assertEquals("value1", typedMap.get("key1"));
    assertEquals("value2", typedMap.get("key2"));
  }

  @Test
  void testCastMap_StringToCharSequence_ShouldPass() {
    Map<Object, Object> rawMap =
        Map.of(
            "key1", "value1",
            "key2", "value2"
        );

    // Cast to Map<CharSequence, CharSequence> since String implements CharSequence
    Map<CharSequence, CharSequence> typedMap = MapUtils.castMap(rawMap, CharSequence.class, CharSequence.class);

    assertEquals("value1", typedMap.get("key1"));
    assertEquals("value2", typedMap.get("key2"));
  }

}
