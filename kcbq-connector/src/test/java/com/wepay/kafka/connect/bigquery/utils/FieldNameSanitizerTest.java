/*
 * Copyright 2020 Confluent, Inc.
 *
 * This software contains code derived from the WePay BigQuery Kafka Connector, Copyright WePay, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wepay.kafka.connect.bigquery.utils;

import java.util.Collections;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FieldNameSanitizerTest {
  private Map<String, Object> testMap;

  @Before
  public void setUp() {
    testMap = new HashMap<String, Object>() {{
      put("A.1", new HashMap<String, Object>() {{
        put("_B1", 1);
        put("B.2", "hello.B-2");
      }});
      put("A-2", new HashMap<String, Object>() {{
        put("=/B.3", "hello B3");
        put("B./4", "hello B4");
        put("2A/", "hello B5");
        put("3A/", "hello B6");
      }});
      put("Foo", "Simple Value");
      put("Foo_1", "Simple Value 1");
      put("Foo-2", "Simple Value 2");
    }};
  }

  @Test
  public void testInvalidSymbol() {
    Map<String, Object> sanitizedMap = FieldNameSanitizer.replaceInvalidKeys(testMap);
    assertTrue(sanitizedMap.containsKey("A_1"));
    assertTrue(sanitizedMap.containsKey("A_2"));

    Map<String, Object> nestedMap1 = (Map<String, Object>) sanitizedMap.get("A_1");
    // Validate changed keys.
    assertTrue(nestedMap1.containsKey("B_2"));
    assertTrue(nestedMap1.containsKey("_B1"));

    // Validate unchanged values.
    assertEquals(nestedMap1.get("B_2"), "hello.B-2");
    assertEquals(nestedMap1.get("_B1"), 1);

    // Validate map size.
    assertEquals(2, nestedMap1.size());

    Map<String, Object> nestedMap2 = (Map<String, Object>) sanitizedMap.get("A_2");
    // Validate changed keys.
    assertTrue(nestedMap2.containsKey("__B_3"));
    assertTrue(nestedMap2.containsKey("B__4"));
    assertTrue(nestedMap2.containsKey("_2A_"));
    assertTrue(nestedMap2.containsKey("_3A_"));

    // Validate unchanged values.
    assertEquals(nestedMap2.get("__B_3"), "hello B3");
    assertEquals(nestedMap2.get("B__4"), "hello B4");
    assertEquals(nestedMap2.get("_2A_"), "hello B5");
    assertEquals(nestedMap2.get("_3A_"), "hello B6");

    // Validate map size.
    assertEquals(4, nestedMap2.size());

    // Validate keys shall be unchanged.
    assertTrue(sanitizedMap.containsKey("Foo"));
    assertTrue(sanitizedMap.containsKey("Foo_1"));

    // Validate key shall be changed.
    assertTrue(sanitizedMap.containsKey("Foo_2"));

    // Validate map size.
    assertEquals(5, sanitizedMap.size());
  }

  /**
   * Verifies that null values are acceptable while sanitizing keys.
   */
  @Test
  public void testNullValue() {
    assertEquals(
        Collections.singletonMap("abc", null),
        FieldNameSanitizer.replaceInvalidKeys(Collections.singletonMap("abc", null)));
  }

  @Test
  public void testDeeplyNestedNullValues() {
    testMap = new HashMap<>();
    testMap.put("top", null);
    testMap.put("middle", Collections.singletonMap("key", null));
    testMap.put("bottom", Collections.singletonMap("key", Collections.singletonMap("key", null)));
    assertEquals(
        testMap,
        FieldNameSanitizer.replaceInvalidKeys(testMap)
    );
  }
}
