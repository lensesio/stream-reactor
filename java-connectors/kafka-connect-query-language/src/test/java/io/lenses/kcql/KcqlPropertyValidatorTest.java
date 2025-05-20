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
package io.lenses.kcql;

import static io.lenses.streamreactor.test.utils.EitherValues.assertLeft;
import static io.lenses.streamreactor.test.utils.EitherValues.assertRight;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;

import org.junit.jupiter.api.Test;

class KcqlPropertyValidatorTest {

  private final Kcql kcqlWithProperties =
      Kcql.parse(
          "INSERT INTO table SELECT * FROM topic PK f1,f2 properties(key1=value1, key2='value2', 'key3'='value3')");
  private final Kcql kcqlNoProperties = Kcql.parse("INSERT INTO table SELECT * FROM topic PK f1,f2");

  @Test
  void testValidateKcqlProperties_withAllowedKeys() {
    assertRight(kcqlWithProperties.validateKcqlProperties("key1", "key2", "key3"))
        .isNotNull();
  }

  @Test
  void testValidateKcqlProperties_withUnexpectedKeys() {
    assertLeft(kcqlWithProperties.validateKcqlProperties("key1", "key2"))
        .isNotNull()
        .isInstanceOf(IllegalArgumentException.class)
        .satisfies(ex -> assertThat(ex.getMessage()).contains("Unexpected properties found: `key3`"));
  }

  @Test
  void testExtractOptionalProperty_withExistingKey() {
    Optional<String> result = kcqlWithProperties.extractOptionalProperty("key1");
    assertTrue(result.isPresent());
    assertEquals("value1", result.get());
  }

  @Test
  void testExtractOptionalProperty_withNonExistingKey() {
    Optional<String> result = kcqlWithProperties.extractOptionalProperty("key4");
    assertFalse(result.isPresent());
  }

  @Test
  void testValidateKcqlProperties_withNoProperties() {
    assertRight(kcqlNoProperties.validateKcqlProperties("key1", "key2", "key3"))
        .isNotNull();
  }

}
