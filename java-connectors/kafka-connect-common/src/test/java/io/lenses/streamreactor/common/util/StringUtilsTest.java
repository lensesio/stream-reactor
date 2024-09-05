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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class StringUtilsTest {

  @Test
  void testIsBlankWithNull() {
    assertTrue(StringUtils.isBlank(null), "null should be considered blank");
  }

  @Test
  void testIsBlankWithEmptyString() {
    assertTrue(StringUtils.isBlank(""), "Empty string should be considered blank");
  }

  @Test
  void testIsBlankWithWhitespace() {
    assertTrue(StringUtils.isBlank("   "), "String with only whitespace should be considered blank");
  }

  @Test
  void testIsBlankWithNonBlankString() {
    assertFalse(StringUtils.isBlank("abc"), "Non-blank string should not be considered blank");
  }

  @Test
  void testIsBlankWithStringContainingWhitespace() {
    assertFalse(StringUtils.isBlank(" abc "), "String with non-whitespace characters should not be considered blank");
  }
}
