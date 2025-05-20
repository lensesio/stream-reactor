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
package io.lenses.kcql.targettype;

import cyclops.control.Either;
import cyclops.control.Option;
import org.junit.jupiter.api.Test;

import static io.lenses.streamreactor.test.utils.EitherValues.assertRight;

class TargetTypeParserTest {

  @Test
  void testParseStaticTargetType() {
    Either<String, TargetType> targetType = TargetTypeParser.parse("static");
    assertRight(targetType).isEqualTo(new StaticTargetType("static"));
  }

  @Test
  void testParseHeaderTargetType() {
    Either<String, TargetType> targetType = TargetTypeParser.parse("_header.headerTarget");
    assertRight(targetType).isEqualTo(new HeaderTargetType("headerTarget"));
  }

  @Test
  void testParseKeyTargetType() {
    Either<String, TargetType> targetType = TargetTypeParser.parse("_key.keyTarget");
    assertRight(targetType).isEqualTo(new KeyTargetType(Option.of("keyTarget")));
  }

  @Test
  void testParseValueTargetType() {
    Either<String, TargetType> targetType = TargetTypeParser.parse("_value.valueTarget");
    assertRight(targetType).isEqualTo(new ValueTargetType(Option.of("valueTarget")));
  }

  @Test
  void testParseDefaultTargetType() {
    Either<String, TargetType> targetType = TargetTypeParser.parse("defaultTarget");
    assertRight(targetType).isEqualTo(new StaticTargetType("defaultTarget"));
  }

  @Test
  void testParseKeyTargetTypeWithoutPath() {
    Either<String, TargetType> targetType = TargetTypeParser.parse("_key");
    assertRight(targetType).isEqualTo(new KeyTargetType(Option.none()));
  }

  @Test
  void testParseValueTargetTypeWithoutPath() {
    Either<String, TargetType> targetType = TargetTypeParser.parse("_value");
    assertRight(targetType).isEqualTo(new ValueTargetType(Option.none()));
  }

  @Test
  void testParseValueTargetTypeWithBackticks() {
    Either<String, TargetType> targetType = TargetTypeParser.parse("_value.'one.field.with.dots'");
    assertRight(targetType).isEqualTo(new ValueTargetType(Option.of("'one.field.with.dots'")));

    Either<String, TargetType> targetType2 =
        TargetTypeParser.parse("_value.'first.field.with.dots'.'second.field.with.dots'");
    assertRight(targetType2).isEqualTo(new ValueTargetType(Option.of(
        "'first.field.with.dots'.'second.field.with.dots'")));
  }
}
