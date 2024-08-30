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
package io.lenses.kcql.targettype;

import cyclops.control.Either;
import cyclops.control.Option;

import java.util.Map;
import java.util.function.Function;

/**
 * This class is responsible for parsing a target string and producing the corresponding TargetType instance.
 * The target string can have one of the following formats:
 * - "_key": produces a KeyTargetType with no path.
 * - "_value": produces a ValueTargetType with no path.
 * - "_header.somePath": produces a HeaderTargetType with "somePath" as the path.
 * - "_key.somePath": produces a KeyTargetType with "somePath" as the path.
 * - "_value.somePath": produces a ValueTargetType with "somePath" as the path.
 * - "someTarget": produces a StaticTargetType with "someTarget" as the target.
 *
 * The HeaderTargetType requires a path, and if it's not provided, an error message is returned wrapped in an Either.
 *
 * The parse method returns an Either. If the parsing is successful, the right side of the Either contains the
 * TargetType instance.
 * If the parsing fails (for example, if a path is not provided for a HeaderTargetType), the left side of the Either
 * contains an error message.
 */
public class TargetTypeParser {

  public static final String TYPE_PREFIX_KEY = "_key";
  public static final String TYPE_PREFIX_VALUE = "_value";
  public static final String TYPE_PREFIX_HEADER = "_header";

  /**
   * A map from prefix strings to functions that create the corresponding TargetType instances.
   * The functions return an Either. If the creation is successful, the right side of the Either contains the TargetType
   * instance.
   * If the creation fails (for example, if a path is not provided for a HeaderTargetType), the left side of the Either
   * contains an error message.
   */
  private static final Map<String, Function<Option<String>, Either<String, TargetType>>> TARGET_TYPE_MAP =
      Map.of(
          TYPE_PREFIX_KEY, (Option<String> path) -> Either.right(new KeyTargetType(path)),
          TYPE_PREFIX_VALUE, (Option<String> path) -> Either.right(new ValueTargetType(path)),
          TYPE_PREFIX_HEADER, (Option<String> path) -> path.toEither("No path specified for header").map(
              HeaderTargetType::new)
      );

  /**
   * Parses the given target string and produces the corresponding TargetType instance.
   * Returns an Either. If the parsing is successful, the right side of the Either contains the TargetType instance.
   * If the parsing fails (for example, if a path is not provided for a HeaderTargetType), the left side of the Either
   * contains an error message.
   *
   * @param target the target string to parse
   * @return an Either where the right side is the corresponding TargetType instance if the parsing is successful, or
   *         the left side is an error message if the parsing fails
   */
  public static Either<String, TargetType> parse(String target) {
    String[] split = target.split("\\.", 2);
    Option<String> path = split.length > 1 ? Option.of(split[1]) : Option.none();

    return Option.ofNullable(TARGET_TYPE_MAP.get(split[0]))
        .map(constructor -> constructor.apply(path))
        .orElseGet(() -> Either.right(new StaticTargetType(target)));
  }
}
