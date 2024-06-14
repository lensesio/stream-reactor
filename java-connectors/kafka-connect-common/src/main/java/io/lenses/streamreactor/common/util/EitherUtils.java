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

import cyclops.control.Either;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.config.ConfigException;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class EitherUtils {

  public static <X extends Throwable, Y> Either<ConfigException, List<Y>> combineErrors(
      Stream<Either<X, Y>> eithers
  ) {
    Map<Boolean, List<Either<X, Y>>> partitioned = eithers.collect(Collectors.partitioningBy(Either::isLeft));

    List<String> lefts =
        partitioned.get(true).stream()
            .flatMap(either -> either.getLeft().fold(
                Stream::of, // Convert left to Stream
                kcql -> Stream.empty() // Right case, empty Stream
            ))
            .map(Throwable::getMessage)
            .collect(Collectors.toList());

    List<Y> rights =
        partitioned.get(false).stream()
            .flatMap(either -> either.get().fold(
                Stream::of,
                err -> Stream.empty()
            ))
            .collect(Collectors.toList());

    return lefts.isEmpty()
        ? Either.right(rights)
        : Either.left(new ConfigException("Invalid " + String.join(System.lineSeparator(), lefts)));

  }
}
