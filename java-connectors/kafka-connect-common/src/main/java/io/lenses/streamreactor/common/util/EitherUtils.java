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

import static java.util.function.Function.identity;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;

import cyclops.control.Either;
import io.lenses.streamreactor.common.exception.StreamReactorException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class EitherUtils {

  /**
   * Unpacks an Either, returning the right value or throwing an exception if it is a left value.
   *
   * @param either The Either to unpack
   * @param <L>    The type of the left value (error)
   * @param <R>    The type of the right value (success)
   * @return The right value if present
   * @throws RuntimeException if the Either contains a left value
   */
  public static <L extends StreamReactorException, R> R unpackOrThrow(Either<L, R> either) throws KafkaException {
    return unpackOrThrow(
        either,
        ConnectException::new
    );
  }

  /**
   * Unpacks an Either, returning the right value or throwing an exception if it is a left value.
   *
   * @param either            The Either to unpack
   * @param exceptionSupplier A function that takes a left value (of type L) and returns an exception (of type O)
   * @param <L>               The type of the left value (error), which must extend StreamReactorException
   * @param <R>               The type of the right value (success)
   * @param <O>               The type of the exception to be thrown, which must extend ConnectException
   * @return The right value if present
   * @throws O if the Either contains a left value
   */
  public static <L extends StreamReactorException, R, O extends KafkaException> R unpackOrThrow(
      Either<L, R> either,
      Function<L, O> exceptionSupplier
  ) throws O {
    return either.fold(
        throwable -> {
          throw exceptionSupplier.apply(throwable);
        },
        identity()
    );
  }

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
