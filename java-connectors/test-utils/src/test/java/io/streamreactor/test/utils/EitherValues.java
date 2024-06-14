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
package io.streamreactor.test.utils;

import static org.assertj.core.api.Assertions.assertThat;

import org.assertj.core.api.AbstractObjectAssert;

import cyclops.control.Either;

/**
 * Utility class for working with Cyclops Either objects in test scenarios.
 *
 * This class provides static methods for extracting and asserting values from
 * Cyclops Either instances, making it easier to handle and validate test outcomes.
 */
public class EitherValues {

  /**
   * Retrieves the Right value from an Either instance.
   *
   * @param <L>    the type of the Left value
   * @param <R>    the type of the Right value
   * @param either the Either instance
   * @return the Right value if present
   * @throws AssertionError if the Either contains a Left value
   */
  public static <L, R> R getRight(Either<L, R> either) {
    return either.fold(
        l -> {
          throw new AssertionError("Expected Right, got Left: " + l);
        },
        r -> r
    );
  }

  /**
   * Retrieves the Left value from an Either instance.
   *
   * @param <L>    the type of the Left value
   * @param <R>    the type of the Right value
   * @param either the Either instance
   * @return the Left value if present
   * @throws AssertionError if the Either contains a Right value
   */
  public static <L, R> L getLeft(Either<L, R> either) {
    return either.fold(
        l -> l,
        r -> {
          throw new AssertionError("Expected Left, got Right: " + r);
        }
    );
  }

  /**
   * Asserts against the left side of an Either.
   *
   * @param <L>    the type of the Left value
   * @param <R>    the type of the Right value
   * @param either the Either instance
   * @return an assertion object for the Left value
   */
  public static <L, R> AbstractObjectAssert<?, L> assertLeft(Either<L, R> either) {
    return assertThat(getLeft(either));
  }

  /**
   * Asserts against the right side of an Either.
   *
   * @param <L>    the type of the Left value
   * @param <R>    the type of the Right value
   * @param either the Either instance
   * @return an assertion object for the Right value
   */
  public static <L, R> AbstractObjectAssert<?, R> assertRight(Either<L, R> either) {
    return assertThat(getRight(either));
  }

}
