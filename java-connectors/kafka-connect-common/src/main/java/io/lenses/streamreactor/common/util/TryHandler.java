/*
 * Copyright 2017-2026 Lenses.io Ltd
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

import cyclops.control.Option;
import cyclops.control.Try;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility class for handling Try operations and logging results.
 */
@Slf4j
public class TryHandler {

  /**
   * Logs the result of a Try operation and converts it to an Option.
   *
   * @param <A>         the type of the successful result
   * @param <B>         the type of the Throwable
   * @param attempt     the Try operation to handle
   * @param errorPrefix the prefix to use in the log message for failed attempts
   * @return an Option containing the successful result, or an empty Option if the Try failed
   */
  public static <A, B extends Throwable> Option<A> logAndDiscardTry(Try<A, B> attempt, String errorPrefix) {
    return attempt.peek(success -> log.debug("Successful try: {}", success)).peekFailed(
        error -> log.debug("{} Failed try: {}", errorPrefix, error.getMessage()))
        .toOption();
  }
}
