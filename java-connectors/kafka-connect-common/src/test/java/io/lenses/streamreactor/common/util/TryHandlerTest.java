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
import org.junit.jupiter.api.Test;

import static io.lenses.streamreactor.test.utils.OptionValues.getValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TryHandlerTest {

  @Test
  void logTryShouldReturnOptionWithResultWhenAndDiscardTryIsSuccessful() {
    Try<String, Throwable> successfulTry = Try.success("Success");
    Option<String> result = TryHandler.logAndDiscardTry(successfulTry, "ErrorPrefix");

    assertTrue(result.isPresent());
    assertEquals("Success", getValue(result));
  }

  @Test
  void logTryShouldReturnEmptyOptionWhenAndDiscardTryFails() {
    Try<String, Throwable> failedTry = Try.failure(new RuntimeException("Failure"));
    Option<String> result = TryHandler.logAndDiscardTry(failedTry, "ErrorPrefix");

    assertTrue(result.stream().isEmpty());
  }

}
