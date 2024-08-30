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
package io.lenses.streamreactor.common.util;

import java.util.function.Supplier;
import java.util.function.Consumer;

public class TimeLogger {

  // Method which wraps any function call and captures the time taken to execute the function in nanoseconds
  // And calls a function provided to record the time taken
  public static <T> T measureTime(String logInfo, Supplier<T> supplier, Consumer<String> recorder) {
    long start = System.nanoTime();
    try {
      return supplier.get();
    } finally {
      long end = System.nanoTime();
      String metricLog = String.format("Time taken to execute %s: %d ns", logInfo, end - start);
      recorder.accept(metricLog);
    }
  }
}
