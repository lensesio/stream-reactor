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
package io.lenses.streamreactor.common.config.base;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * Configuration class for defining retry behavior.
 * This class encapsulates settings related to retrying operations, such as the maximum
 * number of retry attempts and the interval (in milliseconds) between retries.
 *
 * Different connector implementations may interpret these settings differently based
 * on their specific requirements and behavior.
 */
@Data
@Builder
@AllArgsConstructor
public class RetryConfig {

  /**
   * Maximum number of retry attempts allowed.
   * This value specifies the maximum number of times an operation will be retried
   * before giving up. A value of 0 indicates no retries will be attempted.
   */
  private int retryLimit;

  /**
   * Interval (in milliseconds) between retry attempts.
   * This value specifies the time delay between consecutive retry attempts,
   * measured in milliseconds.
   */
  private long retryIntervalMillis;
}
