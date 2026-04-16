/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.source.config

/**
 * Settings for the backoff strategy used when there are no files to process.
 * @param initialDelay The initial delay before the first retry
 * @param maxBackoff The maximum backoff time
 * @param backoffMultiplier The multiplier for the backoff time
 */
case class EmptySourceBackoffSettings(
  initialDelay:      Long,
  maxBackoff:        Long,
  backoffMultiplier: Double,
)
