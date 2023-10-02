/*
 * Copyright 2017-2023 Lenses.io Ltd
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

import scala.concurrent.duration.FiniteDuration

object PartitionSearcherOptions {
  val ExcludeIndexes: Set[String] = Set(".indexes")

}

/**
  * @param wildcardExcludes allows ignoring paths containing certain strings.  Mainly it is used to prevent us from reading anything inside the .indexes key prefix, as these should be ignored by the source.
  */
case class PartitionSearcherOptions(
  recurseLevels:    Int,
  continuous:       Boolean,
  interval:         FiniteDuration,
  wildcardExcludes: Set[String],
)
