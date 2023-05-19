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
package io.lenses.streamreactor.connect.aws.s3.source.config

import cats.effect.Clock
import cats.effect.IO

import java.time.Duration
import java.time.Instant

case class PartitionSearcherOptions(
  recurseLevels:               Int,
  blockOnSearch:               Boolean,
  searchInterval:              Duration, // searches again or resumes search in partial mode
  pauseSearchOnPartitionCount: Option[Int], // this is per root
  pauseSearchAfterTime:        Option[Duration], // this is per root
  clock:                       Clock[IO],
) {

  def rediscoverDue(lastSearchTime: Option[Instant]): IO[Boolean] =
    for {
      lst <- IO(lastSearchTime)
      now <- clock.realTimeInstant
    } yield {
      lst.fold(true) {
        st =>
          val nextSearchTime = st.plus(searchInterval)
          now.isAfter(nextSearchTime)
      }
    }

}
