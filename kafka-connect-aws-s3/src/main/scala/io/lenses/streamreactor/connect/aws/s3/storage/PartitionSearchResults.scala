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
package io.lenses.streamreactor.connect.aws.s3.storage

import cats.implicits.catsSyntaxOptionId
import cats.implicits.none
import io.lenses.streamreactor.connect.aws.s3.source.config.PartitionSearcherOptions
import cats.effect.Clock
import cats.effect.IO

import java.time.Instant

object DirectoryFindCompletionConfig {

  def fromSearchOptions(
    searchOptions: PartitionSearcherOptions,
    clock:         Clock[IO],
  ): IO[DirectoryFindCompletionConfig] =
    for {
      pauseAfter <- IO.delay(searchOptions.pauseSearchAfterTime)
      now        <- clock.realTimeInstant

    } yield DirectoryFindCompletionConfig(
      searchOptions.recurseLevels,
      searchOptions.pauseSearchOnPartitionCount,
      pauseAfter.map(now.plus(_)),
      clock,
    )
}

case class DirectoryFindCompletionConfig(
  levelsToRecurse: Int,
  minResults:      Option[Int],
  maxTime:         Option[Instant],
  clock:           Clock[IO],
) {

  def stopReason(partitionsFound: Int): IO[Option[String]] = {
    val maxPartsFound = minResults.exists(partitionsFound >= _)
    timeExpired.map {
      exp =>
        (maxPartsFound, exp) match {
          case (true, true)   => "pt".some
          case (true, false)  => "p".some
          case (false, true)  => "t".some
          case (false, false) => none
        }
    }

  }

  private def timeExpired: IO[Boolean] =
    for {
      maxTime <- IO.delay(maxTime)
      instant <- clock.realTimeInstant
    } yield {
      maxTime.exists(instant.isAfter)
    }
}

trait DirectoryFindResults {
  def partitions: Set[String]
}

case class CompletedDirectoryFindResults(
  partitions: Set[String],
) extends DirectoryFindResults

// if triggered by minResults or maxTime
case class PausedDirectoryFindResults(
  partitions: Set[String],
  reason:     String,
  resumeFrom: String,
) extends DirectoryFindResults
