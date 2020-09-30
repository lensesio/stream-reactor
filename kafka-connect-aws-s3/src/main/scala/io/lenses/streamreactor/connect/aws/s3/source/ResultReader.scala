/*
 * Copyright 2020 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.source

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.formats.S3FormatStreamReader
import io.lenses.streamreactor.connect.aws.s3.model.{PollResults, SourceData}

import scala.annotation.tailrec


class ResultReader(prefix: String, targetTopic: String) extends LazyLogging {

  /**
    * Retrieves the results for a particular reader, or None if no further results are available
    */
  def retrieveResults(reader: S3FormatStreamReader[_ <: SourceData], limit: Int): Option[PollResults] = {

    val results: Vector[_ <: SourceData] = retrieveResults(limit, reader, Vector.empty[SourceData])
    if (results.isEmpty) {
      logger.debug(s"No results found in reader ${reader.getBucketAndPath}")
      Option.empty[PollResults]
    } else {
      logger.debug(s"Results found in reader ${reader.getBucketAndPath}")
      Some(
        PollResults(
          results,
          reader.getBucketAndPath,
          prefix,
          targetTopic
        )
      )
    }

  }

  @tailrec
  final def retrieveResults(
                             limit: Int,
                             reader: S3FormatStreamReader[_ <: SourceData],
                             accumulatedResults: Vector[_ <: SourceData]
                           ): Vector[_ <: SourceData] = {

    logger.debug(s"Calling retrieveResults with limit ($limit), reader (${reader.getBucketAndPath}/${reader.getLineNumber}), accumulatedResults size ${accumulatedResults.size}")
    if (limit > 0 && reader.hasNext) {
      retrieveResults(limit - 1, reader, accumulatedResults :+ reader.next())
    } else {
      accumulatedResults
    }
  }

}
