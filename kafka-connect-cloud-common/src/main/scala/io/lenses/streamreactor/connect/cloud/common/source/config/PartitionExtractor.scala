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
package io.lenses.streamreactor.connect.cloud.common.source.config

import com.typesafe.scalalogging.LazyLogging

/**
  * For a source, can extract the original partition so that the message can be returned to the original partition.
  */
sealed trait PartitionExtractor {

  def extract(remotePath: String): Option[Int]
}

object PartitionExtractor extends LazyLogging {

  def apply(extractorType: String, extractorRegex: Option[String]): Option[PartitionExtractor] =
    extractorType.toLowerCase match {
      case "regex" =>
        extractorRegex.fold {
          logger.info("No regex provided for regex mode, defaulting to NoOp mode")
          Option.empty[PartitionExtractor]
        }(rex => Some(new RegexPartitionExtractor(rex)))
      case "hierarchical" => Some(HierarchicalPartitionExtractor)
      case _              => Option.empty[PartitionExtractor]
    }
}
class RegexPartitionExtractor(
  val pathRegex: String,
) extends PartitionExtractor
    with LazyLogging {
  private val rc = pathRegex.r
  logger.debug("Initialised RegexPartitionExtractor with regex {}", pathRegex)
  override def extract(remotePath: String): Option[Int] =
    Option(rc.findAllIn(remotePath).group(1)).flatMap(_.toIntOption)
}

object HierarchicalPartitionExtractor extends PartitionExtractor {
  override def extract(input: String): Option[Int] = {
    // Find the index of the last '/'
    val lastSlashIndex = input.lastIndexOf('/')

    // Find the index of the second to last '/'
    val secondLastSlashIndex = input.substring(0, lastSlashIndex).lastIndexOf('/')
    if (secondLastSlashIndex == -1)
      None
    else {
      val substring = input.substring(secondLastSlashIndex + 1, lastSlashIndex)
      Some(substring.toInt)
    }
  }
}
