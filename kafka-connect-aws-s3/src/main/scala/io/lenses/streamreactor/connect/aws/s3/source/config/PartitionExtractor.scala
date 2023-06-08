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

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.config.Format

/**
  * For a source, can extract the original partition so that the message can be returned to the original partition.
  */
sealed trait PartitionExtractor {

  def extract(remoteS3Path: String): Option[Int]
}

object PartitionExtractor extends LazyLogging {
  def apply(extractorType: String, extractorRegex: Option[String]): Option[PartitionExtractor] =
    extractorType.toLowerCase match {
      case "regex" =>
        extractorRegex.fold {
          logger.info("No regex provided for regex mode, defaulting to NoOp mode")
          Option.empty[PartitionExtractor]
        }(rex => Some(new RegexPartitionExtractor(rex)))
      case "hierarchical" => Some(new HierarchicalPartitionExtractor())
      case _              => Option.empty[PartitionExtractor]
    }
}
class RegexPartitionExtractor(
  val pathRegex: String,
) extends PartitionExtractor
    with LazyLogging {
  private val rc = pathRegex.r
  logger.debug("Initialised RegexPartitionExtractor with regex {}", pathRegex)
  override def extract(remoteS3Path: String): Option[Int] =
    Option(rc.findAllIn(remoteS3Path).group(1)).flatMap(_.toIntOption)
}

class HierarchicalPartitionExtractor()
    extends RegexPartitionExtractor(
      s"(?i)^(?:.*)\\/([0-9]*)\\/(?:[0-9]*)[.](?:${Format.values.map(_.entryName).mkString("|")})$$",
    ) {}
