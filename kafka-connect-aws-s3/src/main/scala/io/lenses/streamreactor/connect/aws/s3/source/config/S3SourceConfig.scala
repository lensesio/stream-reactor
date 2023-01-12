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
package io.lenses.streamreactor.connect.aws.s3.source.config

import com.datamountaineer.kcql.Kcql
import io.lenses.streamreactor.connect.aws.s3.config.Format.Json
import io.lenses.streamreactor.connect.aws.s3.config.S3Config.getString
import io.lenses.streamreactor.connect.aws.s3.config.FormatSelection
import io.lenses.streamreactor.connect.aws.s3.config.S3Config
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigDefBuilder
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.SOURCE_PARTITION_EXTRACTOR_REGEX
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.SOURCE_PARTITION_EXTRACTOR_TYPE
import io.lenses.streamreactor.connect.aws.s3.model.CompressionCodec
import io.lenses.streamreactor.connect.aws.s3.model.PartitionExtractor
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3RootLocation

object S3SourceConfig {

  def apply(s3ConfigDefBuilder: S3ConfigDefBuilder): S3SourceConfig = {
    val parsedValues = s3ConfigDefBuilder.getParsedValues
    S3SourceConfig(
      S3Config(parsedValues),
      SourceBucketOptions(
        s3ConfigDefBuilder,
        PartitionExtractor(
          getString(parsedValues, SOURCE_PARTITION_EXTRACTOR_TYPE).getOrElse("none"),
          getString(parsedValues, SOURCE_PARTITION_EXTRACTOR_REGEX),
        ),
      ),
      s3ConfigDefBuilder.getCompressionCodec(),
    )

  }
}

case class S3SourceConfig(
  s3Config:         S3Config,
  bucketOptions:    Seq[SourceBucketOptions] = Seq.empty,
  compressionCodec: CompressionCodec,
)

case class SourceBucketOptions(
  sourceBucketAndPrefix: RemoteS3RootLocation,
  targetTopic:           String,
  format:                FormatSelection,
  recordsLimit:          Int,
  filesLimit:            Int,
  partitionExtractor:    Option[PartitionExtractor],
) {

  def getPartitionExtractorFn: String => Option[Int] =
    partitionExtractor.fold((_: String) => Option.empty[Int])(_.extract)

}

object SourceBucketOptions {

  private val DEFAULT_RECORDS_LIMIT = 1024
  private val DEFAULT_FILES_LIMIT   = 1000

  def apply(config: S3ConfigDefBuilder, partitionExtractor: Option[PartitionExtractor]): Seq[SourceBucketOptions] =
    config.getKCQL.map {

      kcql: Kcql =>
        val formatSelection: FormatSelection = Option(kcql.getStoredAs) match {
          case Some(format: String) => FormatSelection.fromString(format)
          case None => FormatSelection(Json, Set.empty)
        }
        SourceBucketOptions(
          RemoteS3RootLocation(kcql.getSource, allowSlash = true),
          kcql.getTarget,
          format             = formatSelection,
          recordsLimit       = if (kcql.getLimit < 1) DEFAULT_RECORDS_LIMIT else kcql.getLimit,
          filesLimit         = if (kcql.getBatchSize < 1) DEFAULT_FILES_LIMIT else kcql.getBatchSize,
          partitionExtractor = partitionExtractor,
        )
    }.toList

}
