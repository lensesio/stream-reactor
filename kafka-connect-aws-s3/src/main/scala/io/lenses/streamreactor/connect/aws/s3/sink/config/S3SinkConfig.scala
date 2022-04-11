
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

package io.lenses.streamreactor.connect.aws.s3.sink.config

import cats.syntax.all._
import com.datamountaineer.kcql.Kcql
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.config.Format.Json
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.{SEEK_MAX_INDEX_FILES, SEEK_MIGRATION}
import io.lenses.streamreactor.connect.aws.s3.config.S3FlushSettings.{defaultFlushCount, defaultFlushInterval, defaultFlushSize}
import io.lenses.streamreactor.connect.aws.s3.config.{FormatSelection, S3Config, S3ConfigDefBuilder}
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3RootLocation
import io.lenses.streamreactor.connect.aws.s3.model.{LocalStagingArea, PartitionSelection}
import io.lenses.streamreactor.connect.aws.s3.sink._

object S3SinkConfig {

  def apply(s3ConfigDefBuilder: S3ConfigDefBuilder): Either[Throwable, S3SinkConfig] = {
    for {
      sinkBucketOptions <- SinkBucketOptions(s3ConfigDefBuilder)
      offsetSeekerOptions = OffsetSeekerOptions(
        s3ConfigDefBuilder.getInt(SEEK_MAX_INDEX_FILES),
        s3ConfigDefBuilder.getBoolean(SEEK_MIGRATION)
      )
    } yield S3SinkConfig(S3Config(s3ConfigDefBuilder.getParsedValues), sinkBucketOptions, offsetSeekerOptions)

  }

}

case class S3SinkConfig(
                         s3Config: S3Config,
                         bucketOptions: Set[SinkBucketOptions] = Set.empty,
                         offsetSeekerOptions: OffsetSeekerOptions,
                       )

object SinkBucketOptions extends LazyLogging {

  def apply(config: S3ConfigDefBuilder): Either[Throwable, Set[SinkBucketOptions]] = {

    config.getKCQL.map { kcql: Kcql =>

      val formatSelection: FormatSelection = Option(kcql.getStoredAs) match {
        case Some(format: String) => FormatSelection(format)
        case None => FormatSelection(Json, Set.empty)
      }

      val partitionSelection = PartitionSelection(kcql)
      val namingStrategy = partitionSelection match {
        case Some(partSel) => new PartitionedS3FileNamingStrategy(formatSelection, partSel)
        case None => new HierarchicalS3FileNamingStrategy(formatSelection)
      }

      val stagingArea = LocalStagingArea(config)
      stagingArea match {
        case Right(value) => SinkBucketOptions(
          kcql.getSource,
          RemoteS3RootLocation(kcql.getTarget),
          formatSelection = formatSelection,
          fileNamingStrategy = namingStrategy,
          partitionSelection = partitionSelection,
          commitPolicy = config.commitPolicy(kcql),
          localStagingArea = value,
        )
        case Left(exception) => return exception.asLeft[Set[SinkBucketOptions]]
      }
    }.asRight

  }

}

case class SinkBucketOptions(
                              sourceTopic: String,
                              bucketAndPrefix: RemoteS3RootLocation,
                              formatSelection: FormatSelection,
                              fileNamingStrategy: S3FileNamingStrategy,
                              partitionSelection: Option[PartitionSelection] = None,
                              commitPolicy: CommitPolicy = DefaultCommitPolicy(Some(defaultFlushSize.toLong), Some(defaultFlushInterval), Some(defaultFlushCount.toLong)),
                              localStagingArea: LocalStagingArea,
                            )


case class OffsetSeekerOptions(
                                maxIndexFiles: Int,
                                migrate: Boolean,
                              )