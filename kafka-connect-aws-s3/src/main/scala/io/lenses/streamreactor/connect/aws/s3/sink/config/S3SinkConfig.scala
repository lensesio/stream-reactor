
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
import io.lenses.streamreactor.connect.aws.s3.config.S3FlushSettings.{defaultFlushCount, defaultFlushInterval, defaultFlushSize}
import io.lenses.streamreactor.connect.aws.s3.config.{FormatSelection, S3Config, S3ConfigDefBuilder}
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3RootLocation
import io.lenses.streamreactor.connect.aws.s3.model.{PartitionSelection, S3OutputStreamOptions, StreamedWriteOutputStreamOptions}
import io.lenses.streamreactor.connect.aws.s3.sink._

object S3SinkConfig {

  def apply(s3ConfigDefBuilder: S3ConfigDefBuilder): Either[Exception, S3SinkConfig] = {
    SinkBucketOptions(s3ConfigDefBuilder) match {
      case Left(ex) => ex.asLeft[S3SinkConfig]
      case Right(value) => S3SinkConfig(S3Config(s3ConfigDefBuilder.getParsedValues), value).asRight
    }

  }

}

case class S3SinkConfig(
                         s3Config: S3Config,
                         bucketOptions: Set[SinkBucketOptions] = Set.empty
                       )

object SinkBucketOptions extends LazyLogging {

  def apply(config: S3ConfigDefBuilder): Either[Exception, Set[SinkBucketOptions]] = {

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

      val s3WriteOptions = config.s3WriteOptions(config)
      s3WriteOptions match {
        case Right(value) => SinkBucketOptions(
          kcql.getSource,
          RemoteS3RootLocation(kcql.getTarget),
          formatSelection = formatSelection,
          fileNamingStrategy = namingStrategy,
          partitionSelection = partitionSelection,
          commitPolicy = config.commitPolicy(kcql),
          writeMode = value,
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
                              commitPolicy: CommitPolicy = DefaultCommitPolicy(Some(defaultFlushSize), Some(defaultFlushInterval), Some(defaultFlushCount)),
                              writeMode: S3OutputStreamOptions = StreamedWriteOutputStreamOptions(),
                            )
