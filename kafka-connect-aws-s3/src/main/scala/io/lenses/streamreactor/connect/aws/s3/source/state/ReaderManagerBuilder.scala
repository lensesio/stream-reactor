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
package io.lenses.streamreactor.connect.aws.s3.source.state

import cats.effect.IO
import cats.effect.Ref
import io.lenses.streamreactor.connect.aws.s3.source.config.SourceBucketOptions
import io.lenses.streamreactor.connect.aws.s3.storage.S3FileMetadata
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.source.files.CloudSourceFileQueue
import io.lenses.streamreactor.connect.cloud.common.source.reader.ReaderManager
import io.lenses.streamreactor.connect.cloud.common.source.reader.ResultReader
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import org.apache.kafka.connect.errors.ConnectException

/**
  * Responsible for creating an instance of {{{ReaderManager}}} for a given path.
  */
object ReaderManagerBuilder {
  def apply(
    root:             CloudLocation,
    path:             String,
    storageInterface: StorageInterface[S3FileMetadata],
    connectorTaskId:  ConnectorTaskId,
    contextOffsetFn:  CloudLocation => Option[CloudLocation],
    findSboF:         CloudLocation => Option[SourceBucketOptions],
  )(
    implicit
    cloudLocationValidator: CloudLocationValidator,
  ): IO[ReaderManager] =
    for {
      sbo <- IO.fromEither(
        findSboF(root).toRight(
          new ConnectException(s"No root found for path:$path"),
        ),
      )
      ref        <- Ref[IO].of(Option.empty[ResultReader])
      adaptedRoot = root.copy(prefix = Some(path))
      adaptedSbo  = sbo.copy(sourceBucketAndPrefix = adaptedRoot)
      listingFn   = adaptedSbo.createBatchListerFn(storageInterface)
      source = contextOffsetFn(adaptedRoot).fold {
        new CloudSourceFileQueue[S3FileMetadata](connectorTaskId, listingFn)
      } { location =>
        CloudSourceFileQueue.from[S3FileMetadata](
          listingFn,
          storageInterface,
          location,
          connectorTaskId,
        )
      }
    } yield new ReaderManager(
      sbo.recordsLimit,
      source,
      ResultReader.create(sbo.format,
                          sbo.targetTopic,
                          sbo.getPartitionExtractorFn,
                          connectorTaskId,
                          storageInterface,
                          sbo.hasEnvelope,
      ),
      connectorTaskId,
      ref,
    )

}
