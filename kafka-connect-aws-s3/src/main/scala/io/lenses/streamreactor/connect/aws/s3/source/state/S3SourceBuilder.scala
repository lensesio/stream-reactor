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
package io.lenses.streamreactor.connect.aws.s3.source.state

import cats.effect.IO
import cats.effect.kernel.Ref
import io.lenses.streamreactor.connect.aws.s3.auth.AwsS3ClientCreator
import io.lenses.streamreactor.connect.aws.s3.config.ConnectorTaskId
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import io.lenses.streamreactor.connect.aws.s3.source.config.S3SourceConfig
import io.lenses.streamreactor.connect.aws.s3.source.distribution.PartitionSearcher
import io.lenses.streamreactor.connect.aws.s3.source.files.S3SourceFileQueue
import io.lenses.streamreactor.connect.aws.s3.source.reader.PartitionDiscovery
import io.lenses.streamreactor.connect.aws.s3.source.reader.ReaderManager
import io.lenses.streamreactor.connect.aws.s3.source.reader.ReaderManagerState
import io.lenses.streamreactor.connect.aws.s3.source.reader.ResultReader
import io.lenses.streamreactor.connect.aws.s3.storage.AwsS3StorageInterface
import org.apache.kafka.connect.errors.ConnectException

import java.util
import scala.jdk.CollectionConverters.IteratorHasAsScala

object S3SourceState {
  def make(
    props:           util.Map[String, String],
    contextOffsetFn: S3Location => Option[S3Location],
  ): IO[BuilderResult] =
    for {
      connectorTaskId  <- IO.fromEither(ConnectorTaskId.fromProps(props))
      config           <- IO.fromEither(S3SourceConfig.fromProps(props))
      s3Client         <- IO.fromEither(AwsS3ClientCreator.make(config.s3Config))
      storageInterface <- IO.delay(new AwsS3StorageInterface(connectorTaskId, s3Client, config.batchDelete))
      partitionSearcher <- IO.delay(
        new PartitionSearcher(
          config.bucketOptions.map(_.sourceBucketAndPrefix),
          config.partitionSearcher,
          connectorTaskId,
          s3Client.listObjectsV2Paginator(_).iterator().asScala,
        ),
      )

      readerManagerState <- Ref[IO].of(ReaderManagerState(Seq.empty, Seq.empty))
      cancelledRef       <- Ref[IO].of(false)
    } yield {
      val readerManagerCreateFn: (S3Location, String) => IO[ReaderManager] = (root, path) => {
        for {
          sbo <- IO.fromEither(
            config.bucketOptions.find(sb => sb.sourceBucketAndPrefix == root).toRight(
              new ConnectException(s"No root found for path:$path"),
            ),
          )
          ref      <- Ref[IO].of(Option.empty[ResultReader])
          listingFn = sbo.createBatchListerFn(storageInterface)
          source = contextOffsetFn(root).fold(new S3SourceFileQueue(connectorTaskId, listingFn))(
            S3SourceFileQueue.from(
              listingFn,
              storageInterface.getBlobModified,
              _,
              connectorTaskId,
            ),
          )
        } yield ReaderManager(
          sbo.recordsLimit,
          source,
          ResultReader.create(sbo.format,
                              sbo.targetTopic,
                              sbo.getPartitionExtractorFn,
                              connectorTaskId,
                              storageInterface,
          ),
          connectorTaskId,
          ref,
        )
      }
      val partitionDiscoveryLoop = PartitionDiscovery.run(config.partitionSearcher,
                                                          partitionSearcher.find,
                                                          readerManagerCreateFn,
                                                          readerManagerState,
                                                          cancelledRef,
      )
      BuilderResult(new S3SourceTaskState(() => readerManagerState.get.map(_.readerManagers)),
                    cancelledRef,
                    partitionDiscoveryLoop,
      )
    }
}

case class BuilderResult(
  state:                  S3SourceTaskState,
  cancelledRef:           Ref[IO, Boolean],
  partitionDiscoveryLoop: IO[Unit],
)
