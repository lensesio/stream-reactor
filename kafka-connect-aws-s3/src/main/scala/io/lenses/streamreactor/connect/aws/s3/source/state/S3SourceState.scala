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
import cats.effect.kernel.Ref
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.aws.s3.auth.AwsS3ClientCreator
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.CONNECTOR_PREFIX
import io.lenses.streamreactor.connect.aws.s3.source.config.S3SourceConfig
import io.lenses.streamreactor.connect.aws.s3.source.distribution.PartitionSearcher
import io.lenses.streamreactor.connect.aws.s3.storage.AwsS3StorageInterface
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskIdCreator
import io.lenses.streamreactor.connect.cloud.common.model.location.{CloudLocation, CloudLocationValidator}
import io.lenses.streamreactor.connect.cloud.common.source.reader.{PartitionDiscovery, ReaderManager, ReaderManagerState}
import io.lenses.streamreactor.connect.cloud.common.source.state.{BuilderResult, CloudSourceTaskState, ReaderManagerBuilder}

import scala.jdk.CollectionConverters.IteratorHasAsScala

object S3SourceState extends StrictLogging {
  def make(
    props:           Map[String, String],
    contextOffsetFn: CloudLocation => Option[CloudLocation],
  )(
    implicit
    cloudLocationValidator: CloudLocationValidator,
  ): IO[BuilderResult] =
    for {
      connectorTaskId  <- IO.fromEither(new ConnectorTaskIdCreator(CONNECTOR_PREFIX).fromProps(props))
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
      val readerManagerCreateFn: (CloudLocation, String) => IO[ReaderManager] = (root, path) => {
        ReaderManagerBuilder(
          root,
          path,
          storageInterface,
          connectorTaskId,
          contextOffsetFn,
          location => config.bucketOptions.find(sb => sb.sourceBucketAndPrefix == location),
        )
      }
      val partitionDiscoveryLoop = PartitionDiscovery.run(connectorTaskId,
                                                          config.partitionSearcher,
                                                          partitionSearcher.find,
                                                          readerManagerCreateFn,
                                                          readerManagerState,
                                                          cancelledRef,
      )
      BuilderResult(new CloudSourceTaskState(readerManagerState.get.map(_.readerManagers)),
                    cancelledRef,
                    partitionDiscoveryLoop,
      )
    }
}


