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
import cats.implicits._
import io.lenses.streamreactor.connect.aws.s3.auth.AwsS3ClientCreator
import io.lenses.streamreactor.connect.aws.s3.config.ConnectorTaskId
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import io.lenses.streamreactor.connect.aws.s3.source.WrappedSourceException
import io.lenses.streamreactor.connect.aws.s3.source.config.S3SourceConfig
import io.lenses.streamreactor.connect.aws.s3.source.distribution.PartitionSearcher
import io.lenses.streamreactor.connect.aws.s3.source.reader.ReaderManager
import io.lenses.streamreactor.connect.aws.s3.source.reader.ReaderManagerService
import io.lenses.streamreactor.connect.aws.s3.storage.AwsS3StorageInterface
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.SourceRecord

import java.io.Closeable
import java.util

class S3SourceTaskState(
  latestReaderManagersFn: () => Seq[ReaderManager],
) extends Closeable {

  override def close(): Unit =
    latestReaderManagersFn().foreach(_.close())

  def poll(): Either[Throwable, Seq[SourceRecord]] =
    latestReaderManagersFn()
      .flatMap(_.poll())
      .flatMap(_.toSourceRecordList)
      .asRight[Throwable]

}

object S3SourceTaskState {
  def make(
    props:           util.Map[String, String],
    contextOffsetFn: S3Location => Option[S3Location],
  ): Either[Throwable, S3SourceTaskState] = {
    for {
      connectorTaskId <- ConnectorTaskId.fromProps(props)
      config          <- S3SourceConfig.fromProps(props)
      s3Client        <- AwsS3ClientCreator.make(config.s3Config)
      storageInterface = new AwsS3StorageInterface(connectorTaskId, s3Client)
      partitionSearcher = new PartitionSearcher(
        config.bucketOptions.map(_.sourceBucketAndPrefix),
        config.partitionSearcher,
      )(
        connectorTaskId,
        storageInterface,
      )

    } yield {
      val readerManagerCreateFn: (S3Location, String) => ReaderManager = (root, rootPath) => {
        val sbo = config.bucketOptions.find(sb => sb.sourceBucketAndPrefix == root).getOrElse(
          throw new ConnectException("no root found"),
        )
        ReaderManager(root, sbo)(connectorTaskId, storageInterface, contextOffsetFn)
      }
      val readerManagerService =
        new ReaderManagerService(config.partitionSearcher, partitionSearcher, readerManagerCreateFn)
      new S3SourceTaskState(() => readerManagerService.getReaderManagers)
    }
  }
    .leftMap {
      case s: String => new WrappedSourceException(s)
    }
}
