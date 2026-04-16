/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.source.state

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.implicits.toTraverseOps
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.source.CommitWatermark
import io.lenses.streamreactor.connect.cloud.common.source.reader.ReaderManager
import org.apache.kafka.connect.source.SourceRecord
case class CloudLocationKey(bucket: String, prefix: Option[String])

case class CloudSourceTaskState(
  latestReaderManagers:   IO[Map[CloudLocationKey, ReaderManager]],
  cancelledRef:           Ref[IO, Boolean],
  partitionDiscoveryLoop: IO[Unit],
) extends LazyLogging {
  def close(): IO[Unit] =
    latestReaderManagers
      .flatMap(_.values.toList.traverse(_.close()))
      .attempt
      .void

  def poll(): IO[Seq[SourceRecord]] =
    for {
      readers      <- latestReaderManagers
      pollResults  <- readers.view.values.map(_.poll()).toList.traverse(identity)
      sourceRecords = pollResults.flatten
    } yield sourceRecords

  def commitRecord(cloudLocation: CloudLocation, commitWatermark: CommitWatermark): IO[Unit] =
    for {
      rms <- latestReaderManagers
      fin <- rms.get(cloudLocation.toKey).traverse(_.postProcess(commitWatermark))
      _ <- IO.delay(
        logger.info(s"CloudSourceTaskState.commitRecord with cloudLocation: {}, readerManagers: {}, fin: {}",
                    cloudLocation,
                    rms,
                    fin,
        ),
      )
    } yield ()
}
