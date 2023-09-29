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
package io.lenses.streamreactor.connect.aws.s3.source.reader

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.implicits.toShow
import cats.implicits.toTraverseOps
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.source.config.PartitionSearcherOptions
import io.lenses.streamreactor.connect.aws.s3.source.distribution.PartitionSearcherResponse
import io.lenses.streamreactor.connect.cloud.common.utils.PollLoop
import io.lenses.streamreactor.connect.cloud.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.model.location.CloudLocation

object PartitionDiscovery extends LazyLogging {

  type PartitionSearcherF = Seq[PartitionSearcherResponse] => IO[Seq[PartitionSearcherResponse]]

  def run(
    connectorTaskId:       ConnectorTaskId,
    settings:              PartitionSearcherOptions,
    partitionSearcher:     PartitionSearcherF,
    readerManagerCreateFn: (CloudLocation, String) => IO[ReaderManager],
    readerManagerState:    Ref[IO, ReaderManagerState],
    cancelledRef:          Ref[IO, Boolean],
  ): IO[Unit] = {
    val task = for {
      _        <- IO(logger.info(s"[${connectorTaskId.show}] Starting the partition discovery task."))
      oldState <- readerManagerState.get
      newParts <- partitionSearcher(oldState.partitionResponses)
      tuples    = newParts.flatMap(part => part.results.partitions.map(part.root -> _))
      newReaderManagers <- tuples
        .map {
          case (location, path) =>
            logger.info(s"[${connectorTaskId.show}] Creating a new reader manager for [$path].")
            readerManagerCreateFn(location, path)
        }.traverse(identity)
      newState = oldState.copy(
        partitionResponses = newParts,
        readerManagers     = oldState.readerManagers ++ newReaderManagers,
      )
      _ <- readerManagerState.set(newState)
      _ <- IO(logger.info(s"[${connectorTaskId.show}] Finished the partition discovery task."))
    } yield ()

    if (!settings.continuous) {
      IO.delay(logger.info(s"[${connectorTaskId.show}] Partition discovery task will only run once.")) >>
        PollLoop.oneOfIgnoreError(
          settings.interval,
          cancelledRef,
          logError(_, connectorTaskId),
        )(() => task)
    } else {
      IO.delay(logger.info(s"[${connectorTaskId.show}] Partition discovery task will run continuously.")) >>
        PollLoop.run(settings.interval, cancelledRef)(() =>
          task.handleErrorWith(err => IO.delay(logError(err, connectorTaskId))),
        )
    }
  }

  private def logError(err: Throwable, connectorTaskId: ConnectorTaskId): Unit = logger.error(
    s"[${connectorTaskId.show}] Error in partition discovery task. Partition discovery will resume.",
    err,
  )
}
