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
import cats.implicits.toTraverseOps
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import io.lenses.streamreactor.connect.aws.s3.source.config.PartitionSearcherOptions
import io.lenses.streamreactor.connect.aws.s3.source.distribution.PartitionSearcher
import io.lenses.streamreactor.connect.aws.s3.utils.PollLoop

object PartitionDiscovery extends LazyLogging {

  def run(
    settings:              PartitionSearcherOptions,
    partitionSearcher:     PartitionSearcher,
    readerManagerCreateFn: (S3Location, String) => IO[ReaderManager],
    readerManagerState:    Ref[IO, ReaderManagerState],
    cancelledRef:          Ref[IO, Boolean],
  ): IO[Unit] = {
    val task = for {
      _ <- IO(logger.info("Starting the partition discovery task"))
      _ <- discover(partitionSearcher, readerManagerCreateFn, readerManagerState)
      _ <- IO(logger.info("Finished the partition discovery task"))
    } yield ()

    PollLoop.run(settings.interval, cancelledRef)(() =>
      task.handleErrorWith { err =>
        IO(logger.error("Error in partition discovery task. Partition discovery will resume.", err))
      },
    )
  }

  private def discover(
    partitionSearcher:     PartitionSearcher,
    readerManagerCreateFn: (S3Location, String) => IO[ReaderManager],
    readerManagerState:    Ref[IO, ReaderManagerState],
  ): IO[Unit] =
    for {
      oldState <- readerManagerState.get
      newParts <- partitionSearcher.find(oldState.partitionResponses)
      tuples    = newParts.flatMap(part => part.results.partitions.map(part.root -> _))
      newReaderManagers <- tuples
        .map {
          case (location, path) =>
            logger.info("Creating a new reader manager for {} {}", location.toString, path)
            readerManagerCreateFn(location, path)
        }.traverse(identity)
      newState = oldState.copy(
        partitionResponses = newParts,
        readerManagers     = oldState.readerManagers ++ newReaderManagers,
      )
      _ <- readerManagerState.set(newState)
    } yield ()

}
