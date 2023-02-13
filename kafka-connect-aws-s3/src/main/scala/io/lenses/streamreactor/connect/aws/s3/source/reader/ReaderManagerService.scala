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
import cats.effect.unsafe.implicits.global
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3RootLocation
import io.lenses.streamreactor.connect.aws.s3.source.config.PartitionSearcherOptions
import io.lenses.streamreactor.connect.aws.s3.source.distribution.PartitionSearcher
import io.lenses.streamreactor.connect.aws.s3.source.distribution.PartitionSearcherResponse

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

case class ReaderManagerState(
  partitionResponses: Seq[PartitionSearcherResponse],
  readerManagers:     Seq[ReaderManager],
) {
  def lastSearchTime: Option[Instant] = partitionResponses.map(_.lastSearchTime).minOption

}

class ReaderManagerService(
  settings:              PartitionSearcherOptions,
  partitionSearcher:     PartitionSearcher,
  readerManagerCreateFn: (RemoteS3RootLocation, String) => ReaderManager,
) extends LazyLogging {

  private val readerManagerState: AtomicReference[ReaderManagerState] =
    new AtomicReference(ReaderManagerState(Seq.empty, Seq.empty))

  def getReaderManagers: Seq[ReaderManager] = {
    launchDiscover().unsafeRunSync()
    readerManagerState.get().readerManagers
  }

  private def launchDiscover(): IO[Unit] = {

    val lastSearchTime = readerManagerState.get().lastSearchTime
    val rediscoverDue  = settings.rediscoverDue(lastSearchTime)

    if (rediscoverDue && settings.blockOnSearch) {
      rediscover()
    } else if (rediscoverDue && !settings.blockOnSearch) {
      rediscover().start *> IO.unit
    } else {
      IO.unit
    }
  }

  private def rediscover(): IO[Unit] =
    IO {
      readerManagerState.getAndUpdate {
        (state: ReaderManagerState) =>
          val ioState = IO.pure(state)
          calculateUpdatedState(ioState).unsafeRunSync()
      }
    } *> IO.unit

  private def calculateUpdatedState(state: IO[ReaderManagerState]): IO[ReaderManagerState] =
    for {
      _        <- IO(logger.debug("calculating updated state"))
      ioState  <- state
      newParts <- partitionSearcher.findNewPartitions(ioState.partitionResponses)
      newReaderManagers = newParts
        .flatMap(part => part.results.partitions.map(part.root -> _))
        .map {
          case (location, res) => readerManagerCreateFn(location, res)
        }
    } yield ioState.copy(
      partitionResponses = newParts,
      readerManagers     = readerManagerState.get().readerManagers ++ newReaderManagers,
    )

}
