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
import cats.effect.unsafe.implicits.global
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3RootLocation
import io.lenses.streamreactor.connect.aws.s3.source.config.PartitionSearcherOptions
import io.lenses.streamreactor.connect.aws.s3.source.distribution.PartitionSearcher
import io.lenses.streamreactor.connect.aws.s3.source.distribution.PartitionSearcherResponse

import java.time.Instant

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

  private val readerManagerState: Ref[IO, ReaderManagerState] =
    Ref[IO].of(ReaderManagerState(Seq.empty, Seq.empty)).unsafeRunSync()

  def getReaderManagers: Seq[ReaderManager] = {
    launchDiscover().unsafeRunSync()
    readerManagerState.get.map(_.readerManagers).unsafeRunSync()
  }

  private def launchDiscover(): IO[Unit] = {
    for {
      ioState       <- readerManagerState.get
      lastSearchTime = ioState.lastSearchTime
      rediscoverDue  = settings.rediscoverDue(lastSearchTime)
    } yield {
      if (rediscoverDue && settings.blockOnSearch) {
        rediscover()
      } else if (rediscoverDue && !settings.blockOnSearch) {
        rediscover().start *> IO.unit
      } else {
        IO.unit
      }
    }
  }.flatten

  private def rediscover(): IO[Unit] =
    for {
      _ <- IO(logger.debug("calculating updated state"))
      _ <- readerManagerState.getAndUpdate {
        oldState: ReaderManagerState =>
          {
            for {
              newParts <- partitionSearcher.findNewPartitions(oldState.partitionResponses)
              newReaderManagers = newParts
                .flatMap(part => part.results.partitions.map(part.root -> _))
                .map {
                  case (location, res) => readerManagerCreateFn(location, res)
                }
              newState = oldState.copy(
                partitionResponses = newParts,
                readerManagers     = oldState.readerManagers ++ newReaderManagers,
              )
            } yield newState
          }.unsafeRunSync()
      }
    } yield ()

}
