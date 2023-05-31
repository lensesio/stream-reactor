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
  readerManagerCreateFn: (S3Location, String) => IO[ReaderManager],
  readerManagerState:    Ref[IO, ReaderManagerState],
) extends LazyLogging {

  def getReaderManagers: IO[Seq[ReaderManager]] =
    for {
      //TODO: why is this requested here?. this is a get, and should be a pure function
      _     <- launchDiscover()
      state <- readerManagerState.get
    } yield state.readerManagers

  private def launchDiscover(): IO[Unit] =
    for {
      state         <- readerManagerState.get
      lastSearchTime = state.lastSearchTime
      rediscoverDue <- settings.shouldRediscover(lastSearchTime)
      u <- if (rediscoverDue) {
        if (settings.blockOnSearch) rediscover()
        else rediscover().background.use_
      } else IO.unit
    } yield u

  private def rediscover(): IO[Unit] =
    for {
      _        <- IO(logger.debug("calculating updated state"))
      oldState <- readerManagerState.get
      newParts <- partitionSearcher.findNewPartitions(oldState.partitionResponses)
      newReaderManagers <- newParts
        .flatMap(part => part.results.partitions.map(part.root -> _))
        .map { case (location, res) => readerManagerCreateFn(location, res) }.traverse(identity)
      newState = oldState.copy(
        partitionResponses = newParts,
        readerManagers     = oldState.readerManagers ++ newReaderManagers,
      )
      _ <- readerManagerState.set(newState)
    } yield ()

}
