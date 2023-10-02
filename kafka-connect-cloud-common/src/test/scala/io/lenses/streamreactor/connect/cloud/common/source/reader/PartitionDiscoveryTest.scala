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
package io.lenses.streamreactor.connect.cloud.common.source.reader

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.unsafe.implicits.global
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.source.config.PartitionSearcherOptions
import io.lenses.streamreactor.connect.cloud.common.source.config.PartitionSearcherOptions.ExcludeIndexes
import io.lenses.streamreactor.connect.cloud.common.source.distribution.PartitionSearcherResponse
import io.lenses.streamreactor.connect.cloud.common.source.files.SourceFileQueue
import io.lenses.streamreactor.connect.cloud.common.storage.DirectoryFindResults
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt

class PartitionDiscoveryTest extends AnyFlatSpecLike with Matchers with MockitoSugar {
  private implicit val cloudLocationValidator: CloudLocationValidator = SampleData.cloudLocationValidator
  private val connectorTaskId:                 ConnectorTaskId        = ConnectorTaskId("sinkName", 1, 1)
  "PartitionDiscovery" should "handle failure on PartitionSearcher and resume" in {
    val fileQueueProcessor: SourceFileQueue = mock[SourceFileQueue]
    val limit   = 10
    val options = PartitionSearcherOptions(1, continuous = true, 100.millis, ExcludeIndexes)

    trait Count {
      def getCount: IO[Int]

      def find(
        lastFound: Seq[PartitionSearcherResponse],
      ): IO[Seq[PartitionSearcherResponse]]
    }
    val searcherMock = new Count {
      private val count = Ref[IO].of(0).unsafeRunSync()
      def getCount: IO[Int] = count.get

      def find(
        lastFound: Seq[PartitionSearcherResponse],
      ): IO[Seq[PartitionSearcherResponse]] =
        for {
          c <- count.getAndUpdate(_ + 1)
          _ <- if (c == 0) IO.raiseError(new RuntimeException("error")) else IO.unit
        } yield {
          List(
            PartitionSearcherResponse(CloudLocation("bucket", None),
                                      Set("prefix1/", "prefix2/"),
                                      DirectoryFindResults(Set("prefix1/", "prefix2/")),
                                      None,
            ),
          )
        }
    }

    val io = for {
      cancelledRef <- Ref[IO].of(false)
      readerRef    <- Ref[IO].of(Option.empty[ResultReader])
      state        <- Ref[IO].of(ReaderManagerState(Seq.empty, Seq.empty))
      fiber <- PartitionDiscovery.run(
        connectorTaskId,
        options,
        searcherMock.find,
        (_, _) =>
          IO(new ReaderManager(limit,
                               fileQueueProcessor,
                               _ => Left(new RuntimeException()),
                               connectorTaskId,
                               readerRef,
          )),
        state,
        cancelledRef,
      ).start
      _              <- IO.sleep(1000.millis)
      _              <- cancelledRef.set(true)
      _              <- fiber.join
      readerMgrState <- state.get
      callsMade      <- searcherMock.getCount
    } yield readerMgrState -> callsMade

    val (state, callsMade) = io.unsafeRunSync()
    assert(
      state.partitionResponses == List(
        PartitionSearcherResponse(
          CloudLocation("bucket", None),
          Set("prefix1/", "prefix2/"),
          DirectoryFindResults(Set("prefix1/", "prefix2/")),
          None,
        ),
      ),
    )
    callsMade >= 1
  }

}
