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
import cats.implicits.catsSyntaxOptionId
import io.lenses.streamreactor.connect.aws.s3.config.ConnectorTaskId
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import io.lenses.streamreactor.connect.aws.s3.source.config.PartitionSearcherOptions
import io.lenses.streamreactor.connect.aws.s3.source.distribution.PartitionSearcher
import io.lenses.streamreactor.connect.aws.s3.source.distribution.PartitionSearcherResponse
import io.lenses.streamreactor.connect.aws.s3.source.files.SourceFileQueue
import io.lenses.streamreactor.connect.aws.s3.storage.DirectoryFindResults
import io.lenses.streamreactor.connect.aws.s3.storage.MockS3Client
import io.lenses.streamreactor.connect.aws.s3.storage.S3Page
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.IteratorHasAsScala

class PartitionDiscoveryTest extends AnyFlatSpecLike with Matchers with MockitoSugar {
  private val connectorTaskId: ConnectorTaskId = ConnectorTaskId("sinkName", 1, 1)
  "PartitionDiscovery" should "handle failure on PartitionSearcher and resume" in {
    val fileQueueProcessor: SourceFileQueue = mock[SourceFileQueue]
    val limit   = 10
    val options = PartitionSearcherOptions(1, true, 100.millis)

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
            PartitionSearcherResponse(S3Location("bucket", None),
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
          IO(new ReaderManager(limit, fileQueueProcessor, _ => Left(new RuntimeException()), connectorTaskId, readerRef)),
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
          S3Location("bucket", None),
          Set("prefix1/", "prefix2/"),
          DirectoryFindResults(Set("prefix1/", "prefix2/")),
          None,
        ),
      ),
    )
    callsMade >= 1
  }
  "PartitionDiscovery" should "discover all partitions" in {
    val fileQueueProcessor: SourceFileQueue = mock[SourceFileQueue]
    val limit = 10
    val s3Client = new MockS3Client(
      S3Page(
        "prefix1/1.txt",
        "prefix1/2.txt",
        "prefix2/3.txt",
        "prefix2/4.txt",
      ),
    )
    val options = PartitionSearcherOptions(1, true, 100.millis)
    val io = for {
      cancelledRef <- Ref[IO].of(false)
      readerRef    <- Ref[IO].of(Option.empty[ResultReader])
      state        <- Ref[IO].of(ReaderManagerState(Seq.empty, Seq.empty))
      fiber <- PartitionDiscovery.run(
        connectorTaskId,
        options,
        new PartitionSearcher(List(
                                S3Location("bucket", None),
                              ),
                              options,
                              connectorTaskId,
                              s3Client.listObjectsV2Paginator(_).iterator().asScala,
        ).find,
        (_, _) =>
          IO(new ReaderManager(limit, fileQueueProcessor, _ => Left(new RuntimeException()), connectorTaskId, readerRef)),
        state,
        cancelledRef,
      ).start
      _              <- IO.sleep(1000.millis)
      _              <- cancelledRef.set(true)
      _              <- fiber.join
      readerMgrState <- state.get
    } yield readerMgrState

    val state = io.unsafeRunSync()
    assert(
      state.partitionResponses == List(
        PartitionSearcherResponse(
          S3Location("bucket", None),
          Set("prefix1/", "prefix2/"),
          DirectoryFindResults(Set.empty),
          None,
        ),
      ),
    )
  }

  "PartitionDiscovery" should "discover new partitions" in {
    val fileQueueProcessor: SourceFileQueue = mock[SourceFileQueue]
    val limit = 10
    val s3Client = new MockS3Client(
      S3Page(
        "prefix1/1.txt",
        "prefix1/2.txt",
        "prefix2/3.txt",
        "prefix2/4.txt",
      ),
    )
    val options = PartitionSearcherOptions(1, true, 100.millis)
    val io = for {
      cancelledRef <- Ref[IO].of(false)
      readerRef    <- Ref[IO].of(Option.empty[ResultReader])
      state <- Ref[IO].of(
        ReaderManagerState(
          List(PartitionSearcherResponse(
            S3Location("bucket", None),
            Set("prefix1/"),
            DirectoryFindResults(Set("prefix1/")),
            None,
          )),
          Seq.empty,
        ),
      )
      fiber <- PartitionDiscovery.run(
        connectorTaskId,
        options,
        new PartitionSearcher(List(
                                S3Location("bucket", None),
                              ),
                              options,
                              connectorTaskId,
                              s3Client.listObjectsV2Paginator(_).iterator().asScala,
        ).find,
        (_, _) =>
          IO(new ReaderManager(limit, fileQueueProcessor, _ => Left(new RuntimeException()), connectorTaskId, readerRef)),
        state,
        cancelledRef,
      ).start
      _              <- IO.sleep(1000.millis)
      _              <- cancelledRef.set(true)
      _              <- fiber.join
      readerMgrState <- state.get
    } yield readerMgrState

    val state = io.unsafeRunSync()
    assert(
      state.partitionResponses == List(
        PartitionSearcherResponse(
          S3Location("bucket", None),
          Set("prefix1/", "prefix2/"),
          DirectoryFindResults(Set.empty),
          None,
        ),
      ),
    )
  }

  "PartitionDiscovery" should "discover all partitions when prefix is used" in {
    val fileQueueProcessor: SourceFileQueue = mock[SourceFileQueue]
    val limit = 10
    val s3Client = new MockS3Client(
      S3Page(
        "prefix1/one/1.txt",
        "prefix1/two/2.txt",
        "prefix2/3.txt",
        "prefix2/4.txt",
        "prefix3/5.txt",
        "prefix1/three/2.txt",
      ),
    )
    val options = PartitionSearcherOptions(1, true, 100.millis)
    val io = for {
      cancelledRef <- Ref[IO].of(false)
      readerRef    <- Ref[IO].of(Option.empty[ResultReader])
      state        <- Ref[IO].of(ReaderManagerState(Seq.empty, Seq.empty))
      fiber <- PartitionDiscovery.run(
        connectorTaskId,
        options,
        new PartitionSearcher(List(
                                S3Location("bucket", "prefix1/".some),
                              ),
                              options,
                              connectorTaskId,
                              s3Client.listObjectsV2Paginator(_).iterator().asScala,
        ).find,
        (_, _) =>
          IO(new ReaderManager(limit, fileQueueProcessor, _ => Left(new RuntimeException()), connectorTaskId, readerRef)),
        state,
        cancelledRef,
      ).start
      _              <- IO.sleep(1000.millis)
      _              <- cancelledRef.set(true)
      _              <- fiber.join
      readerMgrState <- state.get
    } yield readerMgrState

    val state = io.unsafeRunSync()
    assert(
      state.partitionResponses == List(
        PartitionSearcherResponse(
          S3Location("bucket", "prefix1/".some),
          Set("prefix1/one/", "prefix1/two/", "prefix1/three/"),
          DirectoryFindResults(Set.empty),
          None,
        ),
      ),
    )
  }

  "PartitionDiscovery" should "discover all partitions when prefix is used and apply the distribution across tasks" in {
    val fileQueueProcessor: SourceFileQueue = mock[SourceFileQueue]
    val limit = 10

    val s3Client = new MockS3Client(
      S3Page(
        "prefix1/subprefix_abc/1.txt",
        "prefix1/subprefix_xyz01/2.txt",
        "prefix1/subprefix_untitled/3.txt",
      ),
    )
    val options = PartitionSearcherOptions(1, true, 100.millis)
    List(0 -> "prefix1/subprefix_abc/", 1 -> "prefix1/subprefix_untitled/", 2 -> "prefix1/subprefix_xyz01/").foreach {
      case (i, partition) =>
        val taskId = ConnectorTaskId("sinkName", 3, i)
        val io = for {
          cancelledRef <- Ref[IO].of(false)
          readerRef    <- Ref[IO].of(Option.empty[ResultReader])
          state        <- Ref[IO].of(ReaderManagerState(Seq.empty, Seq.empty))
          fiber <- PartitionDiscovery.run(
            taskId,
            options,
            new PartitionSearcher(List(
                                    S3Location("bucket", "prefix1/".some),
                                  ),
                                  options,
                                  taskId,
                                  s3Client.listObjectsV2Paginator(_).iterator().asScala,
            ).find,
            (
              _,
              _,
            ) => IO(new ReaderManager(limit, fileQueueProcessor, _ => Left(new RuntimeException()), taskId, readerRef)),
            state,
            cancelledRef,
          ).start
          _              <- IO.sleep(1000.millis)
          _              <- cancelledRef.set(true)
          _              <- fiber.join
          readerMgrState <- state.get
        } yield readerMgrState

        val state = io.unsafeRunSync()
        assert(
          state.partitionResponses == List(
            PartitionSearcherResponse(
              S3Location("bucket", "prefix1/".some),
              Set(partition),
              DirectoryFindResults(Set.empty),
              None,
            ),
          ),
        )
    }
  }
}
