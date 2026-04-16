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
package io.lenses.streamreactor.connect.cloud.common.source

import cats.effect.IO
import cats.effect.Ref
import cats.effect.unsafe.implicits.global
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.source.config.CloudSourceBucketOptions
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import io.lenses.streamreactor.connect.cloud.common.storage.FileTouchError
import io.lenses.streamreactor.connect.cloud.common.storage.ListOfMetadataResponse
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyString
import org.mockito.MockitoSugar
import org.mockito.invocation.InvocationOnMock
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._

class LateArrivalTouchTaskTest extends AnyFlatSpec with Matchers with MockitoSugar {

  implicit val validator: CloudLocationValidator = SampleData.cloudLocationValidator

  private val connectorTaskId = ConnectorTaskId("test-connector", 1, 0)

  case class TestFileMetadata(file: String, lastModified: Instant) extends FileMetadata

  "LateArrivalTouchTask.run" should "return immediately if no bucket options have late arrival processing enabled" in {
    val storageInterface = mock[StorageInterface[TestFileMetadata]]
    val bucketOptions    = Seq.empty[CloudSourceBucketOptions[TestFileMetadata]]
    val contextOffsetFn  = (_: CloudLocation) => Option.empty[CloudLocation]
    val cancelledRef     = Ref.unsafe[IO, Boolean](false)

    // Should complete immediately without doing anything
    val result = LateArrivalTouchTask.run(
      connectorTaskId,
      storageInterface,
      bucketOptions,
      1.second,
      contextOffsetFn,
      cancelledRef,
    )

    // The task should complete without error when there are no late arrival configs
    result.unsafeRunSync()

    // Verify no interactions with storage interface
    verifyZeroInteractions(storageInterface)
  }

  it should "touch files older than the watermark timestamp" in {
    val storageInterface = mock[StorageInterface[TestFileMetadata]]
    // Start with cancelledRef = false so the task runs
    val cancelledRef = Ref.unsafe[IO, Boolean](false)

    val sourceLocation     = CloudLocation("test-bucket", Some("prefix"))
    val watermarkTimestamp = Instant.now()

    // Create test file metadata - one older than watermark, one newer
    val olderFile = TestFileMetadata("prefix/old-file.txt", watermarkTimestamp.minusSeconds(3600))
    val newerFile = TestFileMetadata("prefix/new-file.txt", watermarkTimestamp.plusSeconds(3600))

    // Mock the watermark read
    val watermarkLocation = sourceLocation.copy(
      path      = Some("prefix/watermark-file.txt"),
      timestamp = Some(watermarkTimestamp),
    )
    val contextOffsetFn = (loc: CloudLocation) =>
      if (loc.bucket == sourceLocation.bucket && loc.prefix == sourceLocation.prefix) {
        Some(watermarkLocation)
      } else {
        None
      }

    val files = Seq(olderFile, newerFile)
    when(storageInterface.listFileMetaRecursive(anyString(), any[Option[String]]()))
      .thenReturn(Right(Some(ListOfMetadataResponse("test-bucket", Some("prefix"), files, newerFile))))

    when(storageInterface.touchFile(anyString(), anyString()))
      .thenAnswer { (_: InvocationOnMock) =>
        cancelledRef.set(true).unsafeRunSync()
        Right(())
      }

    val bucketOptions = Seq(
      createMockBucketOptions(sourceLocation, processLateArrival = true),
    )

    val result = LateArrivalTouchTask.run(
      connectorTaskId,
      storageInterface,
      bucketOptions,
      1.second, // 1 second interval for testing
      contextOffsetFn,
      cancelledRef,
    )

    result.timeout(5.seconds).unsafeRunSync()

    verify(storageInterface).touchFile("test-bucket", "prefix/old-file.txt")
    verify(storageInterface, never).touchFile("test-bucket", "prefix/new-file.txt")
  }

  it should "not touch any files when no watermark is found" in {
    val storageInterface = mock[StorageInterface[TestFileMetadata]]
    val cancelledRef     = Ref.unsafe[IO, Boolean](false)

    val sourceLocation = CloudLocation("test-bucket", Some("prefix"))

    val callCount = new AtomicInteger(0)
    val contextOffsetFn = (_: CloudLocation) => {
      if (callCount.incrementAndGet() >= 1) {
        cancelledRef.set(true).unsafeRunSync()
      }
      Option.empty[CloudLocation]
    }

    val bucketOptions = Seq(
      createMockBucketOptions(sourceLocation, processLateArrival = true),
    )

    val result = LateArrivalTouchTask.run(
      connectorTaskId,
      storageInterface,
      bucketOptions,
      1.second,
      contextOffsetFn,
      cancelledRef,
    )

    result.timeout(5.seconds).unsafeRunSync()
    verify(storageInterface, never).touchFile(anyString(), anyString())
  }

  it should "continue even if touchFile fails for some files" in {
    val storageInterface = mock[StorageInterface[TestFileMetadata]]
    val cancelledRef     = Ref.unsafe[IO, Boolean](false)

    val sourceLocation     = CloudLocation("test-bucket", Some("prefix"))
    val watermarkTimestamp = Instant.now()

    val oldFile1 = TestFileMetadata("prefix/old-file-1.txt", watermarkTimestamp.minusSeconds(3600))
    val oldFile2 = TestFileMetadata("prefix/old-file-2.txt", watermarkTimestamp.minusSeconds(7200))

    val watermarkLocation = sourceLocation.copy(
      path      = Some("prefix/watermark-file.txt"),
      timestamp = Some(watermarkTimestamp),
    )
    val contextOffsetFn = (_: CloudLocation) => Some(watermarkLocation)

    val files = Seq(oldFile1, oldFile2)
    when(storageInterface.listFileMetaRecursive(anyString(), any[Option[String]]()))
      .thenReturn(Right(Some(ListOfMetadataResponse("test-bucket", Some("prefix"), files, oldFile2))))

    val touchCount = new AtomicInteger(0)
    when(storageInterface.touchFile("test-bucket", "prefix/old-file-1.txt"))
      .thenAnswer { (_: InvocationOnMock) =>
        touchCount.incrementAndGet()
        Left(FileTouchError(new Exception("Touch failed"), "prefix/old-file-1.txt"))
      }
    when(storageInterface.touchFile("test-bucket", "prefix/old-file-2.txt"))
      .thenAnswer { (_: InvocationOnMock) =>
        if (touchCount.incrementAndGet() >= 2) {
          cancelledRef.set(true).unsafeRunSync()
        }
        Right(())
      }

    val bucketOptions = Seq(
      createMockBucketOptions(sourceLocation, processLateArrival = true),
    )

    val result = LateArrivalTouchTask.run(
      connectorTaskId,
      storageInterface,
      bucketOptions,
      1.second,
      contextOffsetFn,
      cancelledRef,
    )

    // Should complete without throwing despite the first touchFile failing
    noException should be thrownBy result.timeout(5.seconds).unsafeRunSync()

    // Both files should have been attempted
    verify(storageInterface).touchFile("test-bucket", "prefix/old-file-1.txt")
    verify(storageInterface).touchFile("test-bucket", "prefix/old-file-2.txt")
  }

  it should "stop promptly when cancelledRef is set during file processing" in {
    val storageInterface = mock[StorageInterface[TestFileMetadata]]
    val cancelledRef     = Ref.unsafe[IO, Boolean](false)

    val sourceLocation     = CloudLocation("test-bucket", Some("prefix"))
    val watermarkTimestamp = Instant.now()

    // Create multiple old files
    val files = (1 to 10).map { i =>
      TestFileMetadata(s"prefix/old-file-$i.txt", watermarkTimestamp.minusSeconds(i.toLong * 1000L))
    }

    val watermarkLocation = sourceLocation.copy(
      path      = Some("prefix/watermark-file.txt"),
      timestamp = Some(watermarkTimestamp),
    )
    val contextOffsetFn = (_: CloudLocation) => Some(watermarkLocation)

    when(storageInterface.listFileMetaRecursive(anyString(), any[Option[String]]()))
      .thenReturn(Right(Some(ListOfMetadataResponse("test-bucket", Some("prefix"), files, files.last))))

    // Cancel after first file is touched
    val touchCount = new AtomicInteger(0)
    when(storageInterface.touchFile(anyString(), anyString()))
      .thenAnswer { (_: InvocationOnMock) =>
        if (touchCount.incrementAndGet() >= 1) {
          cancelledRef.set(true).unsafeRunSync()
        }
        Right(())
      }

    val bucketOptions = Seq(
      createMockBucketOptions(sourceLocation, processLateArrival = true),
    )

    val result = LateArrivalTouchTask.run(
      connectorTaskId,
      storageInterface,
      bucketOptions,
      1.second,
      contextOffsetFn,
      cancelledRef,
    )

    result.timeout(5.seconds).unsafeRunSync()

    // Should have touched only the first file (or maybe 2 due to race)
    // The key is that it didn't touch all 10
    touchCount.get() should be < 5
  }

  private def createMockBucketOptions(
    sourceLocation:     CloudLocation,
    processLateArrival: Boolean,
  ): CloudSourceBucketOptions[TestFileMetadata] =
    CloudSourceBucketOptions[TestFileMetadata](
      sourceBucketAndPrefix = sourceLocation,
      targetTopic           = "test-topic",
      format                = mock[io.lenses.streamreactor.connect.cloud.common.config.FormatSelection],
      recordsLimit          = 1000,
      filesLimit            = 100,
      partitionExtractor    = None,
      orderingType          = mock[io.lenses.streamreactor.connect.cloud.common.source.config.OrderingType],
      hasEnvelope           = false,
      postProcessAction     = None,
      processLateArrival    = processLateArrival,
    )
}
