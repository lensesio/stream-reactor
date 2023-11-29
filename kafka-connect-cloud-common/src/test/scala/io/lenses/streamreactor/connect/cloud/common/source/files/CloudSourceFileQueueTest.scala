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
package io.lenses.streamreactor.connect.cloud.common.source.files

import cats.implicits.catsSyntaxEitherId
import cats.implicits.catsSyntaxOptionId
import cats.implicits.none
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.seek.TestFileMetadata
import io.lenses.streamreactor.connect.cloud.common.storage.FileListError
import io.lenses.streamreactor.connect.cloud.common.storage.ListOfKeysResponse
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData
import org.mockito.ArgumentMatchers._
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class CloudSourceFileQueueTest extends AnyFlatSpec with Matchers with MockitoSugar with BeforeAndAfter {
  private implicit val cloudLocationValidator: CloudLocationValidator = SampleData.cloudLocationValidator

  private val taskId = ConnectorTaskId("topic", 1, 0)
  private val bucket = "bucket"
  private val prefix = "prefix"
  private val files: Seq[String] = (0 to 3).map(file => file.toString + ".json")
  private val fileLocs: Seq[CloudLocation] = files.map(f =>
    CloudLocation(
      bucket = bucket,
      prefix = prefix.some,
      path   = f.some,
    ),
  )
  private val lastModified = Instant.now

  private def listBatch(
    lastFileMeta: Option[TestFileMetadata],
  ): Either[FileListError, Option[ListOfKeysResponse[TestFileMetadata]]] = {
    val num = lastFileMeta match {
      case Some(lastFile) => lastFile.file.stripSuffix(".json").toInt
      case None           => -1
    }
    extract(num).asRight
  }

  private def extract(num: Int): Option[ListOfKeysResponse[TestFileMetadata]] = {
    val filesToRet = files.zipWithIndex.toMap.collect {
      case (file, i) if i == num + 1 || i == num + 2 => file
    }.toSeq
    Option.when(filesToRet.nonEmpty)(ListOfKeysResponse(bucket,
                                                        prefix.some,
                                                        filesToRet,
                                                        TestFileMetadata(filesToRet.last, lastModified),
    ))
  }
  "list" should "cache a batch of results from the beginning" in {

    val batchListerFn =
      mock[Option[TestFileMetadata] => Either[FileListError, Option[ListOfKeysResponse[TestFileMetadata]]]]

    val sourceFileQueue = new CloudSourceFileQueue(taskId, batchListerFn)

    val order = inOrder(batchListerFn)

    doAnswer(x => listBatch(x)).when(batchListerFn)(
      any[Option[TestFileMetadata]],
    )
    // file 0 = 0.json
    sourceFileQueue.next() should be(Right(Some(fileLocs(0).atLine(-1).withTimestamp(lastModified))))
    order.verify(batchListerFn)(none)

    // file 1 = 1.json
    sourceFileQueue.next() should be(Right(Some(fileLocs(1).atLine(-1).withTimestamp(lastModified))))
    order.verifyNoMoreInteractions()

    // file 2 = 2.json
    sourceFileQueue.next() should be(Right(Some(fileLocs(2).atLine(-1).withTimestamp(lastModified))))
    order.verify(batchListerFn)(TestFileMetadata(files(1), lastModified).some)

    // file 3 = 3.json
    sourceFileQueue.next() should be(Right(Some(fileLocs(3).atLine(-1).withTimestamp(lastModified))))
    order.verifyNoMoreInteractions()

    // No more files
    sourceFileQueue.next() should be(Right(None))
    order.verify(batchListerFn)(TestFileMetadata(files(3), lastModified).some)

    // Try again, but still no more files
    sourceFileQueue.next() should be(Right(None))
    order.verify(batchListerFn)(TestFileMetadata(files(3), lastModified).some)

  }

  "list" should "process the init file before reading additional files" in {

    val batchListerFn =
      mock[Option[TestFileMetadata] => Either[FileListError, Option[ListOfKeysResponse[TestFileMetadata]]]]

    val mockStorageIface = mock[StorageInterface[TestFileMetadata]]
    when(
      mockStorageIface.seekToFile(
        anyString(),
        anyString(),
        any[Option[Instant]],
      ),
    ).thenAnswer((_: String, file: String, _: Option[Instant]) => TestFileMetadata(file, lastModified).some)

    val sourceFileQueue =
      CloudSourceFileQueue.from(batchListerFn,
                                mockStorageIface,
                                fileLocs(2).atLine(1000).withTimestamp(lastModified),
                                taskId,
      )

    val order = inOrder(batchListerFn)

    doAnswer(x => listBatch(x)).when(batchListerFn)(
      any[Option[TestFileMetadata]],
    )
    // file 2 = 2.json
    sourceFileQueue.next() should be(Right(Some(fileLocs(2).atLine(1000).withTimestamp(lastModified))))

    // file 3 = 3.json
    sourceFileQueue.next() should be(Right(Some(fileLocs(3).atLine(-1).withTimestamp(lastModified))))
    order.verify(batchListerFn)(TestFileMetadata(files(2), lastModified).some)

    // No more files
    sourceFileQueue.next() should be(Right(None))
    order.verify(batchListerFn)(TestFileMetadata(files(3), lastModified).some)
  }

  "S3SourceFileQueue" should "return none if there are no more files in the queue" in {

    val batchListerFn =
      mock[Option[TestFileMetadata] => Either[FileListError, Option[ListOfKeysResponse[TestFileMetadata]]]]

    def listBatch(
    ): Either[FileListError, Option[ListOfKeysResponse[TestFileMetadata]]] = Right(None)
    doAnswer(listBatch()).when(batchListerFn)(
      any[Option[TestFileMetadata]],
    )
    val sourceFileQueue = new CloudSourceFileQueue(taskId, batchListerFn)
    sourceFileQueue.next() shouldBe Right(None)
  }
  "S3SourceFileQueue" should "return the error on batch listing" in {

    val batchListerFn =
      mock[Option[TestFileMetadata] => Either[FileListError, Option[ListOfKeysResponse[TestFileMetadata]]]]

    val expected = Left(FileListError(null, bucket, s"$bucket/$prefix".some))
    def listBatch(
    ): Either[FileListError, Option[ListOfKeysResponse[TestFileMetadata]]] = expected

    doAnswer(listBatch()).when(batchListerFn)(
      any[Option[TestFileMetadata]],
    )
    val sourceFileQueue = new CloudSourceFileQueue(taskId, batchListerFn)
    sourceFileQueue.next() shouldBe expected
  }

}
