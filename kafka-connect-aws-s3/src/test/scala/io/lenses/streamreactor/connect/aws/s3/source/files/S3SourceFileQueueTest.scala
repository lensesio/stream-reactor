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
package io.lenses.streamreactor.connect.aws.s3.source.files

import cats.implicits.catsSyntaxEitherId
import cats.implicits.catsSyntaxOptionId
import cats.implicits.none
import io.lenses.streamreactor.connect.aws.s3.config.ConnectorTaskId
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import io.lenses.streamreactor.connect.aws.s3.storage.FileListError
import io.lenses.streamreactor.connect.aws.s3.storage.FileLoadError
import io.lenses.streamreactor.connect.aws.s3.storage.FileMetadata
import io.lenses.streamreactor.connect.aws.s3.storage.ListResponse
import org.mockito.ArgumentMatchers._
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class S3SourceFileQueueTest extends AnyFlatSpec with Matchers with MockitoSugar with BeforeAndAfter {

  private val taskId = ConnectorTaskId("topic", 1, 0)
  private val bucket = "bucket"
  private val prefix = "prefix"
  private val files: Seq[String] = (0 to 3).map(file => file.toString + ".json")
  private val fileLocs: Seq[S3Location] = files.map(f =>
    S3Location(
      bucket = bucket,
      prefix = prefix.some,
      path   = f.some,
    ),
  )
  private val lastModified = Instant.now

  private def listBatch(lastFileMeta: Option[FileMetadata]): Either[FileListError, Option[ListResponse[String]]] = {
    val num = lastFileMeta match {
      case Some(lastFile) => lastFile.file.stripSuffix(".json").toInt
      case None           => -1
    }
    extract(num).asRight
  }

  private def extract(num: Int): Option[ListResponse[String]] = {
    val filesToRet = files.zipWithIndex.toMap.collect {
      case (file, i) if i == num + 1 || i == num + 2 => file
    }.toSeq
    Option.when(filesToRet.nonEmpty)(ListResponse(bucket,
                                                  prefix.some,
                                                  filesToRet,
                                                  FileMetadata(filesToRet.last, lastModified),
    ))
  }
  "list" should "cache a batch of results from the beginning" in {

    val batchListerFn = mock[Option[FileMetadata] => Either[FileListError, Option[ListResponse[String]]]]

    val sourceFileQueue = new S3SourceFileQueue(taskId, batchListerFn)

    val order = inOrder(batchListerFn)

    doAnswer(x => listBatch(x)).when(batchListerFn)(
      any[Option[FileMetadata]],
    )
    // file 0 = 0.json
    sourceFileQueue.next() should be(Right(Some(fileLocs(0).atLine(-1).withTimestamp(lastModified))))
    order.verify(batchListerFn)(none)

    // file 1 = 1.json
    sourceFileQueue.next() should be(Right(Some(fileLocs(1).atLine(-1).withTimestamp(lastModified))))
    order.verifyNoMoreInteractions()

    // file 2 = 2.json
    sourceFileQueue.next() should be(Right(Some(fileLocs(2).atLine(-1).withTimestamp(lastModified))))
    order.verify(batchListerFn)(FileMetadata(files(1), lastModified).some)

    // file 3 = 3.json
    sourceFileQueue.next() should be(Right(Some(fileLocs(3).atLine(-1).withTimestamp(lastModified))))
    order.verifyNoMoreInteractions()

    // No more files
    sourceFileQueue.next() should be(Right(None))
    order.verify(batchListerFn)(FileMetadata(files(3), lastModified).some)

    // Try again, but still no more files
    sourceFileQueue.next() should be(Right(None))
    order.verify(batchListerFn)(FileMetadata(files(3), lastModified).some)

  }

  "list" should "process the init file before reading additional files" in {

    val batchListerFn = mock[Option[FileMetadata] => Either[FileListError, Option[ListResponse[String]]]]
    val blobModifiedFn: (String, String) => Either[FileLoadError, Instant] = (_, _) => Instant.now().asRight

    val sourceFileQueue =
      S3SourceFileQueue.from(batchListerFn,
                             blobModifiedFn,
                             fileLocs(2).atLine(1000).withTimestamp(lastModified),
                             taskId,
      )

    val order = inOrder(batchListerFn)

    doAnswer(x => listBatch(x)).when(batchListerFn)(
      any[Option[FileMetadata]],
    )
    // file 2 = 2.json
    sourceFileQueue.next() should be(Right(Some(fileLocs(2).atLine(1000).withTimestamp(lastModified))))

    // file 3 = 3.json
    sourceFileQueue.next() should be(Right(Some(fileLocs(3).atLine(-1).withTimestamp(lastModified))))
    order.verify(batchListerFn)(FileMetadata(files(2), lastModified).some)

    // No more files
    sourceFileQueue.next() should be(Right(None))
    order.verify(batchListerFn)(FileMetadata(files(3), lastModified).some)
  }

  "S3SourceFileQueue" should "return none if there are no more files in the queue" in {

    val batchListerFn = mock[Option[FileMetadata] => Either[FileListError, Option[ListResponse[String]]]]

    def listBatch(lastFileMeta: Option[FileMetadata]): Either[FileListError, Option[ListResponse[String]]] = Right(None)
    doAnswer(x => listBatch(x)).when(batchListerFn)(
      any[Option[FileMetadata]],
    )
    val sourceFileQueue = new S3SourceFileQueue(taskId, batchListerFn)
    sourceFileQueue.next() shouldBe Right(None)
  }
  "S3SourceFileQueue" should "return the error on batch listing" in {

    val batchListerFn = mock[Option[FileMetadata] => Either[FileListError, Option[ListResponse[String]]]]

    val expected = Left(FileListError(null, bucket, s"$bucket/$prefix".some))
    def listBatch(lastFileMeta: Option[FileMetadata]): Either[FileListError, Option[ListResponse[String]]] = expected

    doAnswer(x => listBatch(x)).when(batchListerFn)(
      any[Option[FileMetadata]],
    )
    val sourceFileQueue = new S3SourceFileQueue(taskId, batchListerFn)
    sourceFileQueue.next() shouldBe expected
  }

}
