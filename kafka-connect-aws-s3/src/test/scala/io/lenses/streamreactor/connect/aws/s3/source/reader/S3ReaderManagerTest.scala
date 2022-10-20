/*
 * Copyright 2020 Lenses.io
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

import cats.implicits.catsSyntaxEitherId
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.model._
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3RootLocation
import io.lenses.streamreactor.connect.aws.s3.source.files.S3SourceLister
import io.lenses.streamreactor.connect.aws.s3.source.files.SourceFileQueue
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class S3ReaderManagerTest extends AnyFlatSpec with MockitoSugar with Matchers with LazyLogging with BeforeAndAfter {

  private implicit val storageInterface: StorageInterface = mock[StorageInterface]
  private implicit val sourceLister:     S3SourceLister   = mock[S3SourceLister]
  private val fileQueueProcessor:        SourceFileQueue  = mock[SourceFileQueue]
  private val readerCreator = mock[ReaderCreator]

  private val sourceName                    = "mySource"
  private val recordsLimit                  = 10
  private val bucketAndPrefix               = RemoteS3RootLocation("test:ing")
  private val firstFileBucketAndPath        = bucketAndPrefix.withPath("test:ing/topic/9/0.json")
  private val firstFileBucketAndPathAndLine = firstFileBucketAndPath.atLine(0)

  "poll" should "be empty when no results found" in {

    val target = new S3ReaderManager(
      sourceName,
      recordsLimit,
      None,
      fileQueueProcessor,
      readerCreator.create,
    )

    when(fileQueueProcessor.next()).thenReturn(None.asRight)

    target.poll() should be(empty)

    verify(fileQueueProcessor).next()
    verifyZeroInteractions(readerCreator)
  }

  "poll" should "return single record when found" in {

    val target = new S3ReaderManager(
      sourceName,
      recordsLimit,
      None,
      fileQueueProcessor,
      readerCreator.create,
    )

    when(fileQueueProcessor.next()).thenReturn(
      Some(firstFileBucketAndPathAndLine).asRight,
      None.asRight,
    )

    val pollResults: PollResults = mockResultReader

    when(fileQueueProcessor.markFileComplete(firstFileBucketAndPath)).thenReturn(().asRight)

    target.poll() should be(Seq(pollResults))

    verify(fileQueueProcessor, times(2)).next()
    verify(readerCreator).create(firstFileBucketAndPathAndLine)
  }

  private def mockResultReader = {
    val pollResults = PollResults(
      resultList    = Vector(StringSourceData("abc", 0)),
      bucketAndPath = firstFileBucketAndPath,
      targetTopic   = "target",
    )

    val resultReader = mock[ResultReader]

    when(
      resultReader.retrieveResults(10),
    ).thenReturn(Some(pollResults))

    when(
      resultReader.retrieveResults(9),
    ).thenReturn(Option.empty[PollResults])

    when(
      resultReader.getLocation,
    ).thenReturn(firstFileBucketAndPath)

    when(
      readerCreator.create(firstFileBucketAndPathAndLine),
    ).thenReturn(resultReader.asRight[Throwable])

    pollResults
  }

  before {
    reset(storageInterface, sourceLister, fileQueueProcessor, readerCreator)
  }
}
