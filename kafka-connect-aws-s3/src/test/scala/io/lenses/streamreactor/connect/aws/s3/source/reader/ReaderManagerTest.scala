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

import cats.implicits.catsSyntaxEitherId
import cats.implicits.catsSyntaxOptionId
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.config.ConnectorTaskId
import io.lenses.streamreactor.connect.aws.s3.formats.reader.StringSourceData
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import io.lenses.streamreactor.connect.aws.s3.source.PollResults
import io.lenses.streamreactor.connect.aws.s3.source.files.SourceFileQueue
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class ReaderManagerTest extends AnyFlatSpec with MockitoSugar with Matchers with LazyLogging with BeforeAndAfter {

  private val connectorTaskId               = ConnectorTaskId("mySource", 1, 1)
  private val recordsLimit                  = 10
  private val bucketAndPrefix               = S3Location("test", "ing".some)
  private val firstFileBucketAndPath        = bucketAndPrefix.withPath("test:ing/topic/9/0.json")
  private val firstFileBucketAndPathAndLine = firstFileBucketAndPath.atLine(0).withTimestamp(Instant.now)

  "poll" should "be empty when no results found" in {
    val fileQueueProcessor: SourceFileQueue = mock[SourceFileQueue]

    var locationFnCalls = 0
    val target = new ReaderManager(
      recordsLimit,
      None,
      fileQueueProcessor,
      _ =>
        Left {
          locationFnCalls = locationFnCalls + 1
          new RuntimeException("ShouldNot be called")
        },
      connectorTaskId,
    )

    when(fileQueueProcessor.next()).thenReturn(None.asRight)

    target.poll() should be(empty)

    verify(fileQueueProcessor).next()
    locationFnCalls shouldBe 0
  }

  "poll" should "return single record when found" in {

    val partitionFn:        String => Option[Int] = _ => Option.empty
    val fileQueueProcessor: SourceFileQueue       = mock[SourceFileQueue]
    var calledLocation:     Option[S3Location]    = Option.empty

    when(fileQueueProcessor.next()).thenReturn(
      Some(firstFileBucketAndPathAndLine).asRight,
      None.asRight,
    )

    val pollResults = PollResults(
      resultList    = Vector(StringSourceData("abc", 0)),
      bucketAndPath = firstFileBucketAndPath,
      targetTopic   = "target",
      partitionFn   = partitionFn,
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

    when(fileQueueProcessor.markFileComplete(firstFileBucketAndPath)).thenReturn(().asRight)

    val target = new ReaderManager(
      recordsLimit,
      None,
      fileQueueProcessor,
      location => {
        calledLocation = location.some
        resultReader.asRight
      },
      connectorTaskId,
    )

    target.poll() should be(Seq(pollResults))

    verify(fileQueueProcessor, times(2)).next()
    calledLocation shouldBe firstFileBucketAndPathAndLine.some
  }

}
