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

import io.lenses.streamreactor.connect.aws.s3.formats.S3FormatStreamReader
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3PathLocation
import io.lenses.streamreactor.connect.aws.s3.model.PollResults
import io.lenses.streamreactor.connect.aws.s3.model.SourceData
import io.lenses.streamreactor.connect.aws.s3.model.StringSourceData
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ResultReaderTest extends AnyFlatSpec with MockitoSugar with Matchers {

  private val readerBucketAndPath = RemoteS3PathLocation("bucket-and-path", Some("MyPrefix"), "prefix")
  private val targetTopic         = "MyTargetTopic"
  private val limit               = 10
  private val reader              = mock[S3FormatStreamReader[_ <: SourceData]]
  private implicit val partitionFn: String => Option[Int] = _ => Option.empty

  private val result1 = StringSourceData("myJsonStuff0", 0)
  private val result2 = StringSourceData("myJsonStuff1", 1)
  private val result3 = StringSourceData("myJsonStuff2", 2)

  val target = new ResultReader(reader, targetTopic)

  "resultReader" should "read a single results from the reader" in {
    when(reader.getBucketAndPath).thenReturn(readerBucketAndPath)
    when(reader.hasNext).thenReturn(true, false)
    when(reader.next()).thenReturn(result1)

    target.retrieveResults(limit) should be(Some(
      PollResults(
        Vector(result1),
        readerBucketAndPath,
        targetTopic,
      ),
    ))
  }

  "resultReader" should "read a multiple results from the reader" in {
    when(reader.getBucketAndPath).thenReturn(readerBucketAndPath)
    when(reader.hasNext).thenReturn(true, true, true, false)
    when(reader.next()).thenReturn(result1, result2, result3)

    target.retrieveResults(limit) should be(Some(
      PollResults(
        Vector(
          result1,
          result2,
          result3,
        ),
        readerBucketAndPath,
        targetTopic,
      ),
    ))
  }

  "resultReader" should "return none when no results exist" in {
    when(reader.getBucketAndPath).thenReturn(readerBucketAndPath)
    when(reader.hasNext).thenReturn(false)

    target.retrieveResults(limit) should be(None)
  }

  "resultReader" should "only read up to the limit" in {

    when(reader.getBucketAndPath).thenReturn(readerBucketAndPath)
    when(reader.hasNext).thenReturn(true, true, true, false)
    when(reader.next()).thenReturn(result1, result2, result3)

    target.retrieveResults(2) should be(Some(
      PollResults(
        Vector(result1, result2),
        readerBucketAndPath,
        targetTopic,
      ),
    ))

    target.retrieveResults(2) should be(Some(
      PollResults(
        Vector(result3),
        readerBucketAndPath,
        targetTopic,
      ),
    ))
  }
}
