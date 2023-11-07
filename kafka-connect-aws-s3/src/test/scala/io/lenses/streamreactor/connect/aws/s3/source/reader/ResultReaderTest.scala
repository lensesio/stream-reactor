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

import io.lenses.streamreactor.connect.cloud.common.formats.reader.CloudStreamReader
import io.lenses.streamreactor.connect.cloud.common.source.reader.ResultReader
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.source.SourceRecord
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.Collections

class ResultReaderTest extends AnyFlatSpec with MockitoSugar with Matchers {

  private val targetTopic = "MyTargetTopic"
  private val limit       = 10
  private val reader      = mock[CloudStreamReader]

  private val result1 =
    new SourceRecord(Collections.emptyMap(),
                     Collections.emptyMap(),
                     targetTopic,
                     0,
                     Schema.STRING_SCHEMA,
                     "myJsonStuff0",
    )
  private val result2 =
    new SourceRecord(Collections.emptyMap(),
                     Collections.emptyMap(),
                     targetTopic,
                     0,
                     Schema.STRING_SCHEMA,
                     "myJsonStuff1",
    )
  private val result3 =
    new SourceRecord(Collections.emptyMap(),
                     Collections.emptyMap(),
                     targetTopic,
                     0,
                     Schema.STRING_SCHEMA,
                     "myJsonStuff2",
    )

  val target = new ResultReader(reader)

  "resultReader" should "read a single results from the reader" in {

    when(reader.hasNext).thenReturn(true, false)
    when(reader.next()).thenReturn(result1)

    target.retrieveResults(limit) shouldBe Some(
      Vector(result1),
    )
  }

  "resultReader" should "read a multiple results from the reader" in {
    when(reader.hasNext).thenReturn(true, true, true, false)
    when(reader.next()).thenReturn(result1, result2, result3)

    target.retrieveResults(limit) shouldBe Some(
      Vector(
        result1,
        result2,
        result3,
      ),
    )
  }

  "resultReader" should "return none when no results exist" in {
    when(reader.hasNext).thenReturn(false)

    target.retrieveResults(limit) should be(None)
  }

  "resultReader" should "only read up to the limit" in {
    when(reader.hasNext).thenReturn(true, true, true, false)
    when(reader.next()).thenReturn(result1, result2, result3)

    target.retrieveResults(2) shouldBe Some(
      Vector(result1, result2),
    )

    target.retrieveResults(2) shouldBe Some(
      Vector(result3),
    )
  }
}
