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

package io.lenses.streamreactor.connect.aws.s3.formats

import java.io.ByteArrayInputStream

import io.lenses.streamreactor.connect.aws.s3.config.BytesWriteMode
import io.lenses.streamreactor.connect.aws.s3.model.{BucketAndPath, BytesOutputRow, BytesOutputRowTest}
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BytesFormatChunkedStreamReaderTest extends AnyFlatSpec with MockitoSugar with Matchers {

  import BytesOutputRowTest._

  val bucketAndPath: BucketAndPath = mock[BucketAndPath]

  "read" should "read chunks at a time" in {

    val fileContents = List.fill(2)("lemonOlive").mkString("").getBytes
    val inputStreamFn = () => new ByteArrayInputStream(fileContents)
    val sizeFn = () => fileContents.length.longValue()
    val target = new BytesFormatChunkedStreamReader(inputStreamFn, sizeFn, bucketAndPath, BytesWriteMode.ValueOnly, 5)

    checkRecord(target, BytesOutputRow(None, None, Array.empty[Byte], "lemon".getBytes))
    checkRecord(target, BytesOutputRow(None, None, Array.empty[Byte], "Olive".getBytes))
    checkRecord(target, BytesOutputRow(None, None, Array.empty[Byte], "lemon".getBytes))
    checkRecord(target, BytesOutputRow(None, None, Array.empty[Byte], "Olive".getBytes))

    target.hasNext should be(false)
  }

  private def checkRecord(target: BytesFormatChunkedStreamReader, expectedOutputRow: BytesOutputRow) = {
    target.hasNext should be(true)
    val record = target.next()
    checkEqualsByteArrayValue(record.data, expectedOutputRow)
  }

}
