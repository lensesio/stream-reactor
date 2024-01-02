/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.formats

import io.lenses.streamreactor.connect.cloud.common.formats.bytes.BytesOutputRow
import io.lenses.streamreactor.connect.cloud.common.formats.reader.BytesStreamFileReader
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayInputStream
import java.util.Objects

class BytesFormatStreamFileReaderTest extends AnyFlatSpec with MockitoSugar with Matchers {

  private val fileContents = "lemonOlivelemonOlive".getBytes

  "read" should "read entire file at once" in {
    val target = new BytesStreamFileReader(
      new ByteArrayInputStream(fileContents),
      fileContents.length.toLong,
    )

    checkRecord(target, BytesOutputRow(fileContents))

    target.hasNext should be(false)
  }

  "hasNext" should "return false for empty file" in {

    val target = new BytesStreamFileReader(
      new ByteArrayInputStream(Array[Byte]()),
      0,
    )

    target.hasNext should be(false)
  }

  private def checkRecord(target: BytesStreamFileReader, expectedOutputRow: BytesOutputRow) = {
    target.hasNext should be(true)
    val record = target.next()
    checkEqualsByteArrayValue(record, expectedOutputRow)
  }

  private def checkEqualsByteArrayValue(res: BytesOutputRow, expected: BytesOutputRow): Any =
    Objects.deepEquals(res.value, expected.value) should be(true)

}
