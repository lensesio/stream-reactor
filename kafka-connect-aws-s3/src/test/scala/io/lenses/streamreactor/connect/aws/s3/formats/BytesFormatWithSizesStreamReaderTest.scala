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

class BytesFormatWithSizesStreamReaderTest extends AnyFlatSpec with MockitoSugar with Matchers {

  import BytesOutputRowTest._

  private val bucketAndPath: BucketAndPath = mock[BucketAndPath]

  private val bytesKeyAndValueWithSizes2: Array[Byte] = Array(4L.byteValue(), 3L.byteValue(), 'c', 'a', 'k', 'e', 't', 'e', 'a')

  private val outputKeyAndValueWithSizes2 = BytesOutputRow(Some(4L), Some(3L), "cake".getBytes, "tea".getBytes)

  "next" should "return single record of key and values with sizes" in {

    val inputStreamFn = () => new ByteArrayInputStream(bytesKeyAndValueWithSizes)
    val sizeFn = () => bytesKeyAndValueWithSizes.length.longValue()
    val target = new BytesFormatWithSizesStreamReader(inputStreamFn, sizeFn, bucketAndPath, bytesWriteMode = BytesWriteMode.KeyAndValueWithSizes)

    checkRecord(target, outputKeyAndValueWithSizes)

    target.hasNext should be(false)

  }

  "next" should "return multiple records of key and values with sizes" in {

    val allElements = bytesKeyAndValueWithSizes ++ bytesKeyAndValueWithSizes2
    val inputStreamFn = () => new ByteArrayInputStream(allElements)
    val sizeFn = () => allElements.length.longValue()
    val target = new BytesFormatWithSizesStreamReader(inputStreamFn, sizeFn, bucketAndPath, bytesWriteMode = BytesWriteMode.KeyAndValueWithSizes)

    checkRecord(target, outputKeyAndValueWithSizes)

    checkRecord(target, outputKeyAndValueWithSizes2)

    target.hasNext should be(false)

  }

  private def checkRecord(target: BytesFormatWithSizesStreamReader, expectedOutputRow: BytesOutputRow) = {
    target.hasNext should be(true)
    val record = target.next()
    checkEqualsByteArrayValue(record.data, expectedOutputRow)
  }
}
