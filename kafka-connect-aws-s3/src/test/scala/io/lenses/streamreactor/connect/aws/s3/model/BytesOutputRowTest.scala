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

package io.lenses.streamreactor.connect.aws.s3.model

import java.io.ByteArrayInputStream

import io.lenses.streamreactor.connect.aws.s3.config.BytesWriteMode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks._

object BytesOutputRowTest extends Matchers {

  val outputKeyAndValueWithSizes: BytesOutputRow = BytesOutputRow(Some(4L), Some(5L), "fish".getBytes, "chips".getBytes)
  private val outputKeyWithSize = BytesOutputRow(Some(4L), None, "fish".getBytes, Array())
  private val outputValueWithSize = BytesOutputRow(None, Some(5L), Array(), "chips".getBytes)
  private val outputKeyOnly = BytesOutputRow(None, None, "fish".getBytes, Array())
  private val outputValueOnly = BytesOutputRow(None, None, Array(), "chips".getBytes)

  val bytesKeyAndValueWithSizes: Array[Byte] = Array(4L.byteValue(), 5L.byteValue(), 'f', 'i', 's', 'h', 'c', 'h', 'i', 'p', 's')
  private val bytesKeyWithSize: Array[Byte] = Array(4L.byteValue(), 'f', 'i', 's', 'h')
  private val bytesValueWithSize: Array[Byte] = Array(5L.byteValue(), 'c', 'h', 'i', 'p', 's')
  private val bytesKeyOnly: Array[Byte] = Array('f', 'i', 's', 'h')
  private val bytesValueOnly: Array[Byte] = Array('c', 'h', 'i', 'p', 's')

  def checkEqualsByteArrayValue(res: BytesOutputRow, expected: BytesOutputRow): Any = {
    res.keySize should be(expected.keySize)
    res.valueSize should be(expected.valueSize)
    res.key.deep should be(expected.key.deep)
    res.value.deep should be(expected.value.deep)
  }
}

class BytesOutputRowTest extends AnyFlatSpec with Matchers {

  import BytesOutputRowTest._

  private val testDataWithSize = Table(
    ("byteArray", "expectedOutputRow", "bytesWriteMode"),
    (outputKeyAndValueWithSizes, bytesKeyAndValueWithSizes, BytesWriteMode.KeyAndValueWithSizes),
    (outputKeyWithSize, bytesKeyWithSize, BytesWriteMode.KeyWithSize),
    (outputValueWithSize, bytesValueWithSize, BytesWriteMode.ValueWithSize),
  )

  private val testDataKeyOrValueOnly = Table(
    ("byteArray", "expectedOutputRow", "bytesWriteMode"),
    (outputKeyOnly, bytesKeyOnly, BytesWriteMode.KeyOnly),
    (outputValueOnly, bytesValueOnly, BytesWriteMode.ValueOnly),
  )

  "toByteArray" should "create byte array from byte output rows" in {
    forAll(testDataKeyOrValueOnly) { (outputRow: BytesOutputRow, byteArray: Array[Byte], _) =>
      outputRow.toByteArray should be(byteArray)
    }
  }

  "toByteArray" should "create byte array from byte output rows with sizes" in {
    forAll(testDataWithSize) { (outputRow: BytesOutputRow, byteArray: Array[Byte], _) =>
      outputRow.toByteArray should be(byteArray)
    }
  }

  "apply" should "create output rows from byte arrays keys or values only" in {
    forAll(testDataKeyOrValueOnly) { (outputRow: BytesOutputRow, byteArray: Array[Byte], bytesWriteMode: BytesWriteMode) =>
      val res = BytesOutputRow(byteArray, bytesWriteMode)
      checkEqualsByteArrayValue(res, outputRow)
    }
  }

  "apply" should "create output rows from byte arrays with sizes" in {
    forAll(testDataWithSize) { (outputRow: BytesOutputRow, byteArray: Array[Byte], bytesWriteMode: BytesWriteMode) =>
      val res = BytesOutputRow(new ByteArrayInputStream(byteArray), bytesWriteMode)
      checkEqualsByteArrayValue(res, outputRow)
    }
  }


}
