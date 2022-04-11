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

import io.lenses.streamreactor.connect.aws.s3.formats.bytes.ByteArrayUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks._

import java.io.{ByteArrayInputStream, DataInputStream}
import java.util

object BytesOutputRowTest extends Matchers {

  val outputKeyAndValueWithSizes: BytesOutputRow = BytesOutputRow(Some(4L), Some(5L), "fish".getBytes, "chips".getBytes)
  private val outputKeyWithSize = BytesOutputRow(Some(4L), None, "fish".getBytes, Array())
  private val outputValueWithSize = BytesOutputRow(None, Some(5L), Array(), "chips".getBytes)
  private val outputKeyOnly = BytesOutputRow(None, None, "fish".getBytes, Array())
  private val outputValueOnly = BytesOutputRow(None, None, Array(), "chips".getBytes)
  private val outputLongKeyValueSizes = BytesOutputRow(
    Some(50000),
    Some(2000),
    List.fill(50000)("a").mkString.getBytes,
    List.fill(2000)("b").mkString.getBytes
  )


  val bytesKeyAndValueWithSizes: Array[Byte] = ByteArrayUtils.longToByteArray(4L) ++ ByteArrayUtils.longToByteArray(5) ++ "fishchips".getBytes
  private val bytesKeyWithSize: Array[Byte] = ByteArrayUtils.longToByteArray(4L) ++ "fish".getBytes
  private val bytesValueWithSize: Array[Byte] = ByteArrayUtils.longToByteArray(5L) ++ "chips".getBytes
  private val bytesKeyOnly: Array[Byte] = Array('f', 'i', 's', 'h')
  private val bytesValueOnly: Array[Byte] = Array('c', 'h', 'i', 'p', 's')
  private val bytesLongKeyValueSizes: Array[Byte] = ByteArrayUtils.longToByteArray(50000L) ++ ByteArrayUtils.longToByteArray(2000L) ++ List.fill(50000)("a").mkString.getBytes ++ List.fill(2000)("b").mkString.getBytes

  def checkEqualsByteArrayValue(res: BytesOutputRow, expected: BytesOutputRow): Any = {
    res.keySize should be(expected.keySize)
    res.valueSize should be(expected.valueSize)
    util.Objects.deepEquals(res.key, expected.key) should be (true)
    util.Objects.deepEquals(res.value, expected.value) should be (true)
  }
}

class BytesOutputRowTest extends AnyFlatSpec with Matchers {

  import BytesOutputRowTest._

  private val testDataWithSize = Table(
    ("byteArray", "expectedOutputRow", "bytesWriteMode"),
    (outputKeyAndValueWithSizes, bytesKeyAndValueWithSizes, BytesWriteMode.KeyAndValueWithSizes),
    (outputKeyWithSize, bytesKeyWithSize, BytesWriteMode.KeyWithSize),
    (outputValueWithSize, bytesValueWithSize, BytesWriteMode.ValueWithSize),
    (outputLongKeyValueSizes, bytesLongKeyValueSizes, BytesWriteMode.KeyAndValueWithSizes)
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
      val res = bytesWriteMode.read(byteArray)
      checkEqualsByteArrayValue(res, outputRow)
    }
  }

  "apply" should "create output rows from byte arrays with sizes" in {
    forAll(testDataWithSize) { (outputRow: BytesOutputRow, byteArray: Array[Byte], bytesWriteMode: BytesWriteMode) =>
      val res = bytesWriteMode.read(new DataInputStream(new ByteArrayInputStream(byteArray)))
      checkEqualsByteArrayValue(res, outputRow)
    }
  }


}
