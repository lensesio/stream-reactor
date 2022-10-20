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

import io.lenses.streamreactor.connect.aws.s3.formats.bytes.ByteArrayUtils
import io.lenses.streamreactor.connect.aws.s3.model.ByteArraySinkData
import io.lenses.streamreactor.connect.aws.s3.model.BytesWriteMode
import io.lenses.streamreactor.connect.aws.s3.model.StructSinkData
import io.lenses.streamreactor.connect.aws.s3.utils.TestSampleSchemaAndData._
import io.lenses.streamreactor.connect.aws.s3.stream.S3ByteArrayOutputStream
import org.apache.commons.io.IOUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BytesFormatWriterTest extends AnyFlatSpec with Matchers {

  val bytes:            Array[Byte]       = getPixelBytes
  val byteArrayValue:   ByteArraySinkData = ByteArraySinkData(bytes, None)
  val pixelLengthBytes: Array[Byte]       = ByteArrayUtils.longToByteArray(bytes.length.longValue())

  "convert" should "write a string to byte stream" in {

    val outputStream      = new S3ByteArrayOutputStream()
    val bytesFormatWriter = new BytesFormatWriter(() => outputStream, BytesWriteMode.ValueOnly)
    bytesFormatWriter.write(None, ByteArraySinkData("Sausages".getBytes, None), topic)

    outputStream.toString should be("Sausages")

    bytesFormatWriter.complete()
  }

  "convert" should "write binary with ValueOnly" in {

    val outputStream      = new S3ByteArrayOutputStream()
    val bytesFormatWriter = new BytesFormatWriter(() => outputStream, BytesWriteMode.ValueOnly)
    bytesFormatWriter.write(None, byteArrayValue, topic)

    outputStream.toByteArray should be(bytes)

    bytesFormatWriter.complete()

  }

  "convert" should "write binary with KeyOnly" in {

    val outputStream      = new S3ByteArrayOutputStream()
    val bytesFormatWriter = new BytesFormatWriter(() => outputStream, BytesWriteMode.KeyOnly)
    bytesFormatWriter.write(Some(byteArrayValue), ByteArraySinkData("notUsed".getBytes, None), topic)

    outputStream.toByteArray should be(bytes)

    bytesFormatWriter.complete()

  }

  "convert" should "write binary with KeyAndValueWithSizes" in {

    val outputStream      = new S3ByteArrayOutputStream()
    val bytesFormatWriter = new BytesFormatWriter(() => outputStream, BytesWriteMode.KeyAndValueWithSizes)
    bytesFormatWriter.write(Some(byteArrayValue), byteArrayValue, topic)

    outputStream.toByteArray should be(pixelLengthBytes ++ pixelLengthBytes ++ bytes ++ bytes)

    bytesFormatWriter.complete()

  }

  "convert" should "write binary with KeyWithSize" in {

    val outputStream      = new S3ByteArrayOutputStream()
    val bytesFormatWriter = new BytesFormatWriter(() => outputStream, BytesWriteMode.KeyWithSize)

    bytesFormatWriter.write(Some(byteArrayValue), ByteArraySinkData("notUsed".getBytes, None), topic)

    outputStream.toByteArray should be(pixelLengthBytes ++ bytes)

    bytesFormatWriter.complete()

  }

  "convert" should "write binary with ValueWithSize" in {

    val outputStream      = new S3ByteArrayOutputStream()
    val bytesFormatWriter = new BytesFormatWriter(() => outputStream, BytesWriteMode.ValueWithSize)
    bytesFormatWriter.write(Some(ByteArraySinkData("notUsed".getBytes, None)), byteArrayValue, topic)

    outputStream.toByteArray should be(pixelLengthBytes ++ bytes)

    bytesFormatWriter.complete()

  }

  "convert" should "write  multiple parts of an image and combine" in {

    val stream = classOf[BytesFormatWriter].getResourceAsStream("/streamreactor-logo.png")
    val bytes: Array[Byte] = IOUtils.toByteArray(stream)
    val (bytes1, bytes2) = bytes.splitAt(bytes.length / 2)

    val outputStream      = new S3ByteArrayOutputStream()
    val bytesFormatWriter = new BytesFormatWriter(() => outputStream, BytesWriteMode.ValueOnly)
    bytesFormatWriter.write(None, ByteArraySinkData(bytes1, None), topic)
    bytesFormatWriter.write(None, ByteArraySinkData(bytes2, None), topic)

    outputStream.toByteArray should be(bytes)

    bytesFormatWriter.complete()
  }

  "convert" should "throw error when avro value is supplied" in {

    val outputStream      = new S3ByteArrayOutputStream()
    val bytesFormatWriter = new BytesFormatWriter(() => outputStream, BytesWriteMode.ValueOnly)
    val caught            = bytesFormatWriter.write(None, StructSinkData(users.head), topic)
    bytesFormatWriter.complete()
    caught should be.leftSide
  }

  private def getPixelBytes = {
    //noinspection SpellCheckingInspection
    val stream = classOf[BytesFormatWriter].getResourceAsStream("/redpixel.gif")
    val bytes: Array[Byte] = IOUtils.toByteArray(stream)
    bytes
  }

}
